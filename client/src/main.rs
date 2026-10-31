mod doc_state;
mod net;

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use clap::Parser;
use doc_state::ClientDocState;
use net::{ConnectionStream, connect_stream, make_frame};

slint::include_modules!();

const LINES_PER_PAGE: usize = 35;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "CollabEdit - Collaborative Document Editor & Chat Client"
)]
struct Cli {
    /// Server address in host:port format
    #[arg(default_value = "127.0.0.1:5011")]
    target: String,

    /// Username for collaborative session (optional; can be entered in the GUI)
    #[arg(short, long)]
    username: Option<String>,

    /// Path to custom Root CA certificate (.crt / .pem) for verifying the server
    #[arg(long, env = "TLS_CA_CERT")]
    ca_cert: Option<PathBuf>,

    /// Server Name Indication (SNI) for TLS verification
    #[arg(long, env = "TLS_SERVER_NAME")]
    server_name: Option<String>,

    /// Disable TLS and connect with plain TCP
    #[arg(long, env = "NO_TLS")]
    no_tls: bool,
}

#[derive(Debug, Clone)]
struct ClientChatState {
    channels: Vec<String>,
    active_channel: String,
    messages: Vec<ChatMessage>,
}

impl Default for ClientChatState {
    fn default() -> Self {
        Self {
            channels: vec!["general".to_string()],
            active_channel: "general".to_string(),
            messages: Vec::new(),
        }
    }
}

impl ClientChatState {
    fn add_channel(&mut self, channel: &str) {
        if !self.channels.iter().any(|c| c == channel) {
            self.channels.push(channel.to_string());
        }
    }

    fn add_message(&mut self, sender: &str, channel: &str, text: &str) {
        self.add_channel(channel);
        self.messages.push(ChatMessage {
            sender: sender.into(),
            channel: channel.into(),
            text: text.into(),
        });
    }
}

fn sync_doc_ui(ui: &AppWindow, state: &ClientDocState) {
    ui.set_cursor_line(state.cursor_line as i32);
    ui.set_cursor_col(state.cursor_col as i32);

    let (prefix, suffix) = state.line_prefix_suffix();
    ui.set_line_prefix(prefix.into());
    ui.set_line_suffix(suffix.into());

    // Group lines into pages of LINES_PER_PAGE lines
    let total_lines = state.lines.len();
    let num_pages = (total_lines.div_ceil(LINES_PER_PAGE)).max(1);

    let mut pages_vec: Vec<PageData> = Vec::with_capacity(num_pages);
    for page_idx in 0..num_pages {
        let start_line = page_idx * LINES_PER_PAGE;
        let end_line = (start_line + LINES_PER_PAGE).min(total_lines);

        let mut page_lines: Vec<PageLineData> = Vec::with_capacity(LINES_PER_PAGE);
        for global_idx in start_line..end_line {
            page_lines.push(PageLineData {
                global_idx: global_idx as i32,
                text: state.lines[global_idx].as_str().into(),
            });
        }

        pages_vec.push(PageData {
            page_number: (page_idx + 1) as i32,
            lines: slint::ModelRc::new(slint::VecModel::from(page_lines)),
        });
    }

    ui.set_pages(slint::ModelRc::new(slint::VecModel::from(pages_vec)));
}

fn sync_chat_ui(ui: &AppWindow, chat: &ClientChatState) {
    let ch_model: Vec<slint::SharedString> = chat
        .channels
        .iter()
        .map(|c| slint::SharedString::from(c.as_str()))
        .collect();
    ui.set_channels(slint::ModelRc::new(slint::VecModel::from(ch_model)));
    ui.set_active_channel(chat.active_channel.as_str().into());

    let msg_model: Vec<ChatMessage> = chat
        .messages
        .iter()
        .filter(|m| m.channel == chat.active_channel)
        .cloned()
        .collect();
    ui.set_messages(slint::ModelRc::new(slint::VecModel::from(msg_model)));
}

fn next_msg_id(counter: &AtomicU32) -> u32 {
    let id = counter.fetch_add(2, Ordering::SeqCst);
    if id.is_multiple_of(2) {
        counter.fetch_add(1, Ordering::SeqCst);
    }
    id
}

/// Helper context bundling state, UI handle, transmitter, and message ID generator.
#[derive(Clone)]
struct ClientContext {
    ui: slint::Weak<AppWindow>,
    doc_state: Arc<Mutex<ClientDocState>>,
    chat_state: Arc<Mutex<ClientChatState>>,
    tx_net: mpsc::Sender<Vec<u8>>,
    msg_counter: Arc<AtomicU32>,
}

impl ClientContext {
    /// Mutates the client document state, immediately refreshes the UI, and returns the callback's result.
    fn with_doc_state_mut<R>(&self, f: impl FnOnce(&mut ClientDocState) -> R) -> R {
        let mut st = self.doc_state.lock().unwrap();
        let res = f(&mut st);
        if let Some(ui) = self.ui.upgrade() {
            sync_doc_ui(&ui, &st);
        }
        res
    }

    /// Mutates the client chat state and refreshes the chat UI.
    fn with_chat_state_mut<R>(&self, f: impl FnOnce(&mut ClientChatState) -> R) -> R {
        let mut ch = self.chat_state.lock().unwrap();
        let res = f(&mut ch);
        if let Some(ui) = self.ui.upgrade() {
            sync_chat_ui(&ui, &ch);
        }
        res
    }

    /// Generates a unique client message ID and queues a frame to be sent to the server.
    fn send_frame(&self, msg_type: &[u8; 4], payload: &str) {
        let frame = make_frame(next_msg_id(&self.msg_counter), msg_type, payload);
        let _ = self.tx_net.send(frame);
    }
}

fn find_ca_cert(cli_ca_cert: Option<PathBuf>) -> Option<PathBuf> {
    cli_ca_cert.or_else(|| {
        if Path::new("rootCA.crt").exists() {
            Some(PathBuf::from("rootCA.crt"))
        } else if Path::new("../rootCA.crt").exists() {
            Some(PathBuf::from("../rootCA.crt"))
        } else {
            None
        }
    })
}

fn start_network_worker(
    mut stream: ConnectionStream,
    rx_net: mpsc::Receiver<Vec<u8>>,
    ui_weak: slint::Weak<AppWindow>,
    doc_state_net: Arc<Mutex<ClientDocState>>,
    chat_state_net: Arc<Mutex<ClientChatState>>,
) {
    thread::spawn(move || {
        let _ = stream
            .tcp_mut()
            .set_read_timeout(Some(Duration::from_millis(20)));

        let mut read_buf = Vec::new();
        let mut chunk = [0u8; 4096];

        loop {
            // Drain outgoing messages
            while let Ok(msg) = rx_net.try_recv() {
                use std::io::Write;
                if let Err(e) = stream.write_all(&msg) {
                    eprintln!("Write error: {e}");
                    return;
                }
                let _ = stream.flush();
            }

            // Read incoming bytes
            use std::io::Read;
            match stream.read(&mut chunk) {
                Ok(0) => {
                    eprintln!("Server disconnected (EOF).");
                    let ui_clone = ui_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_clone.upgrade() {
                            ui.set_status_text("Disconnected".into());
                        }
                    });
                    break;
                }
                Ok(n) => {
                    read_buf.extend_from_slice(&chunk[..n]);
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Normal timeout
                }
                Err(e) => {
                    eprintln!("Socket read error: {e}");
                    let ui_clone = ui_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_clone.upgrade() {
                            ui.set_status_text("Connection error".into());
                        }
                    });
                    break;
                }
            }

            // Process protocol frames
            while read_buf.len() >= 12 {
                let length = u32::from_be_bytes(read_buf[0..4].try_into().unwrap()) as usize;
                if read_buf.len() < length {
                    break;
                }

                let _id = u32::from_be_bytes(read_buf[4..8].try_into().unwrap());
                let msg_type = &read_buf[8..12];
                let payload_bytes = &read_buf[12..length];
                let payload = String::from_utf8_lossy(payload_bytes).to_string();

                if msg_type == b"USR " {
                    let registered_user = payload.trim().to_string();
                    let ui_clone = ui_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_clone.upgrade() {
                            ui.set_username(registered_user.into());
                            ui.set_is_logged_in(true);
                            ui.set_login_error("".into());
                            ui.set_status_text("Connected".into());
                        }
                    });
                } else if msg_type == b"GET " {
                    let (rev, content) = if let Some((rev_str, text)) = payload.split_once('\n') {
                        (rev_str.parse::<u32>().ok(), text)
                    } else {
                        (payload.trim().parse::<u32>().ok(), "")
                    };

                    if let Some(rev) = rev {
                        let mut st = doc_state_net.lock().unwrap();
                        st.update_from_server(rev, content);
                        let ui_clone = ui_weak.clone();
                        let state_clone = Arc::clone(&doc_state_net);
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_clone.upgrade() {
                                let st = state_clone.lock().unwrap();
                                sync_doc_ui(&ui, &st);
                            }
                        });
                    }
                } else if msg_type == b"MSG " {
                    // Protocol format: <sender> <channel> <message>
                    let parsed = payload.split_once(' ').and_then(|(sender, rest)| {
                        rest.split_once(' ').map(|(ch, t)| (sender, ch, t))
                    });
                    if let Some((sender, channel, text)) = parsed {
                        let ui_clone = ui_weak.clone();
                        let chat_clone = Arc::clone(&chat_state_net);
                        let s = sender.to_string();
                        let ch = channel.to_string();
                        let t = text.to_string();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_clone.upgrade() {
                                let mut chat = chat_clone.lock().unwrap();
                                chat.add_message(&s, &ch, &t);
                                sync_chat_ui(&ui, &chat);
                            }
                        });
                    }
                } else if msg_type == b"ERR " {
                    let err_msg = payload.clone();
                    let ui_clone = ui_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_clone.upgrade() {
                            if !ui.get_is_logged_in() {
                                ui.set_login_error(format!("Login failed: {}", err_msg).into());
                            } else {
                                ui.set_status_text(format!("Error: {}", err_msg).into());
                            }
                        }
                    });
                }

                read_buf.drain(0..length);
            }
        }
    });
}

fn register_ui_callbacks(ui: &AppWindow, ctx: ClientContext) {
    let ctx_login = ctx.clone();
    ui.on_login_submitted(move |name| {
        let clean_name = name.trim().to_string();
        if !clean_name.is_empty() {
            ctx_login.send_frame(b"USR ", &clean_name);
            ctx_login.send_frame(b"GET ", "");
        }
    });

    let ctx_key = ctx.clone();
    ui.on_key_typed(move |text| {
        if text.is_empty() || text.chars().any(|c| c.is_control()) {
            return;
        }
        let (line, col) = ctx_key.with_doc_state_mut(|st| {
            let line = st.cursor_line;
            let col = st.cursor_col;
            st.insert_text(&text);
            (line, col)
        });
        ctx_key.send_frame(b"INS ", &format!("{} {} {}", line, col, text));
    });

    let ctx_enter = ctx.clone();
    ui.on_enter_pressed(move || {
        let (line, col) = ctx_enter.with_doc_state_mut(|st| {
            let line = st.cursor_line;
            let col = st.cursor_col;
            st.insert_newline();
            (line, col)
        });
        ctx_enter.send_frame(b"INS ", &format!("{} {} \n", line, col));
    });

    let ctx_backspace = ctx.clone();
    ui.on_backspace_pressed(move || {
        let del_op = ctx_backspace.with_doc_state_mut(|st| st.backspace());
        if let Some((line, col, count)) = del_op {
            ctx_backspace.send_frame(b"DEL ", &format!("{} {} {}", line, col, count));
        }
    });

    let ctx_arrow = ctx.clone();
    ui.on_arrow_pressed(move |dir| {
        ctx_arrow.with_doc_state_mut(|st| {
            st.move_cursor(&dir);
        });
    });

    let ctx_click = ctx.clone();
    ui.on_cursor_position_clicked(move |line, col| {
        if line >= 0 && col >= 0 {
            ctx_click.with_doc_state_mut(|st| {
                st.set_cursor_pos(line as usize, col as usize);
            });
        }
    });

    let ctx_select_ch = ctx.clone();
    ui.on_channel_selected(move |channel| {
        ctx_select_ch.with_chat_state_mut(|ch| {
            ch.active_channel = channel.to_string();
        });
    });

    let ctx_send_msg = ctx.clone();
    ui.on_send_chat_message(move |channel, text| {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            ctx_send_msg.send_frame(b"MSG ", &format!("{} {}", channel, trimmed));
        }
    });

    let ctx_add_ch = ctx;
    ui.on_add_channel_submitted(move |new_channel| {
        let trimmed = new_channel.trim().to_string();
        if !trimmed.is_empty() {
            ctx_add_ch.with_chat_state_mut(|ch| {
                ch.add_channel(&trimmed);
                ch.active_channel = trimmed.clone();
            });
            // Send initial ping to create the channel on the server
            ctx_add_ch.send_frame(b"MSG ", &format!("{} joined channel #{}", trimmed, trimmed));
        }
    });
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let ca_cert_path = find_ca_cert(cli.ca_cert);

    println!(
        "Connecting to {} (TLS: {})...",
        cli.target,
        if cli.no_tls { "disabled" } else { "enabled" }
    );

    let stream = connect_stream(
        &cli.target,
        cli.no_tls,
        ca_cert_path.as_deref(),
        cli.server_name.as_deref(),
    )?;

    println!("Connected successfully!");

    let ui = AppWindow::new()?;
    let doc_state = Arc::new(Mutex::new(ClientDocState::new()));
    let chat_state = Arc::new(Mutex::new(ClientChatState::default()));
    let msg_counter = Arc::new(AtomicU32::new(1));
    let (tx_net, rx_net) = mpsc::channel::<Vec<u8>>();

    // Initial UI sync
    {
        let st = doc_state.lock().unwrap();
        sync_doc_ui(&ui, &st);
        let ch = chat_state.lock().unwrap();
        sync_chat_ui(&ui, &ch);
    }

    let ctx = ClientContext {
        ui: ui.as_weak(),
        doc_state: Arc::clone(&doc_state),
        chat_state: Arc::clone(&chat_state),
        tx_net: tx_net.clone(),
        msg_counter: Arc::clone(&msg_counter),
    };

    // If username was provided via CLI, send USR and GET immediately
    if let Some(user) = cli.username {
        if !user.trim().is_empty() {
            ui.set_username(user.clone().into());
            ui.set_is_logged_in(true);
            ui.set_status_text("Connected".into());
            ctx.send_frame(b"USR ", &user);
            ctx.send_frame(b"GET ", "");
        }
    } else {
        ui.set_status_text("Waiting for username".into());
    }

    // Launch background network thread
    start_network_worker(
        stream,
        rx_net,
        ui.as_weak(),
        Arc::clone(&doc_state),
        Arc::clone(&chat_state),
    );

    // Register user input event callbacks
    register_ui_callbacks(&ui, ctx.clone());

    ui.run()?;

    // Disconnect cleanly
    ctx.send_frame(b"BYE ", "");

    Ok(())
}
