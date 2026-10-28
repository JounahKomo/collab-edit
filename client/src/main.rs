use std::{
    fs::File,
    io::{self, BufReader, ErrorKind, Read, Write},
    net::TcpStream,
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
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;

slint::include_modules!();

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "CollabEdit - Collaborative Document Editor Client"
)]
struct Cli {
    /// Server address in host:port format
    #[arg(default_value = "127.0.0.1:5011")]
    target: String,

    /// Username for collaborative session
    #[arg(short, long, default_value = "User")]
    username: String,

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

enum ConnectionStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for ConnectionStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

fn build_client_config(ca_cert_path: Option<&Path>) -> io::Result<ClientConfig> {
    let mut root_store = RootCertStore::empty();

    if let Some(path) = ca_cert_path {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

fn make_frame(msg_id: u32, msg_type: &[u8; 4], payload: &str) -> Vec<u8> {
    let payload_bytes = payload.as_bytes();
    let length = (12 + payload_bytes.len()) as u32;
    let mut frame = Vec::with_capacity(length as usize);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&msg_id.to_be_bytes());
    frame.extend_from_slice(msg_type);
    frame.extend_from_slice(payload_bytes);
    frame
}

#[derive(Debug, Clone)]
struct ClientDocState {
    lines: Vec<String>,
    cursor_line: usize,
    cursor_col: usize,
    revision: u32,
}

impl ClientDocState {
    fn new() -> Self {
        Self {
            lines: vec![String::new()],
            cursor_line: 0,
            cursor_col: 0,
            revision: 0,
        }
    }

    fn update_from_server(&mut self, revision: u32, content: &str) {
        self.revision = revision;
        let mut new_lines: Vec<String> = content.split('\n').map(|s| s.to_string()).collect();
        if new_lines.is_empty() {
            new_lines.push(String::new());
        }
        self.lines = new_lines;
        self.clamp_cursor();
    }

    fn clamp_cursor(&mut self) {
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        if self.cursor_line >= self.lines.len() {
            self.cursor_line = self.lines.len() - 1;
        }
        let line_len = self.lines[self.cursor_line].chars().count();
        if self.cursor_col > line_len {
            self.cursor_col = line_len;
        }
    }

    fn line_prefix_suffix(&self) -> (String, String) {
        if self.cursor_line >= self.lines.len() {
            return (String::new(), String::new());
        }
        let line = &self.lines[self.cursor_line];
        let col = self.cursor_col.min(line.chars().count());
        let prefix: String = line.chars().take(col).collect();
        let suffix: String = line.chars().skip(col).collect();
        (prefix, suffix)
    }

    fn move_cursor(&mut self, dir: &str) {
        match dir {
            "up" => {
                if self.cursor_line > 0 {
                    self.cursor_line -= 1;
                    let line_len = self.lines[self.cursor_line].chars().count();
                    if self.cursor_col > line_len {
                        self.cursor_col = line_len;
                    }
                }
            }
            "down" => {
                if self.cursor_line + 1 < self.lines.len() {
                    self.cursor_line += 1;
                    let line_len = self.lines[self.cursor_line].chars().count();
                    if self.cursor_col > line_len {
                        self.cursor_col = line_len;
                    }
                }
            }
            "left" => {
                if self.cursor_col > 0 {
                    self.cursor_col -= 1;
                } else if self.cursor_line > 0 {
                    self.cursor_line -= 1;
                    self.cursor_col = self.lines[self.cursor_line].chars().count();
                }
            }
            "right" => {
                let line_len = self.lines[self.cursor_line].chars().count();
                if self.cursor_col < line_len {
                    self.cursor_col += 1;
                } else if self.cursor_line + 1 < self.lines.len() {
                    self.cursor_line += 1;
                    self.cursor_col = 0;
                }
            }
            _ => {}
        }
    }

    fn insert_text(&mut self, text: &str) {
        self.clamp_cursor();
        let line = &mut self.lines[self.cursor_line];
        let col = self.cursor_col;
        let prefix: String = line.chars().take(col).collect();
        let suffix: String = line.chars().skip(col).collect();
        *line = format!("{}{}{}", prefix, text, suffix);
        self.cursor_col += text.chars().count();
    }

    fn insert_newline(&mut self) {
        self.clamp_cursor();
        let line = &self.lines[self.cursor_line];
        let col = self.cursor_col;
        let prefix: String = line.chars().take(col).collect();
        let suffix: String = line.chars().skip(col).collect();

        self.lines[self.cursor_line] = prefix;
        self.lines.insert(self.cursor_line + 1, suffix);
        self.cursor_line += 1;
        self.cursor_col = 0;
    }

    fn backspace(&mut self) -> Option<(usize, usize, usize)> {
        self.clamp_cursor();
        if self.cursor_col > 0 {
            let del_col = self.cursor_col - 1;
            let line = &mut self.lines[self.cursor_line];
            let prefix: String = line.chars().take(del_col).collect();
            let suffix: String = line.chars().skip(self.cursor_col).collect();
            *line = format!("{}{}", prefix, suffix);
            self.cursor_col = del_col;
            Some((self.cursor_line, del_col, 1))
        } else if self.cursor_line > 0 {
            let prev_line_idx = self.cursor_line - 1;
            let prev_len = self.lines[prev_line_idx].chars().count();
            let cur_line = self.lines.remove(self.cursor_line);
            self.lines[prev_line_idx].push_str(&cur_line);
            self.cursor_line = prev_line_idx;
            self.cursor_col = prev_len;
            Some((prev_line_idx, prev_len, 1))
        } else {
            None
        }
    }
}

fn sync_ui(ui: &AppWindow, state: &ClientDocState) {
    ui.set_cursor_line(state.cursor_line as i32);
    ui.set_cursor_col(state.cursor_col as i32);
    ui.set_revision(state.revision as i32);

    let (prefix, suffix) = state.line_prefix_suffix();
    ui.set_line_prefix(prefix.into());
    ui.set_line_suffix(suffix.into());

    let lines_model: Vec<slint::SharedString> = state
        .lines
        .iter()
        .map(|s| slint::SharedString::from(s.as_str()))
        .collect();
    ui.set_doc_lines(slint::ModelRc::new(slint::VecModel::from(lines_model)));
}

fn next_msg_id(counter: &AtomicU32) -> u32 {
    let id = counter.fetch_add(2, Ordering::SeqCst);
    if id.is_multiple_of(2) {
        counter.fetch_add(1, Ordering::SeqCst);
    }
    id
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let clean_target = cli
        .target
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');

    let (host, _port) = clean_target
        .split_once(':')
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "expected <host>:<port>"))?;

    println!(
        "Connecting to {} (TLS: {})...",
        clean_target,
        if cli.no_tls { "disabled" } else { "enabled" }
    );

    let tcp_stream = TcpStream::connect(clean_target)?;

    // Check for rootCA.crt fallback if not specified
    let ca_cert_path = cli.ca_cert.or_else(|| {
        if Path::new("rootCA.crt").exists() {
            Some(PathBuf::from("rootCA.crt"))
        } else if Path::new("../rootCA.crt").exists() {
            Some(PathBuf::from("../rootCA.crt"))
        } else {
            None
        }
    });

    let mut stream = if cli.no_tls {
        ConnectionStream::Plain(tcp_stream)
    } else {
        let tls_config = build_client_config(ca_cert_path.as_deref())
            .map_err(|e| io::Error::other(e.to_string()))?;

        let sni_name = cli.server_name.unwrap_or_else(|| {
            if host.parse::<std::net::IpAddr>().is_ok() {
                "localhost".to_string()
            } else {
                host.to_string()
            }
        });

        let server_name: ServerName<'static> = sni_name
            .try_into()
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid SNI host name"))?;

        let conn = ClientConnection::new(Arc::new(tls_config), server_name)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut tls_stream = StreamOwned::new(conn, tcp_stream);
        tls_stream.flush()?;
        ConnectionStream::Tls(Box::new(tls_stream))
    };

    println!("Connected successfully!");

    let ui = AppWindow::new()?;
    ui.set_username(cli.username.clone().into());
    ui.set_status_text("Connected".into());

    let doc_state = Arc::new(Mutex::new(ClientDocState::new()));
    let msg_counter = Arc::new(AtomicU32::new(1));
    let (tx_net, rx_net) = mpsc::channel::<Vec<u8>>();

    // Initial sync
    {
        let st = doc_state.lock().unwrap();
        sync_ui(&ui, &st);
    }

    // Handshake: send USR and GET
    let usr_frame = make_frame(next_msg_id(&msg_counter), b"USR ", &cli.username);
    tx_net.send(usr_frame)?;
    let get_frame = make_frame(next_msg_id(&msg_counter), b"GET ", "");
    tx_net.send(get_frame)?;

    // Background Network Worker Thread
    let ui_weak = ui.as_weak();
    let doc_state_net = Arc::clone(&doc_state);
    thread::spawn(move || {
        let tcp = match &mut stream {
            ConnectionStream::Plain(s) => s,
            ConnectionStream::Tls(s) => s.get_mut(),
        };
        let _ = tcp.set_read_timeout(Some(Duration::from_millis(20)));

        let mut read_buf = Vec::new();
        let mut chunk = [0u8; 4096];

        loop {
            // Drain outgoing messages
            while let Ok(msg) = rx_net.try_recv() {
                if let Err(e) = stream.write_all(&msg) {
                    eprintln!("Write error: {e}");
                    return;
                }
                let _ = stream.flush();
            }

            // Read incoming bytes
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
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
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

            // Process frames
            while read_buf.len() >= 12 {
                let length = u32::from_be_bytes(read_buf[0..4].try_into().unwrap()) as usize;
                if read_buf.len() < length {
                    break;
                }

                let _id = u32::from_be_bytes(read_buf[4..8].try_into().unwrap());
                let msg_type = &read_buf[8..12];
                let payload_bytes = &read_buf[12..length];
                let payload = String::from_utf8_lossy(payload_bytes).to_string();

                if msg_type == b"GET " {
                    if let Some((rev_str, content)) = payload.split_once('\n') {
                        if let Ok(rev) = rev_str.parse::<u32>() {
                            let mut st = doc_state_net.lock().unwrap();
                            st.update_from_server(rev, content);
                            let ui_clone = ui_weak.clone();
                            let state_clone = Arc::clone(&doc_state_net);
                            let _ = slint::invoke_from_event_loop(move || {
                                if let Some(ui) = ui_clone.upgrade() {
                                    let st = state_clone.lock().unwrap();
                                    sync_ui(&ui, &st);
                                }
                            });
                        }
                    } else if let Ok(rev) = payload.trim().parse::<u32>() {
                        let mut st = doc_state_net.lock().unwrap();
                        st.update_from_server(rev, "");
                        let ui_clone = ui_weak.clone();
                        let state_clone = Arc::clone(&doc_state_net);
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_clone.upgrade() {
                                let st = state_clone.lock().unwrap();
                                sync_ui(&ui, &st);
                            }
                        });
                    }
                } else if msg_type == b"ERR " {
                    let err_msg = payload.clone();
                    let ui_clone = ui_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = ui_clone.upgrade() {
                            ui.set_status_text(format!("Error: {}", err_msg).into());
                        }
                    });
                }

                read_buf.drain(0..length);
            }
        }
    });

    // Setup UI callbacks
    {
        let ui_handle = ui.as_weak();
        let state = Arc::clone(&doc_state);
        let tx = tx_net.clone();
        let counter = Arc::clone(&msg_counter);
        ui.on_key_typed(move |text| {
            if text.is_empty() || text.chars().any(|c| c.is_control()) {
                return;
            }
            let (line, col) = {
                let mut st = state.lock().unwrap();
                let line = st.cursor_line;
                let col = st.cursor_col;
                st.insert_text(&text);
                if let Some(ui) = ui_handle.upgrade() {
                    sync_ui(&ui, &st);
                }
                (line, col)
            };
            let frame = make_frame(
                next_msg_id(&counter),
                b"INS ",
                &format!("{} {} {}", line, col, text),
            );
            let _ = tx.send(frame);
        });
    }

    {
        let ui_handle = ui.as_weak();
        let state = Arc::clone(&doc_state);
        let tx = tx_net.clone();
        let counter = Arc::clone(&msg_counter);
        ui.on_enter_pressed(move || {
            let (line, col) = {
                let mut st = state.lock().unwrap();
                let line = st.cursor_line;
                let col = st.cursor_col;
                st.insert_newline();
                if let Some(ui) = ui_handle.upgrade() {
                    sync_ui(&ui, &st);
                }
                (line, col)
            };
            let frame = make_frame(
                next_msg_id(&counter),
                b"INS ",
                &format!("{} {} \n", line, col),
            );
            let _ = tx.send(frame);
        });
    }

    {
        let ui_handle = ui.as_weak();
        let state = Arc::clone(&doc_state);
        let tx = tx_net.clone();
        let counter = Arc::clone(&msg_counter);
        ui.on_backspace_pressed(move || {
            let del_op = {
                let mut st = state.lock().unwrap();
                let op = st.backspace();
                if let Some(ui) = ui_handle.upgrade() {
                    sync_ui(&ui, &st);
                }
                op
            };
            if let Some((line, col, count)) = del_op {
                let frame = make_frame(
                    next_msg_id(&counter),
                    b"DEL ",
                    &format!("{} {} {}", line, col, count),
                );
                let _ = tx.send(frame);
            }
        });
    }

    {
        let ui_handle = ui.as_weak();
        let state = Arc::clone(&doc_state);
        ui.on_arrow_pressed(move |dir| {
            let mut st = state.lock().unwrap();
            st.move_cursor(&dir);
            if let Some(ui) = ui_handle.upgrade() {
                sync_ui(&ui, &st);
            }
        });
    }

    ui.run()?;

    // Send BYE when closing window
    let bye_frame = make_frame(next_msg_id(&msg_counter), b"BYE ", "");
    let _ = tx_net.send(bye_frame);

    Ok(())
}
