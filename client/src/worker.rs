//! Background thread that owns the TCP/TLS stream: it writes queued frames,
//! reads server frames and applies them to the client state and the UI.

use std::{
    io::{self, Read, Write},
    sync::{Arc, Mutex, atomic::AtomicU32, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    AppWindow,
    chat_state::ClientChatState,
    context::next_msg_id,
    cursors::CursorSender,
    doc_state::ClientDocState,
    net::{ConnectionStream, make_frame},
    ui_sync::{sync_chat_ui, sync_doc_ui},
};

const HEADER_LEN: usize = 12;
const READ_TIMEOUT: Duration = Duration::from_millis(20);

/// Shared handles the worker needs to apply server messages.
pub struct WorkerContext {
    pub ui: slint::Weak<AppWindow>,
    pub doc_state: Arc<Mutex<ClientDocState>>,
    pub chat_state: Arc<Mutex<ClientChatState>>,
    pub cursor_sender: Arc<CursorSender>,
    pub udp_token: Arc<Mutex<Option<String>>>,
    pub msg_counter: Arc<AtomicU32>,
    pub own_name: Arc<Mutex<String>>,
}

/// What a single read attempt produced.
enum ReadOutcome {
    Data,
    Idle,
    Closed,
    Failed,
}

/// Starts the worker thread.
pub fn start_network_worker(
    stream: ConnectionStream,
    rx_net: mpsc::Receiver<Vec<u8>>,
    ctx: WorkerContext,
) {
    thread::spawn(move || run(stream, rx_net, ctx));
}

fn run(mut stream: ConnectionStream, rx_net: mpsc::Receiver<Vec<u8>>, ctx: WorkerContext) {
    let _ = stream.tcp_mut().set_read_timeout(Some(READ_TIMEOUT));
    let mut read_buf = Vec::new();

    loop {
        if let Err(e) = flush_outgoing(&mut stream, &rx_net) {
            eprintln!("Write error: {e}");
            return;
        }

        match read_available(&mut stream, &mut read_buf) {
            ReadOutcome::Data | ReadOutcome::Idle => {}
            ReadOutcome::Closed => {
                eprintln!("Server disconnected (EOF).");
                set_status(&ctx.ui, "Disconnected");
                return;
            }
            ReadOutcome::Failed => {
                set_status(&ctx.ui, "Connection error");
                return;
            }
        }

        while let Some((msg_type, payload)) = take_frame(&mut read_buf) {
            handle_frame(&mut stream, &ctx, &msg_type, &payload);
        }
    }
}

/// Writes every queued outgoing frame to the socket.
fn flush_outgoing(
    stream: &mut ConnectionStream,
    rx_net: &mpsc::Receiver<Vec<u8>>,
) -> io::Result<()> {
    while let Ok(msg) = rx_net.try_recv() {
        stream.write_all(&msg)?;
        stream.flush()?;
    }
    Ok(())
}

fn read_available(stream: &mut ConnectionStream, read_buf: &mut Vec<u8>) -> ReadOutcome {
    let mut chunk = [0u8; 4096];
    match stream.read(&mut chunk) {
        Ok(0) => ReadOutcome::Closed,
        Ok(n) => {
            read_buf.extend_from_slice(&chunk[..n]);
            ReadOutcome::Data
        }
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            ReadOutcome::Idle
        }
        Err(e) => {
            eprintln!("Socket read error: {e}");
            ReadOutcome::Failed
        }
    }
}

/// Removes the next complete frame from the buffer, returning its type and
/// payload (lossily decoded as UTF-8).
fn take_frame(read_buf: &mut Vec<u8>) -> Option<([u8; 4], String)> {
    if read_buf.len() < HEADER_LEN {
        return None;
    }
    let length = u32::from_be_bytes(read_buf[0..4].try_into().unwrap()) as usize;
    if length < HEADER_LEN {
        // Corrupt length: drop what we have rather than loop forever.
        read_buf.clear();
        return None;
    }
    if read_buf.len() < length {
        return None;
    }
    let msg_type: [u8; 4] = read_buf[8..HEADER_LEN].try_into().unwrap();
    let payload = String::from_utf8_lossy(&read_buf[HEADER_LEN..length]).to_string();
    read_buf.drain(0..length);
    Some((msg_type, payload))
}

fn handle_frame(
    stream: &mut ConnectionStream,
    ctx: &WorkerContext,
    msg_type: &[u8; 4],
    payload: &str,
) {
    match msg_type {
        b"USR " => on_login_confirmed(stream, ctx, payload.trim()),
        b"GET " => on_document(ctx, payload),
        b"MSG " => on_chat_message(ctx, payload),
        b"ERR " => on_error(ctx, payload),
        _ => {}
    }
}

/// The server accepted our username: announce a UDP token and show the editor.
fn on_login_confirmed(stream: &mut ConnectionStream, ctx: &WorkerContext, username: &str) {
    *ctx.own_name.lock().unwrap() = username.to_string();
    announce_udp_token(stream, ctx);

    let username = username.to_string();
    post_to_ui(&ctx.ui, move |ui| {
        ui.set_username(username.into());
        ui.set_is_logged_in(true);
        ui.set_login_error("".into());
        ui.set_status_text("Connected".into());
    });
}

/// Announces a token over TCP; the hello datagram carrying it (sent by the
/// cursor thread) lets the server learn our UDP address.
fn announce_udp_token(stream: &mut ConnectionStream, ctx: &WorkerContext) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let token = format!("{:x}-{:x}", std::process::id(), nanos);
    let frame = make_frame(next_msg_id(&ctx.msg_counter), b"UDP ", &token);
    let _ = stream.write_all(&frame);
    let _ = stream.flush();
    *ctx.udp_token.lock().unwrap() = Some(token);
}

/// `GET <revision>\n<content>`: replace the local document.
fn on_document(ctx: &WorkerContext, payload: &str) {
    let (rev, content) = match payload.split_once('\n') {
        Some((rev_str, text)) => (rev_str.parse::<u32>().ok(), text),
        None => (payload.trim().parse::<u32>().ok(), ""),
    };
    let Some(rev) = rev else { return };

    {
        let mut st = ctx.doc_state.lock().unwrap();
        st.update_from_server(rev, content);
        ctx.cursor_sender
            .send_if_changed(st.cursor_line, st.cursor_col);
    }
    let doc_state = Arc::clone(&ctx.doc_state);
    post_to_ui(&ctx.ui, move |ui| {
        sync_doc_ui(ui, &doc_state.lock().unwrap());
    });
}

/// `MSG <sender> <channel> <message>`
fn on_chat_message(ctx: &WorkerContext, payload: &str) {
    let parsed = payload
        .split_once(' ')
        .and_then(|(sender, rest)| rest.split_once(' ').map(|(ch, text)| (sender, ch, text)));
    let Some((sender, channel, text)) = parsed else {
        return;
    };

    let (sender, channel, text) = (sender.to_string(), channel.to_string(), text.to_string());
    let chat_state = Arc::clone(&ctx.chat_state);
    post_to_ui(&ctx.ui, move |ui| {
        let mut chat = chat_state.lock().unwrap();
        chat.add_message(&sender, &channel, &text);
        sync_chat_ui(ui, &chat);
    });
}

fn on_error(ctx: &WorkerContext, reason: &str) {
    let reason = reason.to_string();
    post_to_ui(&ctx.ui, move |ui| {
        if ui.get_is_logged_in() {
            ui.set_status_text(format!("Error: {reason}").into());
        } else {
            ui.set_login_error(format!("Login failed: {reason}").into());
        }
    });
}

fn set_status(ui: &slint::Weak<AppWindow>, text: &'static str) {
    post_to_ui(ui, move |ui| ui.set_status_text(text.into()));
}

/// Runs `f` on the UI thread if the window still exists.
fn post_to_ui(ui: &slint::Weak<AppWindow>, f: impl FnOnce(&AppWindow) + Send + 'static) {
    let ui = ui.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(ui) = ui.upgrade() {
            f(&ui);
        }
    });
}
