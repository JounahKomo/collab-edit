mod callbacks;
mod chat_state;
mod context;
mod cursors;
mod doc_state;
mod net;
mod ui_sync;
mod worker;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicU32, mpsc},
};

use callbacks::register_ui_callbacks;
use chat_state::ClientChatState;
use clap::Parser;
use context::ClientContext;
use cursors::{CursorSender, RemoteCursors, start_cursor_receiver};
use doc_state::ClientDocState;
use net::{ConnectionStream, connect_stream};
use ui_sync::{sync_chat_ui, sync_doc_ui};
use worker::{WorkerContext, start_network_worker};

slint::include_modules!();

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

/// Opens the UDP socket used for cursor sharing, aimed at the server.
fn open_udp_socket(stream: &mut ConnectionStream) -> std::io::Result<Arc<std::net::UdpSocket>> {
    let server_addr = stream.tcp_mut().peer_addr()?;
    let bind_addr = if server_addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = std::net::UdpSocket::bind(bind_addr)?;
    socket.connect(server_addr)?;
    Ok(Arc::new(socket))
}

/// Logs in immediately if a username was given on the command line.
fn login_from_cli(ui: &AppWindow, ctx: &ClientContext, username: Option<String>) {
    match username.filter(|u| !u.trim().is_empty()) {
        Some(user) => {
            ui.set_username(user.clone().into());
            ui.set_is_logged_in(true);
            ui.set_status_text("Connected".into());
            ctx.send_frame(b"USR ", &user);
            ctx.send_frame(b"GET ", "");
        }
        None => ui.set_status_text("Waiting for username".into()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let ca_cert_path = find_ca_cert(cli.ca_cert);

    println!(
        "Connecting to {} (TLS: {})...",
        cli.target,
        if cli.no_tls { "disabled" } else { "enabled" }
    );
    let mut stream = connect_stream(
        &cli.target,
        cli.no_tls,
        ca_cert_path.as_deref(),
        cli.server_name.as_deref(),
    )?;
    println!("Connected successfully!");

    let udp_socket = open_udp_socket(&mut stream)?;
    let cursor_sender = Arc::new(CursorSender::new(Arc::clone(&udp_socket)));
    let own_name = Arc::new(Mutex::new(String::new()));
    let udp_token: Arc<Mutex<Option<String>>> = Arc::default();
    let remote_cursors: RemoteCursors = Arc::default();

    let ui = AppWindow::new()?;
    let doc_state = Arc::new(Mutex::new(ClientDocState::new()));
    let chat_state = Arc::new(Mutex::new(ClientChatState::default()));
    let msg_counter = Arc::new(AtomicU32::new(1));
    let (tx_net, rx_net) = mpsc::channel::<Vec<u8>>();

    sync_doc_ui(&ui, &doc_state.lock().unwrap());
    sync_chat_ui(&ui, &chat_state.lock().unwrap());

    let ctx = ClientContext {
        ui: ui.as_weak(),
        doc_state: Arc::clone(&doc_state),
        chat_state: Arc::clone(&chat_state),
        tx_net,
        msg_counter: Arc::clone(&msg_counter),
        cursor_sender: Arc::clone(&cursor_sender),
    };
    login_from_cli(&ui, &ctx, cli.username);

    start_network_worker(
        stream,
        rx_net,
        WorkerContext {
            ui: ui.as_weak(),
            doc_state,
            chat_state,
            cursor_sender: Arc::clone(&cursor_sender),
            udp_token: Arc::clone(&udp_token),
            msg_counter,
            own_name: Arc::clone(&own_name),
        },
    );
    start_cursor_receiver(
        udp_socket,
        ui.as_weak(),
        remote_cursors,
        own_name,
        cursor_sender,
        udp_token,
    );

    register_ui_callbacks(&ui, ctx.clone());
    ui.run()?;

    // Disconnect cleanly
    ctx.send_frame(b"BYE ", "");
    Ok(())
}
