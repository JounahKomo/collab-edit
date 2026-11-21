use std::{io, net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use tokio::net::{TcpListener, UdpSocket};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use server::{
    connection::run_accept_loop, state::SharedState, tls::load_tls_acceptor, udp::spawn_udp_tasks,
};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Collaborative document editor / chat server with TLS"
)]
struct Cli {
    /// Port to listen on
    #[arg(short, long, default_value_t = 5011, env = "PORT")]
    port: u16,

    /// Bind IP address
    #[arg(short = 'H', long, default_value = "0.0.0.0", env = "HOST")]
    host: String,

    /// Path to the TLS certificate (.crt / .pem)
    #[arg(long, default_value = "/run/tls/server.crt", env = "TLS_CERT_PATH")]
    cert: PathBuf,

    /// Path to the TLS private key (.key / .pem)
    #[arg(long, default_value = "/run/tls/server.key", env = "TLS_KEY_PATH")]
    key: PathBuf,

    /// Disable TLS and run plain TCP (useful for legacy test runs)
    #[arg(long, env = "DISABLE_TLS")]
    no_tls: bool,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let acceptor = build_acceptor(&cli)?;
    let address: SocketAddr = format!("{}:{}", cli.host, cli.port)
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let listener = TcpListener::bind(address)
        .await
        .inspect_err(|e| error!("Failed to bind TCP listener to {address}: {e}"))?;
    // Cursor updates (`CUR`) travel over UDP on the same port number as TCP.
    let udp = UdpSocket::bind(address)
        .await
        .inspect_err(|e| error!("Failed to bind UDP socket to {address}: {e}"))?;

    let state = SharedState::default();
    info!(
        "Server listening on {} (TLS: {})",
        address,
        if acceptor.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    spawn_udp_tasks(Arc::new(udp), state.clone());
    run_accept_loop(listener, acceptor, state).await;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Loads the TLS configuration unless TLS is disabled.
fn build_acceptor(cli: &Cli) -> io::Result<Option<TlsAcceptor>> {
    if cli.no_tls {
        info!("TLS disabled via --no-tls");
        return Ok(None);
    }
    match load_tls_acceptor(&cli.cert, &cli.key) {
        Ok(acceptor) => {
            info!(
                "Loaded TLS configuration from cert={:?}, key={:?}",
                cli.cert, cli.key
            );
            Ok(Some(acceptor))
        }
        Err(e) => {
            error!(
                "Failed to load TLS credentials (cert: {:?}, key: {:?}): {e}",
                cli.cert, cli.key
            );
            Err(e)
        }
    }
}
