use std::{
    fs::File,
    io::{self, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Parser;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use server::client::{Client, ClientHandle, ClientId, SharedState};

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
    #[arg(short, long, default_value = "0.0.0.0", env = "HOST")]
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
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    let acceptor: Option<TlsAcceptor> = if cli.no_tls {
        info!("TLS disabled via --no-tls");
        None
    } else {
        match load_tls_acceptor(&cli.cert, &cli.key) {
            Ok(acc) => {
                info!(
                    "Loaded TLS configuration from cert={:?}, key={:?}",
                    cli.cert, cli.key
                );
                Some(acc)
            }
            Err(e) => {
                error!(
                    "Failed to load TLS credentials (cert: {:?}, key: {:?}): {e}",
                    cli.cert, cli.key
                );
                return Err(e);
            }
        }
    };

    let address: SocketAddr = format!("{}:{}", cli.host, cli.port)
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let listener = TcpListener::bind(address)
        .await
        .inspect_err(|e| error!("Failed to bind TCP listener to {address}: {e}"))?;

    let state = SharedState::default();
    let mut next_client_id: ClientId = 1;

    info!(
        "Server listening on {} (TLS: {})",
        address,
        if acceptor.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                info!("Accepted TCP connection from: {}", peer);
                let id = next_client_id;
                next_client_id += 1;

                state.lock().metrics.connections_total += 1;

                if let Err(e) = socket.set_nodelay(true) {
                    warn!("Failed to set TCP_NODELAY for {peer}: {e}");
                }

                let acceptor = acceptor.clone();
                let state_clone = state.clone();

                tokio::spawn(async move {
                    if let Some(acceptor) = acceptor {
                        match acceptor.accept(socket).await {
                            Ok(tls_stream) => {
                                handle_connection(tls_stream, peer, id, state_clone).await;
                            }
                            Err(e) => {
                                warn!("TLS handshake failed with {peer}: {e}");
                                state_clone.lock().metrics.errors += 1;
                            }
                        }
                    } else {
                        handle_connection(socket, peer, id, state_clone).await;
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {e}");
                state.lock().metrics.errors += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_connection<S>(mut stream: S, address: SocketAddr, id: ClientId, state: SharedState)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.lock().clients.insert(id, ClientHandle::new(tx));

    let mut client = Client::new(id, address);

    loop {
        tokio::select! {
            res = stream.read_buf(&mut client.read_buf) => match res {
                Ok(0) => client.is_closed = true,
                Ok(_) => {
                    let mut guard = state.lock();
                    client.process_messages(&mut guard);
                }
                Err(err) => {
                    client.is_closed = true;
                    warn!("Failed reading from client {}: {}", client.address, err);
                    state.lock().metrics.errors += 1;
                }
            },
            Some(msg) = rx.recv() => client.write_buf.extend_from_slice(&msg),
        }

        if !client.write_buf.is_empty() {
            if let Err(err) = stream.write_all(&client.write_buf).await {
                client.is_closed = true;
                warn!("Failed writing to client {}: {}", client.address, err);
                state.lock().metrics.errors += 1;
            }
            client.write_buf.clear();
        }

        if client.is_closed {
            break;
        }
    }

    state.lock().clients.remove(&id);
    info!("Connection closed: {}", client.address);
}

fn load_tls_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("No certificates found in {:?}", path),
        ));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("No private key found in {:?}", path),
        )
    })?;
    Ok(key)
}
