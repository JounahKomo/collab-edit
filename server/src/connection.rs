//! TCP accept loop and the per-connection I/O task.

use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_rustls::TlsAcceptor;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::{
    client::Client,
    state::{ClientHandle, ClientId, OUTBOX_CAPACITY, SharedState},
};

/// A TLS handshake must finish within this time.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// A connection must register with `USR` within this time, or it is closed.
/// (Registered clients may stay idle; only anonymous connections are limited.)
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Accepts connections forever, spawning one task per client.
pub async fn run_accept_loop(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    state: SharedState,
) {
    let mut next_client_id: ClientId = 1;
    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                let id = next_client_id;
                next_client_id += 1;
                state.lock().metrics.connections_total += 1;
                let span = info_span!("conn", id, %peer);
                info!(parent: &span, "Accepted TCP connection");
                tokio::spawn(
                    serve_socket(socket, peer, id, acceptor.clone(), state.clone())
                        .instrument(span),
                );
            }
            Err(e) => {
                error!("Error accepting connection: {e}");
                state.lock().metrics.errors += 1;
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Performs the optional TLS handshake and then runs the connection.
async fn serve_socket(
    socket: TcpStream,
    peer: SocketAddr,
    id: ClientId,
    acceptor: Option<TlsAcceptor>,
    state: SharedState,
) {
    if let Err(e) = socket.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY: {e}");
    }

    match acceptor {
        Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(socket)).await {
            Ok(Ok(tls_stream)) => handle_connection(tls_stream, peer, id, state).await,
            Ok(Err(e)) => {
                warn!("TLS handshake failed: {e}");
                state.lock().metrics.errors += 1;
            }
            Err(_) => {
                warn!("TLS handshake timed out");
                state.lock().metrics.errors += 1;
            }
        },
        None => handle_connection(socket, peer, id, state).await,
    }
}

/// Removes the client from the shared registry when dropped, so the entry is
/// released on every exit path, including a panic while handling a request.
struct Registration {
    id: ClientId,
    state: SharedState,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.state.lock().clients.remove(&self.id);
    }
}

async fn handle_connection<S>(mut stream: S, address: SocketAddr, id: ClientId, state: SharedState)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(OUTBOX_CAPACITY);
    state.lock().clients.insert(id, ClientHandle::new(tx));
    let _registration = Registration {
        id,
        state: state.clone(),
    };

    let mut client = Client::new(id, address);
    let registration_deadline = sleep(REGISTRATION_TIMEOUT);
    tokio::pin!(registration_deadline);
    let mut registered = false;

    while !client.is_closed {
        tokio::select! {
            res = stream.read_buf(&mut client.read_buf) => match res {
                Ok(0) => {
                    debug!("Peer closed the connection");
                    client.is_closed = true;
                }
                Ok(_) => client.process_messages(&mut state.lock()),
                Err(err) => {
                    client.is_closed = true;
                    warn!("Failed reading from client: {err}");
                    state.lock().metrics.errors += 1;
                }
            },
            Some(msg) = rx.recv() => client.write_buf.extend_from_slice(&msg),
            () = &mut registration_deadline, if !registered => {
                warn!("Closing connection that did not register in time");
                state.lock().metrics.errors += 1;
                client.is_closed = true;
            }
        }

        registered = registered || is_registered(&state, id);
        flush_write_buffer(&mut stream, &mut client, &state).await;
    }

    info!("Connection closed");
}

fn is_registered(state: &SharedState, id: ClientId) -> bool {
    state
        .lock()
        .clients
        .get(&id)
        .is_some_and(|c| c.username.is_some())
}

/// Writes any queued output to the socket, closing the client on failure.
async fn flush_write_buffer<S>(stream: &mut S, client: &mut Client, state: &SharedState)
where
    S: AsyncWrite + Unpin,
{
    if client.write_buf.is_empty() {
        return;
    }
    if let Err(err) = stream.write_all(&client.write_buf).await {
        client.is_closed = true;
        warn!("Failed writing to client: {err}");
        state.lock().metrics.errors += 1;
    }
    client.write_buf.clear();
}
