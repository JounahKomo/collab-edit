//! TCP accept loop and the per-connection I/O task.

use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::{
    client::Client,
    state::{ClientHandle, ClientId, SharedState},
};

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
                info!("Accepted TCP connection from: {}", peer);
                let id = next_client_id;
                next_client_id += 1;
                state.lock().metrics.connections_total += 1;
                tokio::spawn(serve_socket(
                    socket,
                    peer,
                    id,
                    acceptor.clone(),
                    state.clone(),
                ));
            }
            Err(e) => {
                error!("Error accepting connection: {e}");
                state.lock().metrics.errors += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
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
        warn!("Failed to set TCP_NODELAY for {peer}: {e}");
    }

    match acceptor {
        Some(acceptor) => match acceptor.accept(socket).await {
            Ok(tls_stream) => handle_connection(tls_stream, peer, id, state).await,
            Err(e) => {
                warn!("TLS handshake failed with {peer}: {e}");
                state.lock().metrics.errors += 1;
            }
        },
        None => handle_connection(socket, peer, id, state).await,
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
                Ok(_) => client.process_messages(&mut state.lock()),
                Err(err) => {
                    client.is_closed = true;
                    warn!("Failed reading from client {}: {}", client.address, err);
                    state.lock().metrics.errors += 1;
                }
            },
            Some(msg) = rx.recv() => client.write_buf.extend_from_slice(&msg),
        }

        flush_write_buffer(&mut stream, &mut client, &state).await;

        if client.is_closed {
            break;
        }
    }

    state.lock().clients.remove(&id);
    info!("Connection closed: {}", client.address);
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
        warn!("Failed writing to client {}: {}", client.address, err);
        state.lock().metrics.errors += 1;
    }
    client.write_buf.clear();
}
