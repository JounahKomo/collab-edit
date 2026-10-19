use std::{io, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use server::client::{Client, ClientHandle, ClientId, SharedState};

#[tokio::main]
async fn main() -> io::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let address: SocketAddr = "0.0.0.0:5015".parse().unwrap();
    let listener = TcpListener::bind(address)
        .await
        .inspect_err(|e| error!("Failed to bind TCP listener to {address}: {e}"))?;

    let state = SharedState::default();
    let mut next_client_id: ClientId = 1;

    info!("Server listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                info!("Accepted connection from: {}", peer);
                let id = next_client_id;
                next_client_id += 1;

                state.lock().metrics.connections_total += 1;

                // Small request/response frames: don't let Nagle delay them.
                if let Err(e) = socket.set_nodelay(true) {
                    warn!("Failed to set TCP_NODELAY for {peer}: {e}");
                }

                tokio::spawn(handle_connection(socket, peer, id, state.clone()));
            }
            Err(e) => {
                error!("Error accepting connection: {e}");
                state.lock().metrics.errors += 1;
                // Avoid a hot loop on persistent errors such as running out of file descriptors.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    address: SocketAddr,
    id: ClientId,
    state: SharedState,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.lock().clients.insert(id, ClientHandle::new(tx));

    let mut client = Client::new(id, address);

    loop {
        tokio::select! {
            res = socket.read_buf(&mut client.read_buf) => match res {
                Ok(0) => client.is_closed = true,
                Ok(_) => {
                    // The lock is held only for this synchronous call, never across an await.
                    let mut guard = state.lock();
                    client.process_messages(&mut guard);
                }
                Err(err) => {
                    client.is_closed = true;
                    warn!("Failed reading from client: {}", err);
                    state.lock().metrics.errors += 1;
                }
            },
            // Data broadcast by other connections.
            Some(msg) = rx.recv() => client.write_buf.extend_from_slice(&msg),
        }

        // Flush everything pending, including responses queued before a close.
        if !client.write_buf.is_empty() {
            if let Err(err) = socket.write_all(&client.write_buf).await {
                client.is_closed = true;
                warn!("Failed writing to client: {}", err);
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
