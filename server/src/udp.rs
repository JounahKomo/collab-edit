//! UDP side of the server: cursor sharing (`UDP ` hello and `CUR ` datagrams).

use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::net::UdpSocket;
use tracing::{info, warn};

use crate::{
    document::MAX_COLUMNS,
    protocol::{frame, parse_frame},
    state::{ClientId, SharedState, State},
};

/// How often every known cursor is re-sent to every registered client.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

/// A datagram to send: destination and complete frame.
pub type Outgoing = (SocketAddr, Vec<u8>);

/// Handles one UDP datagram; anything invalid is dropped silently (UDP has no
/// error channel). Two kinds are accepted:
/// * `UDP <token>` hello: binds the datagram's source address to the client that
///   announced the token over TCP, and is acknowledged with a `UDP ` datagram.
///   Learning the address from the datagram itself makes this work through NAT.
/// * `CUR <line> <col>` from a bound address: stored and relayed as
///   `CUR <username> <line> <col>` to every other client with a bound endpoint.
///
/// Returns the datagrams to send.
pub fn handle_udp_datagram(state: &mut State, src: SocketAddr, data: &[u8]) -> Vec<Outgoing> {
    state.metrics.messages_received += 1;

    let reply = parse_frame(data)
        .and_then(|f| Some((f, std::str::from_utf8(f.payload).ok()?)))
        .and_then(|(f, text)| match f.msg_type {
            b"UDP " => handle_hello(state, src, f.id, text),
            b"CUR " => handle_cursor(state, src, text),
            _ => None,
        });

    reply.unwrap_or_else(|| {
        state.metrics.errors += 1;
        Vec::new()
    })
}

/// Binds `src` to the client that announced `token`, and acknowledges it.
fn handle_hello(
    state: &mut State,
    src: SocketAddr,
    msg_id: u32,
    token: &str,
) -> Option<Vec<Outgoing>> {
    let token = token.trim();
    let client = state
        .clients
        .values_mut()
        .find(|c| c.username.is_some() && c.udp_token.as_deref() == Some(token))?;

    if client.udp_addr != Some(src) {
        info!("UDP endpoint for {:?} is {src}", client.username);
    }
    client.udp_addr = Some(src);
    Some(vec![(src, frame(msg_id, b"UDP ", b""))])
}

/// Parses `<line> <col>`; the column must fit on a page.
fn parse_cursor_position(text: &str) -> Option<(usize, usize)> {
    let mut parts = text.split_whitespace();
    let line: usize = parts.next()?.parse().ok()?;
    let col: usize = parts.next()?.parse().ok()?;
    (parts.next().is_none() && col <= MAX_COLUMNS).then_some((line, col))
}

/// Stores the sender's cursor and relays it to every other bound endpoint.
fn handle_cursor(state: &mut State, src: SocketAddr, text: &str) -> Option<Vec<Outgoing>> {
    let (line, col) = parse_cursor_position(text)?;
    let (sender_id, username) = state
        .clients
        .iter()
        .find(|(_, c)| c.udp_addr == Some(src))
        .and_then(|(id, c)| Some((*id, c.username.clone()?)))?;

    if let Some(me) = state.clients.get_mut(&sender_id) {
        me.cursor = Some((line, col));
    }

    let payload = format!("{username} {line} {col}");
    let mut out = Vec::new();
    for (id, other) in state.clients.iter_mut() {
        if *id == sender_id {
            continue;
        }
        if let (Some(addr), Some(_)) = (other.udp_addr, &other.username) {
            let msg_id = other.take_server_msg_id();
            out.push((addr, frame(msg_id, b"CUR ", payload.as_bytes())));
        }
    }
    Some(out)
}

/// Builds a snapshot of every known cursor for every client with a registered
/// UDP endpoint (each recipient gets all cursors except its own). Sent
/// periodically so late joiners and clients that lost datagrams catch up, and
/// so clients can expire cursors of users that have left.
pub fn cursor_snapshot(state: &mut State) -> Vec<Outgoing> {
    let cursors: Vec<(ClientId, String)> = state
        .clients
        .iter()
        .filter_map(|(id, c)| {
            let (line, col) = c.cursor?;
            Some((*id, format!("{} {line} {col}", c.username.as_ref()?)))
        })
        .collect();

    let mut out = Vec::new();
    for (recipient_id, recipient) in state.clients.iter_mut() {
        let (Some(addr), Some(_)) = (recipient.udp_addr, &recipient.username) else {
            continue;
        };
        for (cursor_owner, payload) in &cursors {
            if cursor_owner != recipient_id {
                let msg_id = recipient.take_server_msg_id();
                out.push((addr, frame(msg_id, b"CUR ", payload.as_bytes())));
            }
        }
    }
    out
}

/// Spawns the UDP receive loop and the periodic cursor snapshot task.
pub fn spawn_udp_tasks(socket: Arc<UdpSocket>, state: SharedState) {
    tokio::spawn(snapshot_loop(Arc::clone(&socket), state.clone()));
    tokio::spawn(receive_loop(socket, state));
}

async fn snapshot_loop(socket: Arc<UdpSocket>, state: SharedState) {
    let mut tick = tokio::time::interval(SNAPSHOT_INTERVAL);
    loop {
        tick.tick().await;
        let out = cursor_snapshot(&mut state.lock());
        send_all(&socket, out).await;
    }
}

async fn receive_loop(socket: Arc<UdpSocket>, state: SharedState) {
    let mut buf = [0u8; 2048];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, src)) => {
                let out = handle_udp_datagram(&mut state.lock(), src, &buf[..n]);
                send_all(&socket, out).await;
            }
            Err(e) => warn!("UDP receive error: {e}"),
        }
    }
}

async fn send_all(socket: &UdpSocket, datagrams: Vec<Outgoing>) {
    for (addr, data) in datagrams {
        if let Err(e) = socket.send_to(&data, addr).await {
            warn!("Failed sending UDP datagram to {addr}: {e}");
        }
    }
}
