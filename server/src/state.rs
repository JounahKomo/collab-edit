//! State shared between all connection tasks and the UDP task.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use crate::{document::Document, protocol::frame};

pub type ClientId = u64;

/// How many broadcast frames may be queued for one client. A client that stops
/// reading would otherwise make the server buffer without bound; once its queue
/// is full further broadcasts to it are dropped (document snapshots are full
/// state, so the next one repairs the loss).
pub const OUTBOX_CAPACITY: usize = 256;

#[derive(Debug, Default, Serialize)]
pub struct ServerMetrics {
    pub connections_total: u64,
    pub messages_received: u64,
    pub errors: u64,
}

/// Per-connection entry in the shared registry. Used by other connections to
/// check username uniqueness and to broadcast messages.
pub struct ClientHandle {
    pub username: Option<String>,
    /// Token announced over TCP with `UDP <token>`; a hello datagram carrying
    /// it binds the datagram's real source address to this client.
    pub udp_token: Option<String>,
    /// UDP endpoint learned from the hello datagram (works through NAT); only
    /// datagrams from exactly this address count as this client's cursor.
    pub udp_addr: Option<SocketAddr>,
    /// Last cursor position (line, col) reported over UDP.
    pub cursor: Option<(usize, usize)>,
    pub next_server_msg_id: u32,
    /// Message IDs already in use on this connection: the ones the client has
    /// sent in its requests plus the ones the server has allocated. IDs are
    /// connection-specific, so a new server-originated ID must not be in here.
    pub used_ids: HashSet<u32>,
    /// Bytes pushed here are written to the client's socket by its own task.
    pub tx: Sender<Vec<u8>>,
}

impl ClientHandle {
    pub fn new(tx: Sender<Vec<u8>>) -> Self {
        ClientHandle {
            username: None,
            udp_token: None,
            udp_addr: None,
            cursor: None,
            next_server_msg_id: 2,
            used_ids: HashSet::new(),
            tx,
        }
    }

    /// Allocates a message ID that is not yet used on this connection, and
    /// marks it as used so it is never handed out twice.
    ///
    /// IDs are handed out in steps of two (2, 4, 6, ...) so that they rarely
    /// meet the IDs of well-behaved clients that count 1, 3, 5, ..., but any
    /// ID the client has actually used is skipped regardless of parity.
    pub fn take_server_msg_id(&mut self) -> u32 {
        // Terminates because only a finite number of IDs can have been used.
        loop {
            let id = self.next_server_msg_id;
            self.next_server_msg_id = self.next_server_msg_id.wrapping_add(2);
            if self.used_ids.insert(id) {
                return id;
            }
        }
    }
}

/// State shared between all tasks.
pub struct State {
    pub clients: HashMap<ClientId, ClientHandle>,
    pub metrics: ServerMetrics,
    pub document: Document,
    pub channels: HashSet<String>,
}

impl Default for State {
    fn default() -> Self {
        let mut channels = HashSet::new();
        channels.insert("general".to_string());
        Self {
            clients: HashMap::new(),
            metrics: ServerMetrics::default(),
            document: Document::default(),
            channels,
        }
    }
}

impl State {
    /// Broadcasts a server-originated message to the sender and all other
    /// registered clients. The sender's copy is appended to `sender_out` (it is
    /// written by the sender's own connection loop); the others are pushed
    /// through their channels.
    pub fn broadcast_to_all(
        &mut self,
        sender_id: ClientId,
        sender_out: &mut Vec<u8>,
        msg_type: &[u8; 4],
        payload: &[u8],
    ) {
        if let Some(me) = self.clients.get_mut(&sender_id) {
            let id = me.take_server_msg_id();
            sender_out.extend_from_slice(&frame(id, msg_type, payload));
        }

        for (other_id, other) in self.clients.iter_mut() {
            if *other_id == sender_id || other.username.is_none() {
                continue;
            }
            let id = other.take_server_msg_id();
            if other.tx.try_send(frame(id, msg_type, payload)).is_err() {
                debug!(
                    client = other_id,
                    "Outbox full or closed; dropped broadcast"
                );
            }
        }
    }
}

/// Shared state handle. The lock is only ever held in synchronous code (never
/// across an `.await`), so a std mutex is the right choice.
#[derive(Clone, Default)]
pub struct SharedState(Arc<Mutex<State>>);

impl SharedState {
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
