use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::document::Document;

pub type ClientId = u64;

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
    pub next_server_msg_id: u32,
    /// Message IDs already in use on this connection: the ones the client has
    /// sent in its requests plus the ones the server has allocated. IDs are
    /// connection-specific, so a new server-originated ID must not be in here.
    pub used_ids: HashSet<u32>,
    /// Bytes pushed here are written to the client's socket by its own task.
    pub tx: UnboundedSender<Vec<u8>>,
}

impl ClientHandle {
    pub fn new(tx: UnboundedSender<Vec<u8>>) -> Self {
        ClientHandle {
            username: None,
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
    fn take_server_msg_id(&mut self) -> u32 {
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

/// State shared between all connection tasks.
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

/// Connection-local state. The socket itself is owned by the connection task.
pub struct Client {
    pub id: ClientId,
    pub address: SocketAddr,
    pub read_buf: Vec<u8>,
    pub write_buf: Vec<u8>,
    pub is_closed: bool,
}

impl Client {
    pub fn new(id: ClientId, address: SocketAddr) -> Self {
        Client {
            id,
            address,
            read_buf: Vec::new(),
            write_buf: Vec::new(),
            is_closed: false,
        }
    }

    pub fn process_messages(&mut self, state: &mut State) {
        let State {
            clients,
            metrics,
            document,
            channels,
        } = state;

        loop {
            if self.read_buf.len() < 4 {
                break;
            }
            let len = u32::from_be_bytes(self.read_buf[..4].try_into().unwrap()) as usize;

            if len < 12 {
                warn!("Protocol error: message length too short");
                self.is_closed = true;
                break;
            }

            if self.read_buf.len() < len {
                break;
            }

            let msg_bytes = self.read_buf[..len].to_vec();
            self.read_buf.drain(..len);

            let msg_id = u32::from_be_bytes(msg_bytes[4..8].try_into().unwrap());
            let msg_type = &msg_bytes[8..12];
            let payload = &msg_bytes[12..];

            metrics.messages_received += 1;
            if let Some(me) = clients.get_mut(&self.id) {
                me.used_ids.insert(msg_id);
            }

            match msg_type {
                b"MET " => match serde_json::to_string(&*metrics) {
                    Ok(metrics_json) => {
                        self.write_buf.extend_from_slice(&frame(
                            msg_id,
                            b"MET ",
                            metrics_json.as_bytes(),
                        ));
                    }
                    Err(e) => {
                        error!("Failed to serialize metrics: {}", e);
                        metrics.errors += 1;
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Internal error serializing metrics",
                        ));
                    }
                },
                b"TST " => {
                    self.write_buf.extend_from_slice(&msg_bytes);
                }
                b"USR " => {
                    if let Ok(username_str) = std::str::from_utf8(payload) {
                        let requested_usr = username_str.to_string();

                        let is_taken = clients
                            .values()
                            .any(|c| c.username.as_ref() == Some(&requested_usr));

                        if is_taken {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Username already taken!",
                            ));
                            warn!("Username: {} already taken", username_str);
                        } else {
                            if let Some(me) = clients.get_mut(&self.id) {
                                me.username = Some(requested_usr);
                            }
                            self.write_buf.extend_from_slice(&msg_bytes);
                        }
                    } else {
                        self.write_buf
                            .extend_from_slice(&create_error(msg_id, "Invalid UTF-8 in username"));
                        warn!("Invalid UTF-8 in USR payload");
                        metrics.errors += 1;
                    }
                }
                b"MSG " => {
                    let sender_usr = match clients.get(&self.id).and_then(|c| c.username.clone()) {
                        Some(usr) => usr,
                        None => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Not registered. Send USR first.",
                            ));
                            warn!("Tried to send MSG without username");
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let payload_str = match std::str::from_utf8(payload) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!("Failed parsing of payload: {}", err);
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    if let Some((channel, message_text)) = payload_str.split_once(' ') {
                        // If channel does not exist, create it dynamically
                        if channels.insert(channel.to_string()) {
                            tracing::info!("Created new channel: {}", channel);
                        }

                        // Format the broadcast: <Sender> <Channel> <Message>
                        let broadcast_payload =
                            format!("{} {} {}", sender_usr, channel, message_text);
                        broadcast_to_all(
                            clients,
                            self.id,
                            &mut self.write_buf,
                            b"MSG ",
                            broadcast_payload.as_bytes(),
                        );
                    } else {
                        // Errors still use the client's `msg_id` as requested by the spec
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Invalid MSG syntax. Expected: <channel> <message>",
                        ));
                        warn!("MSG not according to protocol");
                        metrics.errors += 1;
                    }
                }
                b"GET " => {
                    let is_registered = clients.get(&self.id).is_some_and(|c| c.username.is_some());
                    if !is_registered {
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Not registered. Send USR first.",
                        ));
                        warn!("Tried to GET document without username");
                        metrics.errors += 1;
                        continue;
                    }

                    let doc_payload = format!("{}\n{}", document.revision, document.text());
                    self.write_buf.extend_from_slice(&frame(
                        msg_id,
                        b"GET ",
                        doc_payload.as_bytes(),
                    ));
                }
                b"INS " => {
                    let is_registered = clients.get(&self.id).is_some_and(|c| c.username.is_some());
                    if !is_registered {
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Not registered. Send USR first.",
                        ));
                        warn!("Tried to INS without username");
                        metrics.errors += 1;
                        continue;
                    }

                    let payload_str = match std::str::from_utf8(payload) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!("Failed parsing of INS payload: {}", err);
                            metrics.errors += 1;
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid UTF-8 in INS payload",
                            ));
                            continue;
                        }
                    };

                    let (line_str, rest) = match payload_str.split_once(' ') {
                        Some(pair) => pair,
                        None => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid INS syntax. Expected: <line> <col> <text>",
                            ));
                            warn!("INS invalid syntax");
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let (col_str, text_to_insert) = match rest.split_once(' ') {
                        Some(pair) => pair,
                        None => (rest, ""),
                    };

                    let line: usize = match line_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid line number in INS",
                            ));
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let col: usize = match col_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid column number in INS",
                            ));
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    match document.insert(line, col, text_to_insert) {
                        Ok(()) => {
                            // Echo back exact same message with request ID
                            self.write_buf.extend_from_slice(&msg_bytes);

                            // Broadcast updated document state to all registered clients
                            let doc_payload = format!("{}\n{}", document.revision, document.text());
                            broadcast_to_all(
                                clients,
                                self.id,
                                &mut self.write_buf,
                                b"GET ",
                                doc_payload.as_bytes(),
                            );
                        }
                        Err(err_msg) => {
                            self.write_buf
                                .extend_from_slice(&create_error(msg_id, err_msg));
                            warn!("INS failed: {}", err_msg);
                            metrics.errors += 1;
                        }
                    }
                }
                b"DEL " => {
                    let is_registered = clients.get(&self.id).is_some_and(|c| c.username.is_some());
                    if !is_registered {
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Not registered. Send USR first.",
                        ));
                        warn!("Tried to DEL without username");
                        metrics.errors += 1;
                        continue;
                    }

                    let payload_str = match std::str::from_utf8(payload) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!("Failed parsing of DEL payload: {}", err);
                            metrics.errors += 1;
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid UTF-8 in DEL payload",
                            ));
                            continue;
                        }
                    };

                    let mut parts = payload_str.split_whitespace();
                    let (line_str, col_str, count_str) =
                        match (parts.next(), parts.next(), parts.next(), parts.next()) {
                            (Some(l), Some(c), Some(cnt), None) => (l, c, cnt),
                            _ => {
                                self.write_buf.extend_from_slice(&create_error(
                                    msg_id,
                                    "Invalid DEL syntax. Expected: <line> <col> <count>",
                                ));
                                warn!("DEL invalid syntax");
                                metrics.errors += 1;
                                continue;
                            }
                        };

                    let line: usize = match line_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid line number in DEL",
                            ));
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let col: usize = match col_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Invalid column number in DEL",
                            ));
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let count: usize = match count_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            self.write_buf
                                .extend_from_slice(&create_error(msg_id, "Invalid count in DEL"));
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    match document.delete(line, col, count) {
                        Ok(()) => {
                            // Echo back exact same message with request ID
                            self.write_buf.extend_from_slice(&msg_bytes);

                            // Broadcast updated document state to all registered clients
                            let doc_payload = format!("{}\n{}", document.revision, document.text());
                            broadcast_to_all(
                                clients,
                                self.id,
                                &mut self.write_buf,
                                b"GET ",
                                doc_payload.as_bytes(),
                            );
                        }
                        Err(err_msg) => {
                            self.write_buf
                                .extend_from_slice(&create_error(msg_id, err_msg));
                            warn!("DEL failed: {}", err_msg);
                            metrics.errors += 1;
                        }
                    }
                }

                _ => {
                    self.write_buf
                        .extend_from_slice(&create_error(msg_id, "Unknown message type"));
                    warn!("Unknown message type");
                    metrics.errors += 1;
                }
            }
        }
    }
}

/// Broadcasts a server-originated message to the sender and all other registered clients.
fn broadcast_to_all(
    clients: &mut HashMap<ClientId, ClientHandle>,
    self_id: ClientId,
    self_write_buf: &mut Vec<u8>,
    msg_type: &[u8; 4],
    payload: &[u8],
) {
    // 1. Send the broadcast to the sender using a SERVER allocated ID
    if let Some(me) = clients.get_mut(&self_id) {
        let sender_msg_id = me.take_server_msg_id();
        self_write_buf.extend_from_slice(&frame(sender_msg_id, msg_type, payload));
    }

    // 2. Broadcast to all OTHER registered users; each one's task does the write
    for (other_id, other) in clients.iter_mut() {
        if *other_id == self_id || other.username.is_none() {
            continue;
        }
        let other_msg_id = other.take_server_msg_id();
        let other_msg = frame(other_msg_id, msg_type, payload);
        let _ = other.tx.send(other_msg);
    }
}

/// Builds a wire frame: `len (u32 BE) | id (u32 BE) | type (4 bytes) | payload`.
fn frame(id: u32, msg_type: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let len = (12 + payload.len()) as u32;
    let mut out = Vec::with_capacity(len as usize);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(msg_type);
    out.extend_from_slice(payload);
    out
}

pub fn create_error(id: u32, error: &str) -> Vec<u8> {
    frame(id, b"ERR ", error.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn error_creation_working() {
        let id = 12345;
        let error_message = "Very serious error";
        let error_vec: Vec<u8> = create_error(id, error_message);
        let len = error_vec.len();
        assert_eq!(
            u32::from_be_bytes(error_vec[..4].try_into().unwrap()),
            len as u32
        );
        assert_eq!(u32::from_be_bytes(error_vec[4..8].try_into().unwrap()), id);
        assert_eq!(str::from_utf8(&error_vec[12..len]), Ok(error_message));
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }


    #[test]
    fn dynamic_channel_creation() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx_a));
        state.clients.insert(2, ClientHandle::new(tx_b));

        let mut a = Client::new(1, addr());
        let mut b = Client::new(2, addr());

        b.read_buf.extend_from_slice(&frame(1, b"USR ", b"bob"));
        b.process_messages(&mut state);
        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        a.process_messages(&mut state);
        a.write_buf.clear();

        // Send message to unrecognized channel "announcements"
        a.read_buf
            .extend_from_slice(&frame(3, b"MSG ", b"announcements Hello all!"));
        a.process_messages(&mut state);

        // Verify channel was created
        assert!(state.channels.contains("announcements"));

        // Sender receives echo with server-allocated id
        assert_eq!(a.write_buf, frame(2, b"MSG ", b"alice announcements Hello all!"));

        // Bob receives the broadcast message on "announcements"
        assert_eq!(
            rx_b.try_recv().unwrap(),
            frame(2, b"MSG ", b"alice announcements Hello all!")
        );
    }

    #[test]
    fn broadcast_reaches_other_registered_clients() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx_a));
        state.clients.insert(2, ClientHandle::new(tx_b));

        let mut a = Client::new(1, addr());
        let mut b = Client::new(2, addr());

        b.read_buf.extend_from_slice(&frame(1, b"USR ", b"bob"));
        b.process_messages(&mut state);
        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        a.process_messages(&mut state);
        a.write_buf.clear();

        a.read_buf
            .extend_from_slice(&frame(3, b"MSG ", b"general hi"));
        a.process_messages(&mut state);

        // Sender gets its echo with a server-allocated id.
        assert_eq!(a.write_buf, frame(2, b"MSG ", b"alice general hi"));
        // Other client gets the broadcast through its channel.
        assert_eq!(
            rx_b.try_recv().unwrap(),
            frame(2, b"MSG ", b"alice general hi")
        );
    }

    #[test]
    fn server_ids_skip_ids_used_by_the_client() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx_a));
        let mut a = Client::new(1, addr());

        // The client itself uses the even IDs 2 and 4 in its requests.
        a.read_buf.extend_from_slice(&frame(2, b"USR ", b"alice"));
        a.process_messages(&mut state);
        a.write_buf.clear();
        a.read_buf
            .extend_from_slice(&frame(4, b"MSG ", b"general one"));
        a.process_messages(&mut state);
        assert_eq!(a.write_buf, frame(6, b"MSG ", b"alice general one"));
        a.write_buf.clear();

        // A later broadcast gets yet another unused ID, never a repeat.
        a.read_buf
            .extend_from_slice(&frame(8, b"MSG ", b"general two"));
        a.process_messages(&mut state);
        assert_eq!(a.write_buf, frame(10, b"MSG ", b"alice general two"));
    }

    #[test]
    fn duplicate_username_rejected() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        let (tx_b, _rx_b) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx_a));
        state.clients.insert(2, ClientHandle::new(tx_b));
        let mut a = Client::new(1, addr());
        let mut b = Client::new(2, addr());

        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        a.process_messages(&mut state);
        b.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        b.process_messages(&mut state);

        assert_eq!(b.write_buf, create_error(1, "Username already taken!"));
    }

    #[test]
    fn get_ins_del_flow_and_broadcast() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx_a));
        state.clients.insert(2, ClientHandle::new(tx_b));

        let mut a = Client::new(1, addr());
        let mut b = Client::new(2, addr());

        // Register both users
        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        a.process_messages(&mut state);
        a.write_buf.clear();

        b.read_buf.extend_from_slice(&frame(1, b"USR ", b"bob"));
        b.process_messages(&mut state);
        b.write_buf.clear();

        // Initial GET returns revision 0 and empty document
        a.read_buf.extend_from_slice(&frame(3, b"GET ", b""));
        a.process_messages(&mut state);
        assert_eq!(a.write_buf, frame(3, b"GET ", b"0\n"));
        a.write_buf.clear();

        // Alice inserts text at (0, 0)
        a.read_buf
            .extend_from_slice(&frame(5, b"INS ", b"0 0 Hello World"));
        a.process_messages(&mut state);

        // Alice receives: 1) Echo of INS with req id 5, 2) Broadcast of GET with server id 2
        let mut expected_a = Vec::new();
        expected_a.extend_from_slice(&frame(5, b"INS ", b"0 0 Hello World"));
        expected_a.extend_from_slice(&frame(2, b"GET ", b"1\nHello World"));
        assert_eq!(a.write_buf, expected_a);
        a.write_buf.clear();

        // Bob receives the broadcast of GET with server id 2
        assert_eq!(
            rx_b.try_recv().unwrap(),
            frame(2, b"GET ", b"1\nHello World")
        );

        // Alice deletes "World" (5 chars at col 6)
        a.read_buf.extend_from_slice(&frame(7, b"DEL ", b"0 6 5"));
        a.process_messages(&mut state);

        let mut expected_a_del = Vec::new();
        expected_a_del.extend_from_slice(&frame(7, b"DEL ", b"0 6 5"));
        expected_a_del.extend_from_slice(&frame(4, b"GET ", b"2\nHello "));
        assert_eq!(a.write_buf, expected_a_del);
        a.write_buf.clear();

        // Bob receives delete broadcast with server id 4
        assert_eq!(rx_b.try_recv().unwrap(), frame(4, b"GET ", b"2\nHello "));
    }

    #[test]
    fn ins_and_del_error_handling() {
        let mut state = State::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        state.clients.insert(1, ClientHandle::new(tx));
        let mut a = Client::new(1, addr());

        // Unregistered request fails
        a.read_buf.extend_from_slice(&frame(1, b"GET ", b""));
        a.process_messages(&mut state);
        assert_eq!(
            a.write_buf,
            create_error(1, "Not registered. Send USR first.")
        );
        a.write_buf.clear();

        // Register
        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        a.process_messages(&mut state);
        a.write_buf.clear();

        // Invalid INS syntax
        a.read_buf.extend_from_slice(&frame(3, b"INS ", b"bad"));
        a.process_messages(&mut state);
        assert_eq!(
            a.write_buf,
            create_error(3, "Invalid INS syntax. Expected: <line> <col> <text>")
        );
        a.write_buf.clear();

        // Exceeding MAX_COLUMNS
        a.read_buf
            .extend_from_slice(&frame(5, b"INS ", b"0 85 abc"));
        a.process_messages(&mut state);
        assert_eq!(
            a.write_buf,
            create_error(5, "Column exceeds maximum line width")
        );
        a.write_buf.clear();

        // Invalid DEL syntax
        a.read_buf.extend_from_slice(&frame(7, b"DEL ", b"0 0"));
        a.process_messages(&mut state);
        assert_eq!(
            a.write_buf,
            create_error(7, "Invalid DEL syntax. Expected: <line> <col> <count>")
        );
        a.write_buf.clear();

        // DEL out of bounds
        a.read_buf.extend_from_slice(&frame(9, b"DEL ", b"5 0 1"));
        a.process_messages(&mut state);
        assert_eq!(a.write_buf, create_error(9, "Line out of bounds"));
    }
}
