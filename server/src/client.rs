//! Per-connection TCP session: frame extraction and request dispatch.

use std::net::SocketAddr;

use tracing::warn;

use crate::{
    handlers::{Context, Request, dispatch},
    protocol::{HEADER_LEN, MAX_FRAME_LEN},
    state::{ClientId, State},
};

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

    /// Handles every complete frame in `read_buf`, appending replies to
    /// `write_buf`. A malformed length closes the connection.
    pub fn process_messages(&mut self, state: &mut State) {
        while let Some(msg_bytes) = self.take_frame() {
            let id = u32::from_be_bytes([msg_bytes[4], msg_bytes[5], msg_bytes[6], msg_bytes[7]]);
            state.metrics.messages_received += 1;
            if let Some(me) = state.clients.get_mut(&self.id) {
                me.used_ids.insert(id);
            }

            let request = Request {
                id,
                msg_type: &msg_bytes[8..HEADER_LEN],
                payload: &msg_bytes[HEADER_LEN..],
                raw: &msg_bytes,
            };
            let mut ctx = Context {
                client_id: self.id,
                state,
                out: &mut self.write_buf,
            };
            dispatch(&mut ctx, &request);
        }
    }

    /// Removes and returns the next complete frame from `read_buf`, or `None`
    /// if more bytes are needed (or the connection was closed for a bad length).
    fn take_frame(&mut self) -> Option<Vec<u8>> {
        if self.read_buf.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes([
            self.read_buf[0],
            self.read_buf[1],
            self.read_buf[2],
            self.read_buf[3],
        ]) as usize;
        if !(HEADER_LEN..=MAX_FRAME_LEN).contains(&len) {
            warn!(
                client = self.id,
                "Protocol error: invalid frame length {len} (allowed {HEADER_LEN}..={MAX_FRAME_LEN}); closing connection"
            );
            self.is_closed = true;
            self.read_buf.clear();
            return None;
        }
        if self.read_buf.len() < len {
            return None;
        }
        Some(self.read_buf.drain(..len).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{create_error, frame},
        state::ClientHandle,
        udp::{cursor_snapshot, handle_udp_datagram},
    };
    use tokio::sync::mpsc;

    fn addr() -> SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }

    #[test]
    fn oversized_or_short_frame_length_closes_the_connection() {
        let mut state = State::default();
        let (tx, _rx) = mpsc::channel(16);
        state.clients.insert(1, ClientHandle::new(tx));

        let mut huge = Client::new(1, addr());
        huge.read_buf
            .extend_from_slice(&(MAX_FRAME_LEN as u32 + 1).to_be_bytes());
        huge.process_messages(&mut state);
        assert!(huge.is_closed);
        assert!(huge.read_buf.is_empty());

        let mut short = Client::new(1, addr());
        short.read_buf.extend_from_slice(&5u32.to_be_bytes());
        short.process_messages(&mut state);
        assert!(short.is_closed);
    }

    #[test]
    fn partial_frames_are_buffered_until_complete() {
        let mut state = State::default();
        let (tx, _rx) = mpsc::channel(16);
        state.clients.insert(1, ClientHandle::new(tx));
        let mut a = Client::new(1, addr());

        let tst = frame(1, b"TST ", b"hello");
        a.read_buf.extend_from_slice(&tst[..7]);
        a.process_messages(&mut state);
        assert!(a.write_buf.is_empty() && !a.is_closed);
        a.read_buf.extend_from_slice(&tst[7..]);
        a.process_messages(&mut state);
        assert_eq!(a.write_buf, tst);
    }

    #[test]
    fn dynamic_channel_creation() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::channel(16);
        let (tx_b, mut rx_b) = mpsc::channel(16);
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
        assert_eq!(
            a.write_buf,
            frame(2, b"MSG ", b"alice announcements Hello all!")
        );

        // Bob receives the broadcast message on "announcements"
        assert_eq!(
            rx_b.try_recv().unwrap(),
            frame(2, b"MSG ", b"alice announcements Hello all!")
        );
    }

    #[test]
    fn broadcast_reaches_other_registered_clients() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::channel(16);
        let (tx_b, mut rx_b) = mpsc::channel(16);
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
    fn udp_cur_relayed_to_other_registered_endpoints() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::channel(16);
        let (tx_b, _rx_b) = mpsc::channel(16);
        state.clients.insert(1, ClientHandle::new(tx_a));
        state.clients.insert(2, ClientHandle::new(tx_b));
        let mut a = Client::new(1, "10.0.0.1:1000".parse().unwrap());
        let mut b = Client::new(2, "10.0.0.2:2000".parse().unwrap());
        a.read_buf.extend_from_slice(&frame(1, b"USR ", b"alice"));
        b.read_buf.extend_from_slice(&frame(1, b"USR ", b"bob"));
        a.read_buf.extend_from_slice(&frame(3, b"UDP ", b"tokA"));
        b.read_buf.extend_from_slice(&frame(3, b"UDP ", b"tokB"));
        a.process_messages(&mut state);
        b.process_messages(&mut state);

        // Hello datagrams bind the real (NATed) source addresses.
        let a_udp: SocketAddr = "192.168.65.1:41000".parse().unwrap();
        let b_udp: SocketAddr = "192.168.65.1:41001".parse().unwrap();
        let ack = handle_udp_datagram(&mut state, a_udp, &frame(5, b"UDP ", b"tokA"));
        assert_eq!(ack, vec![(a_udp, frame(5, b"UDP ", b""))]);
        handle_udp_datagram(&mut state, b_udp, &frame(5, b"UDP ", b"tokB"));
        assert!(handle_udp_datagram(&mut state, a_udp, &frame(5, b"UDP ", b"nope")).is_empty());

        let out = handle_udp_datagram(&mut state, a_udp, &frame(1, b"CUR ", b"4 12"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, b_udp);
        assert_eq!(&out[0].1[12..], b"alice 4 12");

        // Spoofed source, bad column and malformed frames are dropped.
        let spoof: SocketAddr = "10.0.0.9:7001".parse().unwrap();
        assert!(handle_udp_datagram(&mut state, spoof, &frame(1, b"CUR ", b"4 12")).is_empty());
        assert!(handle_udp_datagram(&mut state, a_udp, &frame(1, b"CUR ", b"4 81")).is_empty());
        assert!(handle_udp_datagram(&mut state, a_udp, b"junk").is_empty());

        // Snapshot: bob has no cursor yet, so only he receives alice's.
        let snap = cursor_snapshot(&mut state);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0, b_udp);
        assert_eq!(&snap[0].1[12..], b"alice 4 12");
    }

    #[test]
    fn server_ids_skip_ids_used_by_the_client() {
        let mut state = State::default();
        let (tx_a, _rx_a) = mpsc::channel(16);
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
        let (tx_a, _rx_a) = mpsc::channel(16);
        let (tx_b, _rx_b) = mpsc::channel(16);
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
        let (tx_a, _rx_a) = mpsc::channel(16);
        let (tx_b, mut rx_b) = mpsc::channel(16);
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
        let (tx, _rx) = mpsc::channel(16);
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
