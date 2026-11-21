//! Integration tests: a real server (TCP accept loop and UDP tasks) is started
//! in-process on loopback ports and driven through sockets, like a client would.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use server::{
    connection::run_accept_loop,
    protocol::{MAX_FRAME_LEN, frame},
    state::SharedState,
    udp::spawn_udp_tasks,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);

struct TestServer {
    tcp: SocketAddr,
    udp: SocketAddr,
    state: SharedState,
}

async fn start_server() -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp = listener.local_addr().unwrap();
    let udp_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let udp = udp_socket.local_addr().unwrap();
    let state = SharedState::default();

    spawn_udp_tasks(udp_socket, state.clone());
    tokio::spawn(run_accept_loop(listener, None, state.clone()));
    TestServer { tcp, udp, state }
}

/// A decoded frame: (id, type, payload).
type Msg = (u32, String, String);

struct Conn(TcpStream);

impl Conn {
    async fn open(server: &TestServer) -> Conn {
        Conn(TcpStream::connect(server.tcp).await.unwrap())
    }

    /// Opens a connection and registers `name`.
    async fn registered(server: &TestServer, name: &str) -> Conn {
        let mut conn = Self::open(server).await;
        let reply = conn.request(1, b"USR ", name).await;
        assert_eq!(reply.1, "USR ", "registration of {name} failed: {reply:?}");
        conn
    }

    async fn send(&mut self, id: u32, msg_type: &[u8; 4], payload: &str) {
        self.0
            .write_all(&frame(id, msg_type, payload.as_bytes()))
            .await
            .unwrap();
    }

    async fn send_raw(&mut self, bytes: &[u8]) {
        let _ = self.0.write_all(bytes).await;
    }

    /// Reads the next frame, or `None` if the server closed the connection.
    async fn try_recv(&mut self) -> Option<Msg> {
        let mut len_bytes = [0u8; 4];
        timeout(IO_TIMEOUT, self.0.read_exact(&mut len_bytes))
            .await
            .expect("timed out waiting for a frame")
            .ok()?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut rest = vec![0u8; len - 4];
        self.0.read_exact(&mut rest).await.ok()?;
        Some((
            u32::from_be_bytes(rest[0..4].try_into().unwrap()),
            String::from_utf8_lossy(&rest[4..8]).into_owned(),
            String::from_utf8_lossy(&rest[8..]).into_owned(),
        ))
    }

    async fn recv(&mut self) -> Msg {
        self.try_recv()
            .await
            .expect("connection closed unexpectedly")
    }

    async fn request(&mut self, id: u32, msg_type: &[u8; 4], payload: &str) -> Msg {
        self.send(id, msg_type, payload).await;
        self.recv().await
    }
}

#[tokio::test]
async fn echo_registration_and_error_replies() {
    let server = start_server().await;
    let mut a = Conn::open(&server).await;

    assert_eq!(
        a.request(1, b"TST ", "ping").await,
        (1, "TST ".into(), "ping".into())
    );

    // Requests that need a username are refused before USR.
    for ty in [b"GET ", b"MSG ", b"INS ", b"DEL "] {
        let reply = a.request(3, ty, "x").await;
        assert_eq!(
            (reply.0, reply.1.as_str()),
            (3, "ERR "),
            "{ty:?} before USR"
        );
    }
    let unknown = a.request(5, b"NOPE", "").await;
    assert_eq!((unknown.0, unknown.1.as_str()), (5, "ERR "));

    assert_eq!(a.request(7, b"USR ", "alice").await.1, "USR ");
    let mut b = Conn::open(&server).await;
    assert_eq!(b.request(1, b"USR ", "alice").await.1, "ERR ");
}

#[tokio::test]
async fn document_edits_and_chat_are_broadcast_to_all_clients() {
    let server = start_server().await;
    let mut alice = Conn::registered(&server, "alice").await;
    let mut bob = Conn::registered(&server, "bob").await;

    alice.send(3, b"INS ", "0 0 Hello").await;
    assert_eq!(alice.recv().await.1, "INS ");
    assert_eq!(alice.recv().await.2, "1\nHello");
    assert_eq!(bob.recv().await.2, "1\nHello");

    bob.send(3, b"MSG ", "newchan hi there").await;
    assert_eq!(bob.recv().await.2, "bob newchan hi there");
    assert_eq!(alice.recv().await.2, "bob newchan hi there");

    let doc = bob.request(5, b"GET ", "").await;
    assert_eq!((doc.0, doc.2.as_str()), (5, "1\nHello"));
}

#[tokio::test]
async fn hostile_requests_get_errors_and_the_connection_keeps_working() {
    let server = start_server().await;
    let mut a = Conn::registered(&server, "mallory").await;

    let hostile = [
        (b"INS ", "18446744073709551615 0 x"),
        (b"INS ", "4000000000 0 x"),
        (b"INS ", "100000000 0 x"),
        (b"INS ", "0 18446744073709551615 x"),
        (b"INS ", "-1 0 x"),
        (b"INS ", "abc"),
        (b"DEL ", "18446744073709551615 0 1"),
        (b"DEL ", "0 0 18446744073709551615"),
        (b"DEL ", "0 0 0 0"),
        (b"MSG ", "nochannelseparator"),
        (b"UDP ", ""),
    ];
    for (i, (ty, payload)) in hostile.into_iter().enumerate() {
        let id = 3 + 2 * i as u32;
        let reply = a.request(id, ty, payload).await;
        assert_eq!(
            (reply.0, reply.1.as_str()),
            (id, "ERR "),
            "{ty:?} {payload:?} should be rejected"
        );
    }

    // Invalid UTF-8 payloads are rejected too.
    let mut bad = frame(99, b"MSG ", &[0xff, 0xfe, b' ', 0xfd]);
    a.send_raw(&bad).await;
    assert_eq!(a.recv().await.1, "ERR ");
    bad.clear();

    // The same connection still works afterwards.
    assert_eq!(a.request(99, b"TST ", "still alive").await.2, "still alive");
    assert_eq!(a.request(101, b"GET ", "").await.2, "0\n");
}

#[tokio::test]
async fn oversized_and_garbage_frames_close_only_that_connection() {
    let server = start_server().await;
    let mut healthy = Conn::registered(&server, "healthy").await;

    // Declared length far beyond the limit: closed without buffering it.
    let mut huge = Conn::open(&server).await;
    huge.send_raw(&((MAX_FRAME_LEN as u32) + 1).to_be_bytes())
        .await;
    assert!(
        huge.try_recv().await.is_none(),
        "oversized frame must close the connection"
    );

    // A length shorter than the header is a protocol error.
    let mut short = Conn::open(&server).await;
    short.send_raw(&3u32.to_be_bytes()).await;
    assert!(short.try_recv().await.is_none());

    // Random garbage must not take the server down.
    let mut garbage = Conn::open(&server).await;
    garbage
        .send_raw(&[
            0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        ])
        .await;
    let _ = garbage.try_recv().await;

    assert_eq!(healthy.request(3, b"TST ", "ok").await.2, "ok");
}

#[tokio::test]
async fn abrupt_disconnect_releases_the_username() {
    let server = start_server().await;
    let alice = Conn::registered(&server, "alice").await;
    drop(alice);

    // The server notices the closed socket asynchronously; retry briefly.
    let mut registered_again = false;
    for _ in 0..50 {
        let mut again = Conn::open(&server).await;
        if again.request(1, b"USR ", "alice").await.1 == "USR " {
            registered_again = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered_again, "username was never released");
    assert!(server.state.lock().clients.len() <= 2);
}

#[tokio::test(start_paused = true)]
async fn connections_that_never_register_are_closed() {
    let server = start_server().await;
    let mut idle = Conn::open(&server).await;
    // Time is paused and auto-advances while everything is idle, so this read
    // only returns once the server's registration timeout closes the socket.
    let mut buf = [0u8; 1];
    assert_eq!(idle.0.read(&mut buf).await.unwrap_or(0), 0);
    assert_eq!(server.state.lock().clients.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_simultaneous_clients_are_served_promptly() {
    const CLIENTS: usize = 100;
    let server = Arc::new(start_server().await);
    let all_registered = Arc::new(tokio::sync::Barrier::new(CLIENTS));
    let started = std::time::Instant::now();

    let tasks: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let server = Arc::clone(&server);
            let all_registered = Arc::clone(&all_registered);
            tokio::spawn(async move {
                let mut conn = Conn::registered(&server, &format!("user{i}")).await;
                all_registered.wait().await;
                if i == 0 {
                    conn.send(3, b"MSG ", "general hello everyone").await;
                }
                // Every client, the sender included, receives the broadcast.
                loop {
                    let msg = conn.recv().await;
                    if msg.1 == "MSG " {
                        assert_eq!(msg.2, "user0 general hello everyone");
                        break;
                    }
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.expect("client task failed");
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "100 clients took {:?}",
        started.elapsed()
    );

    let mut probe = Conn::registered(&server, "probe").await;
    let metrics = probe.request(3, b"MET ", "").await;
    assert!(
        metrics.2.contains("\"connections_total\":101"),
        "{}",
        metrics.2
    );
}

#[tokio::test]
async fn udp_cursors_are_relayed_and_snapshotted() {
    let server = start_server().await;
    let mut alice = Conn::registered(&server, "alice").await;
    let mut bob = Conn::registered(&server, "bob").await;
    assert_eq!(alice.request(3, b"UDP ", "tok-alice").await.1, "UDP ");
    assert_eq!(bob.request(3, b"UDP ", "tok-bob").await.1, "UDP ");

    let a_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let b_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for (socket, token) in [(&a_udp, "tok-alice"), (&b_udp, "tok-bob")] {
        socket
            .send_to(&frame(5, b"UDP ", token.as_bytes()), server.udp)
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let (n, _) = timeout(IO_TIMEOUT, socket.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[8..12], b"UDP ", "hello ack, got {:?}", &buf[..n]);
    }

    // A datagram from an unknown socket is ignored.
    let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    stranger
        .send_to(&frame(1, b"CUR ", b"0 0"), server.udp)
        .await
        .unwrap();

    a_udp
        .send_to(&frame(7, b"CUR ", b"4 12"), server.udp)
        .await
        .unwrap();
    let mut buf = [0u8; 256];
    let (n, _) = timeout(IO_TIMEOUT, b_udp.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[8..12], b"CUR ");
    assert_eq!(&buf[12..n], b"alice 4 12");

    // The periodic snapshot repeats the cursor without a new datagram.
    let (n, _) = timeout(IO_TIMEOUT, b_udp.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[12..n], b"alice 4 12");
}

#[tokio::test]
async fn metrics_count_connections_and_errors() {
    let server = start_server().await;
    let mut a = Conn::registered(&server, "metrics").await;
    let _ = a.request(3, b"INS ", "garbage").await;
    let metrics = a.request(5, b"MET ", "").await;
    assert!(
        metrics.2.contains("\"connections_total\":1"),
        "{}",
        metrics.2
    );
    assert!(metrics.2.contains("\"errors\":1"), "{}", metrics.2);
}
