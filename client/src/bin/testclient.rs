use std::{
    fs::File,
    io::{self, BufReader, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;

const TIMEOUT: Duration = Duration::from_secs(5);
const MIN_CONNECTIONS: usize = 5;
const BROADCAST_TEXT: &str = "Hello everyone!";

/// Every message ID a connection uses in its own requests.
const CLIENT_IDS: [u32; 12] = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23];

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Server host to connect to
    #[arg(short, long, default_value = "127.0.0.1", env = "SERVER_ADDR")]
    addr: String,

    /// Server port to connect to
    #[arg(short, long, default_value_t = 5011, env = "SERVER_PORT")]
    port: u16,

    /// Username prefix; connection N registers as "<user><N>"
    #[arg(short, long, default_value = "User")]
    user: String,

    /// Number of concurrent connections (at least 5)
    #[arg(short, long, default_value_t = MIN_CONNECTIONS)]
    connections: usize,

    /// Protocol version to test (defaults to 'base-3'; use 'komon-2' or 'komon-3' to test document and multi-channel features)
    #[arg(long, default_value = "base-3", env = "PROTOCOL")]
    protocol: String,

    /// Path to custom Root CA certificate (.crt / .pem) for verifying the server
    #[arg(long, env = "TLS_CA_CERT")]
    ca_cert: Option<PathBuf>,

    /// Server Name Indication (SNI) for TLS verification (defaults to addr if it's a domain, or 'localhost')
    #[arg(long, env = "TLS_SERVER_NAME")]
    server_name: Option<String>,

    /// Connect with plain TCP instead of TLS
    #[arg(long, env = "NO_TLS")]
    no_tls: bool,
}

struct Report {
    index: usize,
    username: String,
    checks: Vec<(String, bool)>,
    elapsed: Duration,
    work: Duration,
    waiting: Duration,
}

impl Report {
    fn check(&mut self, description: &str, passed: bool) {
        self.checks.push((description.to_string(), passed));
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

struct ConnectionConfig {
    address: String,
    user_prefix: String,
    barrier: Arc<Barrier>,
    tls_config: Option<Arc<ClientConfig>>,
    sni_name: String,
    is_komon2: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let connections = args.connections.max(MIN_CONNECTIONS);
    if connections != args.connections {
        eprintln!("Using the minimum of {MIN_CONNECTIONS} connections");
    }

    let address = format!("{}:{}", args.addr, args.port);
    let is_komon2 = matches!(args.protocol.as_str(), "komon-2" | "komon-3");

    println!(
        "Opening {connections} concurrent connections to {address} (Protocol: {}, TLS: {})...",
        args.protocol,
        if args.no_tls { "disabled" } else { "enabled" }
    );

    let tls_config = if args.no_tls {
        None
    } else {
        match build_client_config(args.ca_cert.as_deref()) {
            Ok(cfg) => Some(Arc::new(cfg)),
            Err(e) => {
                eprintln!("Failed to configure TLS client: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    let sni_name = args.server_name.clone().unwrap_or_else(|| {
        if args.addr.parse::<std::net::IpAddr>().is_ok() {
            "localhost".to_string()
        } else {
            args.addr.clone()
        }
    });

    let config = Arc::new(ConnectionConfig {
        address,
        user_prefix: args.user,
        barrier: Arc::new(Barrier::new(connections)),
        tls_config,
        sni_name,
        is_komon2,
    });
    let total_start = Instant::now();

    let handles: Vec<_> = (0..connections)
        .map(|index| {
            let config = Arc::clone(&config);
            thread::spawn(move || run_connection(index, connections, &config))
        })
        .collect();

    let mut reports: Vec<Report> = handles
        .into_iter()
        .map(|h| h.join().expect("connection thread panicked"))
        .collect();
    let total_elapsed = total_start.elapsed();
    reports.sort_by_key(|r| r.index);

    let mut all_passed = true;
    for report in &reports {
        println!(
            "\n[connection {} / {}] total {:.3} ms = work {:.3} ms + barrier wait {:.3} ms",
            report.index,
            report.username,
            ms(report.elapsed),
            ms(report.work),
            ms(report.waiting)
        );
        for (description, passed) in &report.checks {
            all_passed &= *passed;
            println!(
                "  {} {}",
                if *passed { "PASS" } else { "FAIL" },
                description
            );
        }
    }

    println!(
        "\nAll threads finished in {:.3} ms wall-clock: {}",
        ms(total_elapsed),
        if all_passed {
            "ALL CHECKS PASSED"
        } else {
            "SOME CHECKS FAILED"
        }
    );

    if all_passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

enum ConnectionStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for ConnectionStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

fn build_client_config(ca_cert_path: Option<&Path>) -> io::Result<ClientConfig> {
    let mut root_store = RootCertStore::empty();

    if let Some(path) = ca_cert_path {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

fn connect(
    address: &str,
    tls_config: Option<Arc<ClientConfig>>,
    sni_name: &str,
) -> io::Result<ConnectionStream> {
    let tcp_stream = TcpStream::connect(address)?;
    tcp_stream.set_read_timeout(Some(TIMEOUT))?;
    tcp_stream.set_write_timeout(Some(TIMEOUT))?;
    tcp_stream.set_nodelay(true)?;

    if let Some(config) = tls_config {
        let server_name: ServerName<'static> = sni_name
            .to_string()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid SNI host name"))?;

        let conn = ClientConnection::new(config, server_name)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut tls_stream = StreamOwned::new(conn, tcp_stream);
        tls_stream.flush()?;
        Ok(ConnectionStream::Tls(Box::new(tls_stream)))
    } else {
        Ok(ConnectionStream::Plain(tcp_stream))
    }
}

/// Everything a test phase needs to know about its connection.
struct PhaseContext<'a> {
    index: usize,
    /// Index of the connection that sends the broadcast messages.
    broadcaster: usize,
    config: &'a ConnectionConfig,
}

impl PhaseContext<'_> {
    fn username_of(&self, index: usize) -> String {
        format!("{}{}", self.config.user_prefix, index)
    }
}

type PhaseBody = fn(&PhaseContext, &mut ConnectionStream, &mut Report) -> io::Result<()>;

/// A named test phase; `sync` makes all connections wait at the barrier after it.
struct Phase {
    name: &'static str,
    body: PhaseBody,
    sync: bool,
}

const BASE_PHASES: &[Phase] = &[
    Phase {
        name: "basic messages",
        body: phase_basic_messages,
        sync: false,
    },
    Phase {
        name: "registration",
        body: phase_registration,
        sync: true,
    },
    Phase {
        name: "error cases",
        body: phase_error_cases,
        sync: true,
    },
    Phase {
        name: "broadcast",
        body: phase_broadcast,
        sync: true,
    },
    Phase {
        name: "metrics",
        body: phase_metrics,
        sync: true,
    },
];

/// Document and multi-channel phases (komon-2 and later).
const DOCUMENT_PHASES: &[Phase] = &[
    Phase {
        name: "document GET",
        body: phase_document_get,
        sync: true,
    },
    Phase {
        name: "document INS & broadcast",
        body: phase_document_insert,
        sync: true,
    },
    Phase {
        name: "document DEL & broadcast",
        body: phase_document_delete,
        sync: true,
    },
    Phase {
        name: "document bounds error",
        body: phase_document_bounds_error,
        sync: true,
    },
    Phase {
        name: "multi-channel broadcast",
        body: phase_multi_channel,
        sync: true,
    },
];

fn run_connection(index: usize, total: usize, config: &ConnectionConfig) -> Report {
    let start = Instant::now();
    let mut report = Report {
        index,
        username: format!("{}{}", config.user_prefix, index),
        checks: Vec::new(),
        elapsed: Duration::ZERO,
        work: Duration::ZERO,
        waiting: Duration::ZERO,
    };
    let ctx = PhaseContext {
        index,
        broadcaster: total - 1,
        config,
    };

    let connect_start = Instant::now();
    let mut stream = match connect(&config.address, config.tls_config.clone(), &config.sni_name) {
        Ok(s) => Some(s),
        Err(e) => {
            report.check(&format!("connect to server ({e})"), false);
            None
        }
    };
    report.work += connect_start.elapsed();

    let document_phases = if config.is_komon2 {
        DOCUMENT_PHASES
    } else {
        &[]
    };
    for phase in BASE_PHASES.iter().chain(document_phases) {
        run_phase(&ctx, phase, &mut stream, &mut report);
    }

    report.elapsed = start.elapsed();
    report
}

/// Runs one phase, records its time, and drops the stream after an I/O error.
fn run_phase(
    ctx: &PhaseContext,
    phase: &Phase,
    stream: &mut Option<ConnectionStream>,
    report: &mut Report,
) {
    let work_start = Instant::now();
    if let Some(s) = stream.as_mut()
        && let Err(e) = (phase.body)(ctx, s, report)
    {
        report.check(&format!("{}: I/O error ({e})", phase.name), false);
        *stream = None;
    }
    report.work += work_start.elapsed();

    if phase.sync {
        let wait_start = Instant::now();
        ctx.config.barrier.wait();
        report.waiting += wait_start.elapsed();
    }
}

/// Sends one message and returns the next frame decoded as (len, id, type, payload).
fn request(
    s: &mut ConnectionStream,
    id: u32,
    msg_type: &str,
    payload: &str,
) -> io::Result<(u32, u32, String, String)> {
    send(s, id, msg_type, payload)?;
    Ok(decode_message(&read_frame(s)?))
}

fn send(s: &mut ConnectionStream, id: u32, msg_type: &str, payload: &str) -> io::Result<()> {
    s.write_all(&create_message(id, msg_type, payload))?;
    s.flush()
}

fn next_message(s: &mut ConnectionStream) -> io::Result<(u32, u32, String, String)> {
    Ok(decode_message(&read_frame(s)?))
}

fn phase_basic_messages(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    let tst = create_message(1, "TST ", "Testing");
    s.write_all(&tst)?;
    s.flush()?;
    r.check("TST is echoed back", read_frame(s)? == tst);

    let (_, id, ty, _) = request(s, 3, "BYE ", "Testing")?;
    r.check(
        "unknown message type gets ERR with the same id",
        ty == "ERR " && id == 3,
    );

    if ctx.index == 1 {
        let (_, id, ty, _) = request(s, 5, "MSG ", "general too early")?;
        r.check(
            "MSG before USR gets ERR with the same id",
            ty == "ERR " && id == 5,
        );
    }
    Ok(())
}

fn phase_registration(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    let usr = create_message(7, "USR ", &ctx.username_of(ctx.index));
    s.write_all(&usr)?;
    s.flush()?;
    r.check("USR is accepted and echoed", read_frame(s)? == usr);
    Ok(())
}

fn phase_error_cases(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    if ctx.index == 2 {
        let (_, id, ty, _) = request(s, 9, "USR ", &ctx.username_of(0))?;
        r.check("duplicate username gets ERR", ty == "ERR " && id == 9);
    }
    if ctx.index == 3 {
        let (_, id, ty, _) = request(s, 11, "MSG ", "malformed")?;
        r.check(
            "MSG without <channel> <text> gets ERR with the same id",
            ty == "ERR " && id == 11,
        );
    }
    Ok(())
}

fn phase_broadcast(ctx: &PhaseContext, s: &mut ConnectionStream, r: &mut Report) -> io::Result<()> {
    let expected_payload = format!(
        "{} general {}",
        ctx.username_of(ctx.broadcaster),
        BROADCAST_TEXT
    );
    if ctx.index == ctx.broadcaster {
        let (len, id, ty, payload) = request(s, 13, "MSG ", &format!("general {BROADCAST_TEXT}"))?;
        r.check(
            "MSG is echoed to the sender with a newly allocated server id",
            ty == "MSG "
                && !CLIENT_IDS.contains(&id)
                && payload == expected_payload
                && len as usize == 12 + payload.len(),
        );
    } else {
        let (_, id, ty, payload) = next_message(s)?;
        r.check(
            "broadcast MSG from another user received",
            ty == "MSG " && !CLIENT_IDS.contains(&id) && payload == expected_payload,
        );
    }
    Ok(())
}

fn phase_metrics(ctx: &PhaseContext, s: &mut ConnectionStream, r: &mut Report) -> io::Result<()> {
    if ctx.index == 0 {
        let (_, id, ty, payload) = request(s, 15, "MET ", "")?;
        r.check(
            "MET returns the metrics JSON",
            ty == "MET "
                && id == 15
                && payload.contains("\"connections_total\"")
                && payload.contains("\"messages_received\""),
        );
    }
    Ok(())
}

fn phase_document_get(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    if ctx.index == 0 {
        let (_, id, ty, payload) = request(s, 17, "GET ", "")?;
        r.check(
            "GET returns document status (<revision>\\n<content>)",
            ty == "GET " && id == 17 && payload.contains('\n'),
        );
    }
    Ok(())
}

/// Shared by the INS and DEL phases: the editing connection gets the echo of its
/// request followed by a GET broadcast; every other connection gets the broadcast.
struct EditExpectation {
    request_id: u32,
    request_type: &'static str,
    request_payload: &'static str,
    expected_text: &'static str,
    echo_check: &'static str,
    sender_broadcast_check: &'static str,
    peer_broadcast_check: &'static str,
}

fn run_edit_phase(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
    e: &EditExpectation,
) -> io::Result<()> {
    let broadcast_ok = |(_, id, ty, payload): (u32, u32, String, String)| {
        ty == "GET " && !CLIENT_IDS.contains(&id) && payload.contains(e.expected_text)
    };

    if ctx.index == 0 {
        let (_, id, ty, _) = request(s, e.request_id, e.request_type, e.request_payload)?;
        r.check(e.echo_check, ty == e.request_type && id == e.request_id);
        r.check(e.sender_broadcast_check, broadcast_ok(next_message(s)?));
    } else {
        r.check(e.peer_broadcast_check, broadcast_ok(next_message(s)?));
    }
    Ok(())
}

fn phase_document_insert(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    run_edit_phase(
        ctx,
        s,
        r,
        &EditExpectation {
            request_id: 19,
            request_type: "INS ",
            request_payload: "0 0 Collaborative",
            expected_text: "Collaborative",
            echo_check: "INS echoed with request ID",
            sender_broadcast_check: "INS triggers GET broadcast to sender with server ID",
            peer_broadcast_check: "peers receive GET broadcast after INS",
        },
    )
}

fn phase_document_delete(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    run_edit_phase(
        ctx,
        s,
        r,
        &EditExpectation {
            request_id: 21,
            request_type: "DEL ",
            request_payload: "0 0 5",
            expected_text: "borative",
            echo_check: "DEL echoed with request ID",
            sender_broadcast_check: "DEL triggers GET broadcast to sender with server ID",
            peer_broadcast_check: "peers receive GET broadcast after DEL",
        },
    )
}

fn phase_document_bounds_error(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    if ctx.index == 1 {
        let (_, id, ty, _) = request(s, 23, "INS ", "0 85 out_of_bounds")?;
        r.check(
            "INS beyond max columns returns ERR with same ID",
            ty == "ERR " && id == 23,
        );
    }
    Ok(())
}

/// Connection 0 posts to a not-yet-existing channel; the server creates it and
/// broadcasts to everyone.
fn phase_multi_channel(
    ctx: &PhaseContext,
    s: &mut ConnectionStream,
    r: &mut Report,
) -> io::Result<()> {
    let channel_name = "project-updates";
    let msg_text = "Welcome to the new channel!";
    let expected = format!("{} {channel_name} {msg_text}", ctx.username_of(0));

    let (len, id, ty, payload) = if ctx.index == 0 {
        request(s, 25, "MSG ", &format!("{channel_name} {msg_text}"))?
    } else {
        next_message(s)?
    };
    let description = if ctx.index == 0 {
        "MSG to new channel dynamically creates channel and echoes to sender"
    } else {
        "peers receive broadcast from dynamically created channel"
    };
    r.check(
        description,
        ty == "MSG "
            && !CLIENT_IDS.contains(&id)
            && payload == expected
            && len as usize == 12 + payload.len(),
    );
    Ok(())
}

fn read_frame<R: Read>(stream: &mut R) -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame shorter than header",
        ));
    }
    let mut frame = vec![0u8; len];
    frame[..4].copy_from_slice(&len_bytes);
    stream.read_exact(&mut frame[4..])?;
    Ok(frame)
}

pub fn create_message(id: u32, msg_type: &str, payload: &str) -> Vec<u8> {
    let mut message = Vec::new();
    let len = (12 + payload.len()) as u32;

    message.extend_from_slice(&len.to_be_bytes());
    message.extend_from_slice(&id.to_be_bytes());
    message.extend_from_slice(msg_type.as_bytes());
    message.extend_from_slice(payload.as_bytes());

    message
}

pub fn decode_message(message: &[u8]) -> (u32, u32, String, String) {
    let len = u32::from_be_bytes(message[..4].try_into().unwrap());
    let id = u32::from_be_bytes(message[4..8].try_into().unwrap());
    let msg_type = String::from_utf8_lossy(&message[8..12]).into_owned();
    let payload = String::from_utf8_lossy(&message[12..(len as usize)]).into_owned();

    (len, id, msg_type, payload)
}
