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

    /// Protocol version to test (defaults to 'base-3'; use 'komon-2' to test document and multi-channel features)
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
    let is_komon2 = args.protocol == "komon-2";

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

fn run_connection(index: usize, total: usize, config: &ConnectionConfig) -> Report {
    let start = Instant::now();
    let username = format!("{}{}", config.user_prefix, index);
    let first_username = format!("{}0", config.user_prefix);
    let mut report = Report {
        index,
        username: username.clone(),
        checks: Vec::new(),
        elapsed: Duration::ZERO,
        work: Duration::ZERO,
        waiting: Duration::ZERO,
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

    let phase = |stream: &mut Option<ConnectionStream>,
                 report: &mut Report,
                 name: &str,
                 body: &dyn Fn(&mut ConnectionStream, &mut Report) -> io::Result<()>,
                 sync: bool| {
        let work_start = Instant::now();
        if let Some(s) = stream.as_mut()
            && let Err(e) = body(s, report)
        {
            report.check(&format!("{name}: I/O error ({e})"), false);
            *stream = None;
        }
        report.work += work_start.elapsed();

        if sync {
            let wait_start = Instant::now();
            config.barrier.wait();
            report.waiting += wait_start.elapsed();
        }
    };

    let broadcaster = total - 1;

    // Phase 1: basic message handling
    phase(
        &mut stream,
        &mut report,
        "basic messages",
        &|s, r| {
            let tst = create_message(1, "TST ", "Testing");
            s.write_all(&tst)?;
            s.flush()?;
            r.check("TST is echoed back", read_frame(s)? == tst);

            s.write_all(&create_message(3, "BYE ", "Testing"))?;
            s.flush()?;
            let (_, id, ty, _) = decode_message(&read_frame(s)?);
            r.check(
                "unknown message type gets ERR with the same id",
                ty == "ERR " && id == 3,
            );

            if index == 1 {
                s.write_all(&create_message(5, "MSG ", "general too early"))?;
                s.flush()?;
                let (_, id, ty, _) = decode_message(&read_frame(s)?);
                r.check(
                    "MSG before USR gets ERR with the same id",
                    ty == "ERR " && id == 5,
                );
            }
            Ok(())
        },
        false,
    );

    // Phase 2: registration
    phase(
        &mut stream,
        &mut report,
        "registration",
        &|s, r| {
            let usr = create_message(7, "USR ", &username);
            s.write_all(&usr)?;
            s.flush()?;
            r.check("USR is accepted and echoed", read_frame(s)? == usr);
            Ok(())
        },
        true,
    );

    // Phase 3: error use cases
    phase(
        &mut stream,
        &mut report,
        "error cases",
        &|s, r| {
            if index == 2 {
                s.write_all(&create_message(9, "USR ", &first_username))?;
                s.flush()?;
                let (_, id, ty, _) = decode_message(&read_frame(s)?);
                r.check("duplicate username gets ERR", ty == "ERR " && id == 9);
            }
            if index == 3 {
                if config.is_komon2 {
                    // In komon-2, unrecognized channels are dynamically created and echoed
                    s.write_all(&create_message(9, "MSG ", "nochannel Hi"))?;
                    s.flush()?;
                    let (_, id, ty, payload) = decode_message(&read_frame(s)?);
                    r.check(
                        "MSG to new channel dynamically creates channel and broadcasts",
                        ty == "MSG " && !CLIENT_IDS.contains(&id) && payload.contains("nochannel Hi"),
                    );
                } else {
                    s.write_all(&create_message(9, "MSG ", "nochannel Hi"))?;
                    s.flush()?;
                    let (_, id, ty, _) = decode_message(&read_frame(s)?);
                    r.check(
                        "MSG to unknown channel gets ERR with the same id",
                        ty == "ERR " && id == 9,
                    );
                }

                s.write_all(&create_message(11, "MSG ", "malformed"))?;
                s.flush()?;
                let (_, id, ty, _) = decode_message(&read_frame(s)?);
                r.check(
                    "MSG without <channel> <text> gets ERR with the same id",
                    ty == "ERR " && id == 11,
                );
            }
            Ok(())
        },
        true,
    );

    // Phase 4: broadcast
    let expected_payload = format!(
        "{}{} general {}",
        config.user_prefix, broadcaster, BROADCAST_TEXT
    );
    phase(
        &mut stream,
        &mut report,
        "broadcast",
        &|s, r| {
            if index == broadcaster {
                s.write_all(&create_message(
                    13,
                    "MSG ",
                    &format!("general {BROADCAST_TEXT}"),
                ))?;
                s.flush()?;
                let (len, id, ty, payload) = decode_message(&read_frame(s)?);
                r.check(
                    "MSG is echoed to the sender with a newly allocated server id",
                    ty == "MSG "
                        && !CLIENT_IDS.contains(&id)
                        && payload == expected_payload
                        && len as usize == 12 + payload.len(),
                );
            } else {
                let (_, id, ty, payload) = decode_message(&read_frame(s)?);
                r.check(
                    "broadcast MSG from another user received",
                    ty == "MSG " && !CLIENT_IDS.contains(&id) && payload == expected_payload,
                );
            }
            Ok(())
        },
        true,
    );

    // Phase 5: metrics
    phase(
        &mut stream,
        &mut report,
        "metrics",
        &|s, r| {
            if index == 0 {
                s.write_all(&create_message(15, "MET ", ""))?;
                s.flush()?;
                let (_, id, ty, payload) = decode_message(&read_frame(s)?);
                r.check(
                    "MET returns the metrics JSON",
                    ty == "MET "
                        && id == 15
                        && payload.contains("\"connections_total\"")
                        && payload.contains("\"messages_received\""),
                );
            }
            Ok(())
        },
        true,
    );

    // Document & Multi-Channel Protocol Testing (komon-2 only)
    if config.is_komon2 {
        // Phase 6: GET document
        phase(
            &mut stream,
            &mut report,
            "document GET",
            &|s, r| {
                if index == 0 {
                    s.write_all(&create_message(17, "GET ", ""))?;
                    s.flush()?;
                    let (_, id, ty, payload) = decode_message(&read_frame(s)?);
                    let valid_get = ty == "GET " && id == 17 && payload.contains('\n');
                    r.check(
                        "GET returns document status (<revision>\\n<content>)",
                        valid_get,
                    );
                }
                Ok(())
            },
            true,
        );

        // Phase 7: INS document and broadcast
        phase(
            &mut stream,
            &mut report,
            "document INS & broadcast",
            &|s, r| {
                if index == 0 {
                    s.write_all(&create_message(19, "INS ", "0 0 Collaborative"))?;
                    s.flush()?;
                    // 1) Echo of INS with request ID 19
                    let ins_frame = read_frame(s)?;
                    let (_, ins_id, ins_ty, _) = decode_message(&ins_frame);
                    r.check(
                        "INS echoed with request ID",
                        ins_ty == "INS " && ins_id == 19,
                    );

                    // 2) Broadcast GET with server-allocated ID
                    let get_frame = read_frame(s)?;
                    let (_, get_id, get_ty, payload) = decode_message(&get_frame);
                    r.check(
                        "INS triggers GET broadcast to sender with server ID",
                        get_ty == "GET "
                            && !CLIENT_IDS.contains(&get_id)
                            && payload.contains("Collaborative"),
                    );
                } else {
                    // Other clients receive GET broadcast with their server ID
                    let get_frame = read_frame(s)?;
                    let (_, get_id, get_ty, payload) = decode_message(&get_frame);
                    r.check(
                        "peers receive GET broadcast after INS",
                        get_ty == "GET "
                            && !CLIENT_IDS.contains(&get_id)
                            && payload.contains("Collaborative"),
                    );
                }
                Ok(())
            },
            true,
        );

        // Phase 8: DEL document and broadcast
        phase(
            &mut stream,
            &mut report,
            "document DEL & broadcast",
            &|s, r| {
                if index == 0 {
                    s.write_all(&create_message(21, "DEL ", "0 0 5"))?;
                    s.flush()?;
                    // 1) Echo of DEL with request ID 21
                    let del_frame = read_frame(s)?;
                    let (_, del_id, del_ty, _) = decode_message(&del_frame);
                    r.check(
                        "DEL echoed with request ID",
                        del_ty == "DEL " && del_id == 21,
                    );

                    // 2) Broadcast GET with server-allocated ID
                    let get_frame = read_frame(s)?;
                    let (_, get_id, get_ty, payload) = decode_message(&get_frame);
                    r.check(
                        "DEL triggers GET broadcast to sender with server ID",
                        get_ty == "GET "
                            && !CLIENT_IDS.contains(&get_id)
                            && payload.contains("borative"),
                    );
                } else {
                    // Other clients receive GET broadcast with their server ID
                    let get_frame = read_frame(s)?;
                    let (_, get_id, get_ty, payload) = decode_message(&get_frame);
                    r.check(
                        "peers receive GET broadcast after DEL",
                        get_ty == "GET "
                            && !CLIENT_IDS.contains(&get_id)
                            && payload.contains("borative"),
                    );
                }
                Ok(())
            },
            true,
        );

        // Phase 9: Document error cases (exceeding MAX_COLUMNS)
        phase(
            &mut stream,
            &mut report,
            "document bounds error",
            &|s, r| {
                if index == 1 {
                    s.write_all(&create_message(23, "INS ", "0 85 out_of_bounds"))?;
                    s.flush()?;
                    let (_, id, ty, _) = decode_message(&read_frame(s)?);
                    r.check(
                        "INS beyond max columns returns ERR with same ID",
                        ty == "ERR " && id == 23,
                    );
                }
                Ok(())
            },
            true,
        );

        // Phase 10: Multi-channel dynamic creation and broadcasting
        phase(
            &mut stream,
            &mut report,
            "multi-channel broadcast",
            &|s, r| {
                let channel_name = "project-updates";
                let msg_text = "Welcome to the new channel!";

                if index == 0 {
                    // Connection 0 sends a message to an unrecognized channel name
                    s.write_all(&create_message(25, "MSG ", &format!("{channel_name} {msg_text}")))?;
                    s.flush()?;
                    let (len, id, ty, payload) = decode_message(&read_frame(s)?);
                    let expected_echo = format!("{}0 {channel_name} {msg_text}", config.user_prefix);
                    r.check(
                        "MSG to new channel dynamically creates channel and echoes to sender",
                        ty == "MSG "
                            && !CLIENT_IDS.contains(&id)
                            && payload == expected_echo
                            && len as usize == 12 + payload.len(),
                    );
                } else {
                    // Other connections receive the broadcast on the dynamically created channel
                    let (len, id, ty, payload) = decode_message(&read_frame(s)?);
                    let expected_broadcast = format!("{}0 {channel_name} {msg_text}", config.user_prefix);
                    r.check(
                        "peers receive broadcast from dynamically created channel",
                        ty == "MSG "
                            && !CLIENT_IDS.contains(&id)
                            && payload == expected_broadcast
                            && len as usize == 12 + payload.len(),
                    );
                }
                Ok(())
            },
            true,
        );
    }

    report.elapsed = start.elapsed();
    report
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
