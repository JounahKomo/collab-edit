use std::{
    io::{self, Read, Write},
    net::TcpStream,
    process::ExitCode,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;

const TIMEOUT: Duration = Duration::from_secs(2);
const MIN_CONNECTIONS: usize = 5;
const BROADCAST_TEXT: &str = "Hello everyone!";

/// Every message ID a connection uses in its own requests. IDs are
/// connection-specific, and the server must allocate IDs for the messages it
/// originates (broadcast MSG) that are not already in use on that connection.
const CLIENT_IDS: [u32; 8] = [1, 3, 5, 7, 9, 11, 13, 15];

#[derive(Parser, Debug)]
struct Args {
    /// Server host to connect to
    #[arg(short, long, default_value = "127.0.0.1")]
    addr: String,

    /// Server port to connect to
    #[arg(short, long, default_value_t = 5015)]
    port: u16,

    /// Username prefix; connection N registers as "<user><N>"
    #[arg(short, long, default_value = "User")]
    user: String,

    /// Number of concurrent connections (at least 5)
    #[arg(short, long, default_value_t = MIN_CONNECTIONS)]
    connections: usize,
}

/// Outcome of one connection thread.
struct Report {
    index: usize,
    username: String,
    checks: Vec<(String, bool)>,
    /// Total time from thread start to finish (work + waiting).
    elapsed: Duration,
    /// Time spent connecting and exchanging messages with the server.
    work: Duration,
    /// Time spent idle at barriers waiting for the other connections.
    waiting: Duration,
}

impl Report {
    fn check(&mut self, description: &str, passed: bool) {
        self.checks.push((description.to_string(), passed));
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let connections = args.connections.max(MIN_CONNECTIONS);
    if connections != args.connections {
        eprintln!("Using the minimum of {MIN_CONNECTIONS} connections");
    }

    let address = format!("{}:{}", args.addr, args.port);
    println!("Opening {connections} concurrent connections to {address}...");

    // Every thread passes every barrier, even after a failure, so one broken
    // connection can never deadlock the others.
    let barrier = Arc::new(Barrier::new(connections));
    let total_start = Instant::now();

    let handles: Vec<_> = (0..connections)
        .map(|index| {
            let address = address.clone();
            let user_prefix = args.user.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                run_connection(index, connections, &address, &user_prefix, &barrier)
            })
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

fn run_connection(
    index: usize,
    total: usize,
    address: &str,
    user_prefix: &str,
    barrier: &Barrier,
) -> Report {
    let start = Instant::now();
    let username = format!("{user_prefix}{index}");
    let first_username = format!("{user_prefix}0");
    let mut report = Report {
        index,
        username: username.clone(),
        checks: Vec::new(),
        elapsed: Duration::ZERO,
        work: Duration::ZERO,
        waiting: Duration::ZERO,
    };

    let connect_start = Instant::now();
    let mut stream = match connect(address) {
        Ok(s) => Some(s),
        Err(e) => {
            report.check(&format!("connect to server ({e})"), false);
            None
        }
    };
    report.work += connect_start.elapsed();

    // Runs one phase if the connection is still healthy, then waits for the
    // other threads so that the phases line up across all connections.
    let phase = |stream: &mut Option<TcpStream>,
                 report: &mut Report,
                 name: &str,
                 body: &dyn Fn(&mut TcpStream, &mut Report) -> io::Result<()>,
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
            barrier.wait();
            report.waiting += wait_start.elapsed();
        }
    };

    let broadcaster = total - 1;

    // Phase 1: basic message handling, plus use-case specific errors.
    phase(
        &mut stream,
        &mut report,
        "basic messages",
        &|s, r| {
            let tst = create_message(1, "TST ", "Testing");
            s.write_all(&tst)?;
            r.check("TST is echoed back", read_frame(s)? == tst);

            s.write_all(&create_message(3, "BYE ", "Testing"))?;
            let (_, id, ty, _) = decode_message(&read_frame(s)?);
            r.check(
                "unknown message type gets ERR with the same id",
                ty == "ERR " && id == 3,
            );

            if index == 1 {
                s.write_all(&create_message(5, "MSG ", "general too early"))?;
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

    // Phase 2: every connection registers a unique username.
    phase(
        &mut stream,
        &mut report,
        "registration",
        &|s, r| {
            let usr = create_message(7, "USR ", &username);
            s.write_all(&usr)?;
            r.check("USR is accepted and echoed", read_frame(s)? == usr);
            Ok(())
        },
        true, // all users are registered after this point
    );

    // Phase 3: error use cases that need other registered users.
    phase(
        &mut stream,
        &mut report,
        "error cases",
        &|s, r| {
            if index == 2 {
                s.write_all(&create_message(9, "USR ", &first_username))?;
                let (_, id, ty, _) = decode_message(&read_frame(s)?);
                r.check("duplicate username gets ERR", ty == "ERR " && id == 9);
            }
            if index == 3 {
                s.write_all(&create_message(9, "MSG ", "nochannel Hi"))?;
                let (_, id, ty, _) = decode_message(&read_frame(s)?);
                r.check(
                    "MSG to unknown channel gets ERR with the same id",
                    ty == "ERR " && id == 9,
                );

                s.write_all(&create_message(11, "MSG ", "malformed"))?;
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

    // Phase 4: one connection broadcasts, all the others must receive it.
    let expected_payload = format!("{}{} general {}", user_prefix, broadcaster, BROADCAST_TEXT);
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

    // Phase 5: server metrics.
    phase(
        &mut stream,
        &mut report,
        "metrics",
        &|s, r| {
            if index == 0 {
                s.write_all(&create_message(15, "MET ", ""))?;
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
        false,
    );

    report.elapsed = start.elapsed();
    report
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn connect(address: &str) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// Reads exactly one length-prefixed frame from the stream.
fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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
