use std::{
    fs::File,
    io::{self, BufReader, ErrorKind, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    str,
};

use clap::Parser;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Interactive client with optional TLS support"
)]
struct Cli {
    /// Server address in host:port format
    target: String,

    /// Path to custom Root CA certificate (.crt / .pem) for verifying the server
    #[arg(long, env = "TLS_CA_CERT")]
    ca_cert: Option<PathBuf>,

    /// Server Name Indication (SNI) for TLS verification
    #[arg(long, env = "TLS_SERVER_NAME")]
    server_name: Option<String>,

    /// Disable TLS and connect with plain TCP
    #[arg(long, env = "NO_TLS")]
    no_tls: bool,
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

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    let (host, _port) = cli
        .target
        .split_once(':')
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "expected <host>:<port>"))?;

    let tcp_stream = TcpStream::connect(&cli.target)?;
    println!(
        "Connected to: {} (TLS: {})",
        cli.target,
        if cli.no_tls { "disabled" } else { "enabled" }
    );
    println!("Type commands in format: TYPE message (e.g. 'USR Jaakko' or 'MSG general Hello')");

    let stream = if cli.no_tls {
        ConnectionStream::Plain(tcp_stream)
    } else {
        let tls_config = build_client_config(cli.ca_cert.as_deref())
            .map_err(|e| io::Error::other(e.to_string()))?;

        let sni_name = cli.server_name.unwrap_or_else(|| {
            if host.parse::<std::net::IpAddr>().is_ok() {
                "localhost".to_string()
            } else {
                host.to_string()
            }
        });

        let server_name: ServerName<'static> = sni_name
            .try_into()
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "Invalid SNI host name"))?;

        let conn = ClientConnection::new(std::sync::Arc::new(tls_config), server_name)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut tls_stream = StreamOwned::new(conn, tcp_stream);
        tls_stream.flush()?;
        ConnectionStream::Tls(Box::new(tls_stream))
    };

    let mut write_stream = stream;

    // Use try_clone on underlying socket if plain, or use a channel / split approach
    // Since rustls StreamOwned does not implement try_clone, we can use a reader thread with raw reads or non-blocking I/O
    // Alternatively, split the socket or run a single thread with non-blocking / timeout.
    // In Rust, rustls::StreamOwned doesn't clone, but we can do a background thread reading by taking ownership if we communicate via channels, or use non-blocking timeouts in a single thread.
    // Or we can use standard line reading loop:
    run_interactive_loop(&mut write_stream)
}

fn run_interactive_loop(stream: &mut ConnectionStream) -> io::Result<()> {
    let mut msg_id: u32 = 1;

    // Set non-blocking or short timeout on underlying socket to poll reads and writes
    let tcp = match stream {
        ConnectionStream::Plain(s) => s,
        ConnectionStream::Tls(s) => s.get_mut(),
    };
    tcp.set_read_timeout(Some(std::time::Duration::from_millis(100)))?;

    loop {
        // First check for any incoming messages
        let mut header = [0u8; 12];
        match stream.read_exact(&mut header) {
            Ok(()) => {
                let length = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
                let id = u32::from_be_bytes(header[4..8].try_into().unwrap());
                let msg_type = String::from_utf8_lossy(&header[8..12]);
                let payload_len = length.saturating_sub(12);
                let mut payload = vec![0u8; payload_len];
                let _ = stream.read_exact(&mut payload);
                let text = str::from_utf8(&payload).unwrap_or("<invalid utf-8>");
                println!("\r[ID:{} {}] {}\n> ", id, msg_type, text);
                io::stdout().flush().unwrap();
            }
            Err(ref e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                // No incoming message right now
            }
            Err(e) => {
                // Connection might be closed
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    println!("\nDisconnected from server.");
                    break;
                }
            }
        }

        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();

        if parts[0].eq_ignore_ascii_case("quit") {
            break;
        }

        if parts[0].len() != 3 {
            println!("Error: Message type must be exactly 3 characters (e.g. USR, MSG, TST)");
            continue;
        }

        let current_id = msg_id;
        msg_id = msg_id.wrapping_add(2);
        let content = if parts.len() > 1 {
            parts[1..].join(" ")
        } else {
            String::new()
        };
        let mut msg_type_str = parts[0].to_uppercase();
        msg_type_str.push(' ');

        let length = (12 + content.len()) as u32;

        let mut message = Vec::new();
        message.extend_from_slice(&length.to_be_bytes());
        message.extend_from_slice(&current_id.to_be_bytes());
        message.extend_from_slice(msg_type_str.as_bytes());
        message.extend_from_slice(content.as_bytes());

        if let Err(e) = stream.write_all(&message) {
            eprintln!("Failed to send message: {}", e);
            break;
        }
        let _ = stream.flush();
    }

    Ok(())
}
