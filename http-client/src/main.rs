use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
};

use clap::Parser;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Deploy server container via HTTPS to pronets course server"
)]
struct Args {
    /// JWT token for authentication (can also be passed via JWT_TOKEN env var)
    #[arg(short, long, env = "JWT_TOKEN")]
    jwt: String,

    /// Course server host
    #[arg(long, default_value = "pronets1.dice.aalto.fi", env = "COURSE_HOST")]
    host: String,

    /// Course server HTTPS port
    #[arg(long, default_value_t = 443, env = "COURSE_PORT")]
    port: u16,

    /// Git repository URL
    #[arg(
        long,
        default_value = "git@version.aalto.fi:komonej1/pns-project-joona-komonen.git",
        env = "GIT_REPO"
    )]
    git_repo: String,

    /// Exposed port(s) for the container
    #[arg(long, default_value = "5011", env = "PORTS")]
    ports: String,

    /// Protocol version
    #[arg(long, default_value = "komon-2", env = "PROTOCOL")]
    protocol: String,

    /// Optional environment variables in KEY=VALUE format
    #[arg(short, long)]
    env: Vec<String>,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    // Build JSON payload without "name", with "secure": true
    let env_json = if args.env.is_empty() {
        String::new()
    } else {
        let items: Vec<String> = args
            .env
            .iter()
            .map(|e| format!("\"{}\"", e.replace('"', "\\\"")))
            .collect();
        format!(",\"env\": [{}]", items.join(","))
    };

    let json_body = format!(
        "{{\"git-repo\": \"{}\",\"ports\": \"{}\",\"protocol\": \"{}\",\"secure\": true{}}}",
        args.git_repo, args.ports, args.protocol, env_json
    );

    println!("Connecting to {}:{} via TLS...", args.host, args.port);

    // Setup TLS configuration with trusted public roots (pronets1 uses GEANT TLS RSA CA)
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let server_name: ServerName<'static> = args
        .host
        .clone()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid SNI server name"))?;

    let tcp_stream = TcpStream::connect(format!("{}:{}", args.host, args.port))?;
    let conn = ClientConnection::new(Arc::new(config), server_name)
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut tls_stream = StreamOwned::new(conn, tcp_stream);

    let request = format!(
        "POST /run-docker-secure HTTP/1.1\r\n\
Host: {}\r\n\
Authorization: Bearer {}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\
\r\n\
{}",
        args.host,
        args.jwt.trim(),
        json_body.len(),
        json_body
    );

    tls_stream.write_all(request.as_bytes())?;
    tls_stream.flush()?;

    println!("Sent secure deployment request to /run-docker-secure:");
    println!("Payload: {}", json_body);
    println!("\nWaiting for response...");

    let mut response = String::new();
    tls_stream.read_to_string(&mut response)?;

    println!("\nResponse from server:\n{}", response);

    Ok(())
}
