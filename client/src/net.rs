//! Network connection and streaming helpers for client TLS and plain TCP.

use std::{
    fs::File,
    io::{self, BufReader, Read, Write},
    net::TcpStream,
    path::Path,
};

use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pki_types::ServerName;

/// Unified stream wrapping either plain TCP or rustls TLS.
pub enum ConnectionStream {
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

impl ConnectionStream {
    /// Obtains a mutable reference to the underlying TcpStream for setting timeouts.
    pub fn tcp_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => s.get_mut(),
        }
    }
}

/// Builds a rustls ClientConfig using a custom root CA certificate or system/webpki roots.
pub fn build_client_config(ca_cert_path: Option<&Path>) -> io::Result<ClientConfig> {
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

/// Formats a wire protocol frame: [4-byte length][4-byte msg_id][4-byte msg_type][payload].
pub fn make_frame(msg_id: u32, msg_type: &[u8; 4], payload: &str) -> Vec<u8> {
    let payload_bytes = payload.as_bytes();
    let length = (12 + payload_bytes.len()) as u32;
    let mut frame = Vec::with_capacity(length as usize);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&msg_id.to_be_bytes());
    frame.extend_from_slice(msg_type);
    frame.extend_from_slice(payload_bytes);
    frame
}

/// Connects to the target host:port, optionally setting up TLS with the provided configuration.
pub fn connect_stream(
    target: &str,
    no_tls: bool,
    ca_cert_path: Option<&Path>,
    server_name_override: Option<&str>,
) -> io::Result<ConnectionStream> {
    let clean_target = target
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');

    let (host, _port) = clean_target
        .split_once(':')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected <host>:<port>"))?;

    let tcp_stream = TcpStream::connect(clean_target)?;

    if no_tls {
        Ok(ConnectionStream::Plain(tcp_stream))
    } else {
        let tls_config = build_client_config(ca_cert_path)?;

        let sni_name = match server_name_override {
            Some(name) => name.to_string(),
            None => {
                if host.parse::<std::net::IpAddr>().is_ok() {
                    "localhost".to_string()
                } else {
                    host.to_string()
                }
            }
        };

        let server_name: ServerName<'static> = sni_name
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid SNI host name"))?;

        let conn = ClientConnection::new(std::sync::Arc::new(tls_config), server_name)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut tls_stream = StreamOwned::new(conn, tcp_stream);
        tls_stream.flush()?;
        Ok(ConnectionStream::Tls(Box::new(tls_stream)))
    }
}
