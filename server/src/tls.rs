//! TLS acceptor construction from PEM certificate and key files.

use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

pub fn load_tls_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("No certificates found in {:?}", path),
        ));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("No private key found in {:?}", path),
        )
    })?;
    Ok(key)
}
