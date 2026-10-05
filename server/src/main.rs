use std::{
    collections::HashMap,
    io::{self, Read, Write},
};

use mio::{
    net::TcpListener,
    Events, Interest, Poll, Token,
};

use tracing::{error, warn, info};
use tracing_subscriber::EnvFilter;

use server::client::Client;

const SERVER: Token = Token(0);

fn main() -> io::Result<()> {

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let mut poll = Poll::new().inspect_err(|e| error!("Failed to create Poll: {e}"))?;
    let mut events = Events::with_capacity(128);

    let address = "0.0.0.0:5015".parse().unwrap();
    let mut server = TcpListener::bind(address).inspect_err(|e| error!("Failed to bind TCP listener to {address}: {e}"))?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)
		.inspect_err(|e| error!("Registering poll failed: {e}"))?;

    let mut clients: HashMap<Token, Client> = HashMap::new();
    let mut next_token_id = 1;

    info!("Server listening on {}", address);

    loop {
        poll.poll(&mut events, None).inspect_err(|e| error!("Failed to poll events: {e})"))?;

        for event in events.iter() {
            match event.token() {
                SERVER => loop {
                    match server.accept() {
                        Ok((mut socket, address)) => {
                            info!("Accepted connection from: {}", address);
                            let token = Token(next_token_id);
                            next_token_id += 1;

                            poll.registry().register(
                                &mut socket,
                                token,
                                Interest::READABLE,
                            )?;

                            clients.insert(token, Client::new(socket, address));
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            break;
                        }
                        Err(e) => {
                            error!("Error accepting connection: {e}");
                            break;
                        }
                    }
                },
                token => {
                    let mut client = match clients.remove(&token) {
                        Some(c) => c,
                        None => {
							warn!("No client found for token: {:#?}", token);
							continue;
						}
                    };

                    let mut needs_write = false;

                    if event.is_readable() {
                        let mut buf = [0; 4096];
                        loop {
                            match client.socket.read(&mut buf) {
                                Ok(0) => {
                                    client.is_closed = true;
                                    break;
                                }
                                Ok(n) => {
                                    client.read_buf.extend_from_slice(&buf[..n]);
                                }
                                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    break;
                                }
                                Err(err) => {
                                    client.is_closed = true;
									warn!("Failed reading from client: {}", err);
                                    break;
                                }
                            }
                        }
                    }

                    if !client.is_closed {
                        client.process_messages(&mut clients);
                    }

                    if event.is_writable() || !client.write_buf.is_empty() {
                        while !client.write_buf.is_empty() {
                            match client.socket.write(&client.write_buf) {
                                Ok(0) => {
                                    client.is_closed = true;
                                    break;
                                }
                                Ok(n) => {
                                    client.write_buf.drain(..n);
                                }
                                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    needs_write = true;
                                    break;
                                }
                                Err(err) => {
                                    client.is_closed = true;
									warn!("Failed writing to client: {}", err);
                                    break;
                                }
                            }
                        }
                    }

                    if client.is_closed {
                        info!("Connection closed: {}", client.address);
                    } else {
                        let interest = if needs_write || !client.write_buf.is_empty() {
                            Interest::READABLE | Interest::WRITABLE
                        } else {
                            Interest::READABLE
                        };

                        if let Err(e) = poll.registry().reregister(&mut client.socket, token, interest) {
                            warn!("Failed to reregister client: {}", e);
                        } else {
                            clients.insert(token, client);
                        }
                    }
                }
            }
        }
    }
}
