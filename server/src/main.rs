use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::SocketAddr,
};

use mio::{
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};

const SERVER: Token = Token(0);

pub struct Client {
    socket: TcpStream,
    pub address: SocketAddr,
    pub username: Option<String>,
    pub read_buf: Vec<u8>,
    pub write_buf: Vec<u8>,
    pub next_server_msg_id: u32,
    pub is_closed: bool,
}

impl Client {
    fn new(socket: TcpStream, address: SocketAddr) -> Self {
        Client {
            socket,
            address,
            username: None,
            read_buf: Vec::new(),
            write_buf: Vec::new(),
            next_server_msg_id: 2,
            is_closed: false,
        }
    }
}

fn main() -> io::Result<()> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let address = "0.0.0.0:5015".parse().unwrap();
    let mut server = TcpListener::bind(address)?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    let mut clients: HashMap<Token, Client> = HashMap::new();
    let mut next_token_id = 1;

    println!("Server listening on {}", address);

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                SERVER => loop {
                    match server.accept() {
                        Ok((mut socket, address)) => {
                            println!("Accepted connection from: {}", address);
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
                            eprintln!("Error accepting connection: {}", e);
                            break;
                        }
                    }
                },
                token => {
                    let mut client = match clients.remove(&token) {
                        Some(c) => c,
                        None => continue,
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
                                Err(_) => {
                                    client.is_closed = true;
                                    break;
                                }
                            }
                        }
                    }

                    if !client.is_closed {
                        process_messages(&mut client, &mut clients);
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
                                Err(_) => {
                                    client.is_closed = true;
                                    break;
                                }
                            }
                        }
                    }

                    if client.is_closed {
                        println!("Connection closed: {}", client.address);
                    } else {
                        let interest = if needs_write || !client.write_buf.is_empty() {
                            Interest::READABLE | Interest::WRITABLE
                        } else {
                            Interest::READABLE
                        };

                        if let Err(e) = poll.registry().reregister(&mut client.socket, token, interest) {
                            eprintln!("Failed to reregister client: {}", e);
                        } else {
                            clients.insert(token, client);
                        }
                    }
                }
            }
        }
    }
}

fn process_messages(active_client: &mut Client, clients: &mut HashMap<Token, Client>) {
    loop {
        if active_client.read_buf.len() < 4 { break; }
        let len = u32::from_be_bytes(active_client.read_buf[..4].try_into().unwrap()) as usize;

        if len < 12 {
            eprintln!("Protocol error: message length too short");
            active_client.is_closed = true;
            break;
        }

        if active_client.read_buf.len() < len { break; }

        let msg_bytes = active_client.read_buf[..len].to_vec();
        active_client.read_buf.drain(..len);

        let msg_id = u32::from_be_bytes(msg_bytes[4..8].try_into().unwrap());
        let msg_type = &msg_bytes[8..12];
        let payload = &msg_bytes[12..];

        match msg_type {
            b"TST " => {
                active_client.write_buf.extend_from_slice(&msg_bytes);
            }
            b"USR " => {
                if let Ok(username_str) = std::str::from_utf8(payload) {
                    let requested_usr = username_str.to_string();

                    let is_taken = clients.values().any(|c| c.username.as_ref() == Some(&requested_usr));

                    if is_taken {
                        active_client.write_buf.extend_from_slice(&create_error(msg_id, "Username already taken!"));
                    } else {
                        active_client.username = Some(requested_usr);
                        active_client.write_buf.extend_from_slice(&msg_bytes);
                    }
                }
            }
            b"MSG " => {
				let sender_usr = match &active_client.username {
                    Some(usr) => usr.clone(),
                    None => {
                        active_client.write_buf.extend_from_slice(&create_error(msg_id, "Not registered. Send USR first."));
                        continue;
                    }
                };

                let payload_str = match std::str::from_utf8(payload) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                if let Some((channel, message_text)) = payload_str.split_once(' ') {
                    if channel != "general" {
                        active_client.write_buf.extend_from_slice(&create_error(msg_id, "Unknown channel"));
                        continue;
                    }

                    // Format the broadcast: <Sender> <Channel> <Message>
                    let broadcast_payload = format!("{} {} {}", sender_usr, channel, message_text);
                    let payload_bytes = broadcast_payload.as_bytes();
                    let response_len = (12 + payload_bytes.len()) as u32;

                    // 1. Send the broadcast back to the sender using a SERVER allocated ID
                    let sender_msg_id = active_client.next_server_msg_id;
                    active_client.next_server_msg_id = active_client.next_server_msg_id.wrapping_add(2); // Keep it Even

                    let mut sender_msg = Vec::with_capacity(response_len as usize);
                    sender_msg.extend_from_slice(&response_len.to_be_bytes());
                    sender_msg.extend_from_slice(&sender_msg_id.to_be_bytes());
                    sender_msg.extend_from_slice(b"MSG ");
                    sender_msg.extend_from_slice(payload_bytes);
                    active_client.write_buf.extend_from_slice(&sender_msg);

                    // 2. Broadcast to all OTHER users immediately
                    for other_client in clients.values_mut() {
                        if other_client.username.is_some() {
                            let other_msg_id = other_client.next_server_msg_id;
                            other_client.next_server_msg_id = other_client.next_server_msg_id.wrapping_add(2); // Keep it Even

                            let mut other_msg = Vec::with_capacity(response_len as usize);
                            other_msg.extend_from_slice(&response_len.to_be_bytes());
                            other_msg.extend_from_slice(&other_msg_id.to_be_bytes());
                            other_msg.extend_from_slice(b"MSG ");
                            other_msg.extend_from_slice(payload_bytes);

                            other_client.write_buf.extend_from_slice(&other_msg);

                            // Flush instantly
                            while !other_client.write_buf.is_empty() {
                                match other_client.socket.write(&other_client.write_buf) {
                                    Ok(0) => { other_client.is_closed = true; break; }
                                    Ok(n) => { other_client.write_buf.drain(..n); }
                                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                    Err(_) => { other_client.is_closed = true; break; }
                                }
                            }
                        }
                    }
                } else {
                    // Errors still use the client's `msg_id` as requested by the spec
                    active_client.write_buf.extend_from_slice(&create_error(msg_id, "Invalid MSG syntax. Expected: <channel> <message>"));
                }

			}

            _ => {
                active_client.write_buf.extend_from_slice(&create_error(msg_id, "Unknown message type"));
            }
        }
    }
}

pub fn create_error(id: u32, error: &str) -> Vec<u8> {
    let error_bytes = error.as_bytes();
    let len = (12 + error_bytes.len()) as u32;

    let mut error_vec = Vec::with_capacity(len as usize);
    error_vec.extend_from_slice(&len.to_be_bytes());
    error_vec.extend_from_slice(&id.to_be_bytes());
    error_vec.extend_from_slice(b"ERR ");
    error_vec.extend_from_slice(error_bytes);

    error_vec
}
