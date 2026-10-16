use mio::{Token, net::TcpStream};
use std::{
    collections::HashMap,
    io::{self, Write},
    net::SocketAddr,
};

use serde::Serialize;
use tracing::{error, warn};

#[derive(Debug, Default, Serialize)]
pub struct ServerMetrics {
    pub connections_total: u64,
    pub messages_received: u64,
    pub errors: u64,
}

pub struct Client {
    pub socket: TcpStream,
    pub address: SocketAddr,
    pub username: Option<String>,
    pub read_buf: Vec<u8>,
    pub write_buf: Vec<u8>,
    pub next_server_msg_id: u32,
    pub is_closed: bool,
}

impl Client {
    pub fn new(socket: TcpStream, address: SocketAddr) -> Self {
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

    pub fn process_messages(
        &mut self,
        clients: &mut HashMap<Token, Client>,
        metrics: &mut ServerMetrics,
    ) {
        loop {
            if self.read_buf.len() < 4 {
                break;
            }
            let len = u32::from_be_bytes(self.read_buf[..4].try_into().unwrap()) as usize;

            if len < 12 {
                warn!("Protocol error: message length too short");
                self.is_closed = true;
                break;
            }

            if self.read_buf.len() < len {
                break;
            }

            let msg_bytes = self.read_buf[..len].to_vec();
            self.read_buf.drain(..len);

            let msg_id = u32::from_be_bytes(msg_bytes[4..8].try_into().unwrap());
            let msg_type = &msg_bytes[8..12];
            let payload = &msg_bytes[12..];

            metrics.messages_received += 1;

            match msg_type {
                b"MET" => match serde_json::to_string(metrics) {
                    Ok(metrics_json) => {
                        let json_bytes = metrics_json.as_bytes();
                        let response_len = (12 + json_bytes.len()) as u32;

                        let mut met_msg = Vec::with_capacity(response_len as usize);
                        met_msg.extend_from_slice(&response_len.to_be_bytes());
                        met_msg.extend_from_slice(&msg_id.to_be_bytes());
                        met_msg.extend_from_slice(b"MET ");
                        met_msg.extend_from_slice(json_bytes);

                        self.write_buf.extend_from_slice(&met_msg);
                    }
                    Err(e) => {
                        error!("Failed to serialize metrics: {}", e);
                        metrics.errors += 1;
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Internal error serializing metrics",
                        ));
                    }
                },
                b"TST " => {
                    self.write_buf.extend_from_slice(&msg_bytes);
                }
                b"USR " => {
                    if let Ok(username_str) = std::str::from_utf8(payload) {
                        let requested_usr = username_str.to_string();

                        let is_taken = clients
                            .values()
                            .any(|c| c.username.as_ref() == Some(&requested_usr));

                        if is_taken {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Username already taken!",
                            ));
                            warn!("Username: {} already taken", username_str);
                        } else {
                            self.username = Some(requested_usr);
                            self.write_buf.extend_from_slice(&msg_bytes);
                        }
                    } else {
                        self.write_buf
                            .extend_from_slice(&create_error(msg_id, "Invalid UTF-8 in username"));
                        warn!("Invalid UTF-8 in USR payload");
                        metrics.errors += 1;
                    }
                }
                b"MSG " => {
                    let sender_usr = match &self.username {
                        Some(usr) => usr.clone(),
                        None => {
                            self.write_buf.extend_from_slice(&create_error(
                                msg_id,
                                "Not registered. Send USR first.",
                            ));
                            warn!("Tried to send MSG without username");
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    let payload_str = match std::str::from_utf8(payload) {
                        Ok(s) => s,
                        Err(err) => {
                            warn!("Failed parsing of payload: {}", err);
                            metrics.errors += 1;
                            continue;
                        }
                    };

                    if let Some((channel, message_text)) = payload_str.split_once(' ') {
                        if channel != "general" {
                            self.write_buf
                                .extend_from_slice(&create_error(msg_id, "Unknown channel"));
                            warn!("Tried using invalid channel: {}", channel);
                            metrics.errors += 1;
                            continue;
                        }

                        // Format the broadcast: <Sender> <Channel> <Message>
                        let broadcast_payload =
                            format!("{} {} {}", sender_usr, channel, message_text);
                        let payload_bytes = broadcast_payload.as_bytes();
                        let response_len = (12 + payload_bytes.len()) as u32;

                        // 1. Send the broadcast back to the sender using a SERVER allocated ID
                        let sender_msg_id = self.next_server_msg_id;
                        self.next_server_msg_id = self.next_server_msg_id.wrapping_add(2); // Keep it Even

                        let mut sender_msg = Vec::with_capacity(response_len as usize);
                        sender_msg.extend_from_slice(&response_len.to_be_bytes());
                        sender_msg.extend_from_slice(&sender_msg_id.to_be_bytes());
                        sender_msg.extend_from_slice(b"MSG ");
                        sender_msg.extend_from_slice(payload_bytes);
                        self.write_buf.extend_from_slice(&sender_msg);

                        // 2. Broadcast to all OTHER users immediately
                        for other_client in clients.values_mut() {
                            if other_client.username.is_some() {
                                let other_msg_id = other_client.next_server_msg_id;
                                other_client.next_server_msg_id =
                                    other_client.next_server_msg_id.wrapping_add(2); // Keep it Even

                                let mut other_msg = Vec::with_capacity(response_len as usize);
                                other_msg.extend_from_slice(&response_len.to_be_bytes());
                                other_msg.extend_from_slice(&other_msg_id.to_be_bytes());
                                other_msg.extend_from_slice(b"MSG ");
                                other_msg.extend_from_slice(payload_bytes);

                                other_client.write_buf.extend_from_slice(&other_msg);

                                // Flush instantly
                                while !other_client.write_buf.is_empty() {
                                    match other_client.socket.write(&other_client.write_buf) {
                                        Ok(0) => {
                                            other_client.is_closed = true;
                                            break;
                                        }
                                        Ok(n) => {
                                            other_client.write_buf.drain(..n);
                                        }
                                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(err) => {
                                            other_client.is_closed = true;
                                            error!("Failed writing to client: {}", err);
                                            metrics.errors += 1;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Errors still use the client's `msg_id` as requested by the spec
                        self.write_buf.extend_from_slice(&create_error(
                            msg_id,
                            "Invalid MSG syntax. Expected: <channel> <message>",
                        ));
                        warn!("MSG not according to protocol");
                        metrics.errors += 1;
                    }
                }

                _ => {
                    self.write_buf
                        .extend_from_slice(&create_error(msg_id, "Unknown message type"));
                    warn!("Unknown message type");
                    metrics.errors += 1;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_creation_working() {
        let id = 12345;
        let error_message = "Very serious error";
        let error_vec: Vec<u8> = create_error(id, error_message);
        let len = error_vec.len();
        assert_eq!(
            u32::from_be_bytes(error_vec[..4].try_into().unwrap()),
            len as u32
        );
        assert_eq!(u32::from_be_bytes(error_vec[4..8].try_into().unwrap()), id);
        assert_eq!(str::from_utf8(&error_vec[12..len]), Ok(error_message));
    }
}
