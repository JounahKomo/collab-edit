use std::{
    env,
    io::{self, ErrorKind, Read, Write},
    net::TcpStream,
    str,
    sync::mpsc,
    thread,
    time::Duration,
};

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Server host to connect to
    #[arg(short, long, default_value = "127.0.0.1")]
    addr: String,

    /// Server port to connect to
    #[arg(short, long, default_value_t = 1234)]
    port: u16,

    /// Username to use in the chat
    #[arg(short, long, default_value = "User")]
    user: String,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let address = format!("{}:{}", args.addr, args.port);

    let mut write_stream = TcpStream::connect(&address)?;
    println!("Connecting to {}...", address);

    let mut read_stream = write_stream.try_clone().expect("Failed to clone stream");

    let (tx, rx) = mpsc::channel();

    // 4. Spawn the background reader thread
    thread::spawn(move || {
        let mut buffer = [0; 1024];
        loop {
            match read_stream.read(&mut buffer) {
                Ok(0) => break, // Connection gracefully closed by server
                Ok(n) => {
                    // Extract the exact bytes read and send them to the channel
                    let received_data = buffer[..n].to_vec();
                    if tx.send(received_data).is_err() {
                        // The receiver was dropped, safe to exit
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to read from socket: {e}");
                    break;
                }
            }
        }
    });

    let tst_message = create_message(1, "TST ", "Testing");
    write_stream.write_all(&tst_message)?;

    let response = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("Server timed out before responding");

    if response == tst_message {
        println!("Server is handling TST message correctly");
    } else {
        println!("Server is not handling the TST message correctly");
    }

    let invalid_message = create_message(3, "BYE ", "Testing");
    write_stream.write_all(&invalid_message)?;

    let response = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("Server timed out before responding");

    let (len, id, msg_type, payload) = decode_message(response.clone());

    if msg_type == String::from("ERR ") {
        println!("Server responded to invalid message type correctly");
    } else {
        println!("Server did not respond correctly to invalid message type");
    }

    let usr_message = create_message(5, "USR ", &args.user);
    write_stream.write_all(&usr_message)?;

    let response = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("Server timed out before responding");

    if response == usr_message {
        println!("Server is handling USR message correctly");
    } else {
        println!("Server is not handling the USR message correctly");
    }

    let msg_message = create_message(7, "MSG ", "general Hello world!");
    write_stream.write_all(&msg_message)?;

    let response = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("Server timed out before responding");

    let (response_len, _, response_type, response_payload) = decode_message(response);

    let msg_type = "MSG " == response_type;
    let usr = args.user + " ";
    let payload = (usr.clone() + "general Hello world!") == response_payload;
    let len = ((12 + "general Hello world!".len() + usr.len()) as u32) == response_len;

    if msg_type && payload && len {
        println!("Server is handling MSG message correctly");
    } else {
        println!("Server is not handling the MSG message correctly");
    }

    Ok(())
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

pub fn decode_message(message: Vec<u8>) -> (u32, u32, String, String) {
    let len = u32::from_be_bytes(message[..4].try_into().unwrap());

    let id = u32::from_be_bytes(message[4..8].try_into().unwrap());

    let msg_type = String::from_utf8(message[8..12].to_vec()).unwrap();

    let payload = String::from_utf8(message[12..(len as usize)].to_vec()).unwrap();

    (len, id, msg_type, payload)
}
