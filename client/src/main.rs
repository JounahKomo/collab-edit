// Simple client that will send a test message to server

use std::{
    env,
    io::{self, ErrorKind, Read, Write},
    net::TcpStream,
    str,
};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("arguments: <host>:<port>");
        return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid command"));
    }

    let mut stream = TcpStream::connect(&args[1])?;
    println!("Connected to: {}", &args[1]);

    let identifier: u32 = 0;
    let content = b"Hello server!";

    let length = 4 + 4 + 4 + content.len();

    let mut message = Vec::new();

    message.extend_from_slice(&(length as u32).to_be_bytes());
    message.extend_from_slice(&identifier.to_be_bytes());
    message.extend_from_slice(b"TST ");
    message.extend_from_slice(content);

    let writen = stream.write(&message)?;
    println!("Wrote {} bytes to server", writen);

    let mut buf: [u8; 160] = [0; 160];
    let readn = stream.read(&mut buf)?;

    let message = str::from_utf8(&buf[..readn]).unwrap();
    println!("Received message {} from server", message);

    Ok(())
}
