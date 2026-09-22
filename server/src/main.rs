// Simple server that will read a test message and respond to confirm it is online and responsive

use std::{
    io::{self, Read, Write},
    net::TcpListener,
    str,
};

fn main() -> io::Result<()> {
    let address = "0.0.0.0:5015";

    let server = TcpListener::bind(address)?;

    'outer: loop {
        let (mut socket, address) = server.accept()?;
        println!("Accepted connection from: {}", address);

        let mut buf: [u8; 160] = [0; 160];
        let mut readn = socket.read(&mut buf)?;

        while readn < 4 {
            let n = socket.read(&mut buf[readn..4])?;

            if n == 0 {
                continue 'outer;
            }

            readn += n;
        }

        let len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;

        if len < 12 || len > buf.len() {
            eprintln!("Invalid message length: {}", len);
            continue 'outer;
        }

        while readn < len {
            let n = socket.read(&mut buf[readn..len])?;

            if n == 0 {
                continue 'outer;
            }

            readn += n
        }

        let identifier = u32::from_be_bytes(buf[4..8].try_into().unwrap());

        let message_type = &buf[8..12];

        let payload = str::from_utf8(&buf[12..len]).unwrap();

        match message_type {
            b"TST " => {
                let writen = socket.write(&mut buf[..len])?;
                println!("Wrote {} bytes", writen);
            }
            _ => {
                continue;
            }
        }
    }
}
