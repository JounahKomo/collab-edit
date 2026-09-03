use std::io::{self, Write, Read};

use std::net::TcpStream;

fn main() -> io::Result<()> {
    
    let address = "www.aalto.fi:80";

    let mut stream = TcpStream::connect(address)?;

    println!("Connected to {}", address);

    let message = "GET /index.html HTTP/1.1\r\nHost: www.aalto.fi\r\n\r\n";

    stream.write_all(message.as_bytes())?;

    println!("Sent request: {}", message);

    Ok(())

}