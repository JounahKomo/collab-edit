use std::io::{self, Write, Read};

use std::net::TcpStream;

fn main() -> io::Result<()> {
    
    let address = "www.aalto.fi:80";

    let mut stream = TcpStream::connect(address)?;

    println!("Connected to {}", address);

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();

    let message = "GET /index.html HTTP/1.1\r\nHost: www.aalto.fi\r\n\r\n";

    stream.write_all(message.as_bytes())?;

    println!("Sent request: {}", message);

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    println!("Received response: {}", response);

    stream = TcpStream::connect("pronets1.dice.aalto.fi:80")?;

    let json = r#"{"name": "komonej1","git-repo": "git@version.aalto.fi:komonej1/pns-project-joona-komonen.git"}"#;

    let new_message = format!(
    "POST /fetch-git HTTP/1.1\r\n\
    Host: pronets1.dice.aalto.fi\r\n\
    Content-Type: application/json\r\n\
    Content-Length: {}\r\n\
    Connection: close\r\n\
    \r\n\
    {}",
    json.len(),
    json
    );

    stream.write_all(new_message.as_bytes())?;

    println!("Wrote to server: {}", new_message);

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    println!("Received response: {}", response);

    Ok(())

}