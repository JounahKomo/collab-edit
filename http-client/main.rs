use std::io::{self, Write, Read};

use std::net::TcpStream;

fn main() -> io::Result<()> {

    let mut stream = TcpStream::connect("pronets1.dice.aalto.fi:80")?;

    let json = r#"{"name": "komonej1","git-repo": "git@version.aalto.fi:komonej1/pns-project-joona-komonen.git","ports": "5015","protocol": "base-3"}"#;

    let new_message = format!(
    "POST /run-docker HTTP/1.1\r\n\
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
