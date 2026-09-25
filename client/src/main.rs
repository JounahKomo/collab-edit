use std::{
    env,
    io::{self, ErrorKind, Read, Write},
    net::TcpStream,
    str, thread,
};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("arguments: <host>:<port>");
        return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid command"));
    }

    let mut stream = TcpStream::connect(&args[1])?;
    println!("Connected to: {}", &args[1]);
    println!("Type commands in format: TYPE message (e.g. 'USR Jaakko' or 'MSG general Hello')");

    // Clone the socket so a background thread can receive data while you type
    let mut read_stream = stream.try_clone()?;

    // --- RECEIVE THREAD (Runs permanently in background) ---
    thread::spawn(move || {
        loop {
            let mut header = [0u8; 12];
            
            // This blocks purely waiting for new data. It has NO timeouts.
            if read_stream.read_exact(&mut header).is_err() {
                println!("\nDisconnected from server.");
                break;
            }

            let length = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
            let id = u32::from_be_bytes(header[4..8].try_into().unwrap());
            let msg_type = String::from_utf8_lossy(&header[8..12]);

            let payload_len = length.saturating_sub(12);
            let mut payload = vec![0u8; payload_len];
            
            if read_stream.read_exact(&mut payload).is_err() {
                break;
            }

            let text = str::from_utf8(&payload).unwrap_or("<invalid utf-8>");
            
            // \r clears your current line so incoming messages neatly display
            println!("\r[ID:{} {}] {}\n> ", id, msg_type, text);
            io::stdout().flush().unwrap();
        }
    });

    // --- SEND LOOP (Main thread handles typing) ---
    let mut msg_id: u32 = 1;
    
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let args: Vec<&str> = input.split_whitespace().collect();

        if args[0].eq_ignore_ascii_case("quit") {
            break;
        }

        if args[0].len() != 3 {
            println!("Error: Message type must be exactly 3 characters (e.g. USR, MSG, TST)");
            continue;
        }

		let current_id = msg_id;
		msg_id = msg_id.wrapping_add(2);
        let content = if args.len() > 1 { args[1..].join(" ") } else { String::new() };
        let mut msg_type_str = args[0].to_uppercase();
        msg_type_str.push(' '); // E.g., "MSG "

        let length = (12 + content.len()) as u32;

        let mut message = Vec::new();
        message.extend_from_slice(&length.to_be_bytes());
        message.extend_from_slice(&current_id.to_be_bytes());
        message.extend_from_slice(msg_type_str.as_bytes());
        message.extend_from_slice(content.as_bytes());

        if let Err(e) = stream.write_all(&message) {
            eprintln!("Failed to send message: {}", e);
            break;
        }
    }
    
    Ok(())
}
