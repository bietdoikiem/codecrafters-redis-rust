mod utils;

use crate::utils::{deserialize_array_command, get_stream_input_str};
use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn encode_resp_simple_string(s: &str) -> Vec<u8> {
    let mut encoded = vec![];
    encoded.push(b'+');
    encoded.extend(s.as_bytes());
    encoded.push(b'\r');
    encoded.push(b'\n');
    encoded
}

fn handle_client(stream: &mut TcpStream) -> std::io::Result<()> {
    let response = encode_resp_simple_string("PONG");
    stream.write_all(&response)?;
    Ok(())
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let user_input = get_stream_input_str(&mut stream);
                match user_input {
                    Some(input) => {
                        let cmd_array = deserialize_array_command(&input).unwrap();
                        let cmd = cmd_array[0].clone().unwrap();
                        stream.write("+PONG\r\n".as_bytes()).unwrap();
                        // if cmd == "PING" {
                        //     stream.write("+PONG\r\n".as_bytes()).unwrap();
                        // } else {
                        //     stream
                        //         .write("-UNKNOWN_COMMAND_TYPE\r\n".as_bytes())
                        //         .unwrap();
                        // }
                    }
                    None => {
                        println!("invalid input");
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
