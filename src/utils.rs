use std::sync::{Arc, Mutex};
use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::cmd::{RespValue, deserialize_array_command, parse_cmd, Command};
use crate::store::Store;

const BUFFER_SIZE_LIMIT: usize = 512; // in MB
const ERROR_UNKNOWN_COMMAND: &str = "ERR unknown command";
const ERROR_EMPTY_COMMAND: &str = "ERR empty command";

pub fn buf_to_string(buf: &mut BytesMut, size: usize) -> String {
    let utf8_str = String::from_utf8_lossy(&buf[..size]);
    return utf8_str.into_owned();
}

pub fn handle_command_response(cmd: Command, client_store: &Arc<Mutex<Store>>) -> String {
    let main_cmd = cmd.cmd;
    match main_cmd.to_ascii_uppercase().as_str() {
        "PING" => {
            RespValue::SimpleString("PONG".to_string()).encode()
        },
        "ECHO" => {
            if let Some(echo_arg) = cmd.args.get(0) {
                RespValue::SimpleString(echo_arg.to_string()).encode()
            } else {
                RespValue::SimpleString("".to_string()).encode()
            }
        }
        "SET" => {
            // SET [key] [value]
            if let (Some(key), Some(value)) = (cmd.args.get(0), cmd.args.get(1)) {
                client_store.lock().unwrap().set(key.clone(), value.clone());
                RespValue::SimpleString("OK".to_string()).encode()
            } else {
                RespValue::Error("SET requires exactly two arguments".to_string()).encode()
            }
        }
        "GET" => {
            // GET [key]
            if let Some(key) = cmd.args.get(0) {
                if let Some(value) = client_store.lock().unwrap().get(key.clone()) {
                    RespValue::SimpleString(value).encode()
                } else {
                    RespValue::SimpleString("-1".to_string()).encode()
                }
            } else {
                RespValue::Error("GET requires exactly one argument".to_string()).encode()
            }
        }
        _ => RespValue::Error(format!("{ERROR_UNKNOWN_COMMAND} '{main_cmd}'")).encode()
    }
}


/// Handle TCP connection from client
///
/// # Arguments
///
/// * `stream` - TCP Stream
/// * `client_store` Client Store
///
/// # Returns
///
/// Connection Result (Failed or not)
pub async fn handle_connection(mut stream: TcpStream, client_store: Arc<Mutex<Store>>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(BUFFER_SIZE_LIMIT);
    loop {
        let bytes_read = stream.read_buf(&mut buf).await?;
        if bytes_read == 0 {
            println!("Client closed the connection");
            break;
        }
        let cmd_str = buf_to_string(&mut buf, bytes_read);
        let resp = match deserialize_array_command(&cmd_str) {
            Some(cmd_array) => {
                let cmd = parse_cmd(cmd_array);
                handle_command_response(cmd, &client_store)
            }
            None => RespValue::Error(format!("{ERROR_EMPTY_COMMAND}")).encode()
        };

        stream.write(resp.as_bytes()).await?;
        buf.clear();
    }
    Ok(())
}
