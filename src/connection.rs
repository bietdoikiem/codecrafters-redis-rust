use std::sync::{Arc, Mutex};
use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::cmd::{deserialize_command_into_array, parse_cmd, handle_command_response};
use crate::store::Store;

const BUFFER_SIZE_LIMIT: usize = 512; // in MB

pub fn buf_to_string(buf: &mut BytesMut, size: usize) -> String {
    let utf8_str = String::from_utf8_lossy(&buf[..size]);
    return utf8_str.into_owned();
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
        let resp = match deserialize_command_into_array(&cmd_str) {
            Some(cmd_array) => {
                let cmd = parse_cmd(cmd_array);
                handle_command_response(cmd, &client_store)
            }
            None => panic!("Invalid/Malformed command (0 byte)")
        };

        stream.write(resp.as_bytes()).await?;
        buf.clear();
    }
    Ok(())
}
