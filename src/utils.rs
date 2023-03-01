use std::sync::{Arc, Mutex};
use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::store::Store;

const CARRIAGE_RETURN: char = '\r';
const ARRAY_DENOTE: char = '*';
const BULK_STRING_DENOTE: char = '$';
const SIMPLE_STRING_DENOTE: char = '+';
const ERROR_DENOTE: char = '-';
const NULL_DENOTE_STR: &str = "$-1";
const BUFFER_SIZE_LIMIT: usize = 512; // in Megabytes
const ERROR_UNKNOWN_COMMAND: &str = "ERR unknown command";
const ERROR_EMPTY_COMMAND: &str = "ERR empty command";
const CRLF: &str = "\r\n";

pub fn buf_to_string(buf: &mut BytesMut, size: usize) -> String {
    let utf8_str = String::from_utf8_lossy(&buf[..size]);
    return utf8_str.into_owned();
}

/// Deserialize array command
///
/// # Arguments
///
/// * `cmd` - Command string
///
/// # Returns
///
/// List of commands parsed from ReSP format
pub fn deserialize_array_command(cmd: &String) -> Option<Vec<Option<String>>> {
    let cmd_len = cmd.len();
    if cmd_len == 0 {
        return None;
    }

    let mut cmd_array: Vec<Option<String>> = vec![];

    // Flow-control pointer vars
    let mut cur_idx = 0;
    let mut cmd_iterator = cmd.chars();
    let mut parsing_array_len = false;
    let mut parsing_array_content = false;
    let mut parsing_bulk_string_len = false;
    let mut array_lower_bound = 0;
    let mut array_prefix_len = -1;
    let mut bulk_string_prefix_len = -1;
    let mut bulk_string_len_lower_bound = 0;

    while cur_idx < cmd_len {
        let cur_char = cmd_iterator.next().unwrap();
        match cur_char {
            ARRAY_DENOTE => {
                parsing_array_len = true;
                array_lower_bound = cur_idx + 1; // Next char
            }
            CARRIAGE_RETURN => {
                if array_prefix_len == 0 {
                    break;
                }
                if parsing_array_len {
                    let prefix_length_str = &cmd[array_lower_bound..cur_idx];
                    match prefix_length_str.parse::<i64>() {
                        Ok(val) => {
                            array_prefix_len = val;
                            if array_prefix_len == -1 {
                                return None;
                            }
                        }
                        Err(e) => {
                            println!("error parsing integer: {}", e)
                        }
                    }
                    parsing_array_len = false;
                } else if parsing_bulk_string_len {
                    let bulk_string_prefix_len_str = &cmd[bulk_string_len_lower_bound..cur_idx];
                    match bulk_string_prefix_len_str.parse::<i64>() {
                        Ok(val) => {
                            bulk_string_prefix_len = val;
                        }
                        Err(e) => {
                            println!("error parsing integer: {}", e)
                        }
                    }
                    // If got the length
                    parsing_bulk_string_len = false;
                    parsing_array_content = true;
                }

                // Skip next LF
                cmd_iterator.next();
                cur_idx += 1;
            }
            BULK_STRING_DENOTE => {
                bulk_string_len_lower_bound = cur_idx + 1;
                parsing_bulk_string_len = true;
            }
            _ => {
                if parsing_array_content {
                    if bulk_string_prefix_len == -1 {
                        cmd_array.push(None);
                    } else {
                        let content_slice =
                            &cmd[cur_idx..cur_idx + bulk_string_prefix_len as usize];

                        cmd_array.push(Some(content_slice.to_string()));

                        // Skip processed bulk string prefix size
                        cur_idx += bulk_string_prefix_len as usize;
                        for _ in 0..=bulk_string_prefix_len - 1 {
                            cmd_iterator.next();
                        }
                        parsing_array_content = false;
                    }
                }
            }
        };
        cur_idx += 1;
    }
    Some(cmd_array)
}

struct Command {
    cmd: String,
    args: Vec<String>,
}

/// Handle TCP connection from client
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
                let cmd = parse_simple_cmd(cmd_array);
                let main_cmd = cmd.cmd;
                match main_cmd.to_ascii_uppercase().as_str() {
                    "PING" => {
                        format!("{SIMPLE_STRING_DENOTE}PONG{CRLF}")
                    },
                    "ECHO" => {
                        if let Some(echo_arg) = cmd.args.get(0) {
                            format!("{SIMPLE_STRING_DENOTE}{echo_arg}{CRLF}")
                        } else {
                            format!("{SIMPLE_STRING_DENOTE}{CRLF}")
                        }
                    }
                    "SET" => {
                        // SET [key] [value]
                        if let (Some(key), Some(value)) = (cmd.args.get(0), cmd.args.get(1)) {
                            client_store.lock().unwrap().set(key.clone(), value.clone());
                            format!("{SIMPLE_STRING_DENOTE}OK{CRLF}")
                        } else {
                            format!("{ERROR_DENOTE}SET requires two arguments{CRLF}")
                        }
                    }
                    "GET" => {
                        // GET [key]
                        if let Some(key) = cmd.args.get(0) {
                            if let Some(value) = client_store.lock().unwrap().get(key.clone()) {
                                format!("{SIMPLE_STRING_DENOTE}{value}{CRLF}")
                            } else {
                                format!("{NULL_DENOTE_STR}{CRLF}")
                            }
                        } else {
                            format!("{ERROR_DENOTE}GET requires one argument{CRLF}")
                        }
                    }
                    _ => format!("{ERROR_DENOTE}{ERROR_UNKNOWN_COMMAND} '{main_cmd}'{CRLF}")
                }
            }
            None => {
                format!("{ERROR_DENOTE}{ERROR_EMPTY_COMMAND}{CRLF}")
            }
        };
        stream.write(resp.as_bytes()).await?;
        buf.clear();
    }
    Ok(())
}

/// Parse simple command with 1 argument only
///
/// # Arguments
///
/// * `cmd_array` - Command array (including argument)
fn parse_simple_cmd(cmd_array: Vec<Option<String>>) -> Command {
    let cmd_str = match cmd_array.get(0) {
        Some(main_cmd) => main_cmd.as_ref().unwrap().to_string(),
        None => {
            panic!("Command is null");
        }
    };

    let mut cmd_args = vec![];

    // Add arguments
    for (_, arg) in cmd_array.iter().skip(1).enumerate() {
        cmd_args.push(arg.as_ref().unwrap().to_string());
    }

    Command {
        cmd: cmd_str,
        args: cmd_args,
    }
}

#[cfg(test)]
mod utils_tests {
    use super::deserialize_array_command;

    #[test]
    fn test_deserialize_array_command_successfully() {
        let test_cmd = String::from("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n");
        let expect_array: Vec<Option<String>> =
            vec![Some(String::from("PING")), Some(String::from("PONG"))];
        let cmd_array = deserialize_array_command(&test_cmd);
        assert_eq!(expect_array, cmd_array.unwrap());
    }

    #[test]
    fn test_deserialize_array_2_commands_successfully() {
        let test_cmd1 = String::from("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n");
        let expect_array1: Vec<Option<String>> =
            vec![Some(String::from("PING")), Some(String::from("PONG"))];
        let cmd_array1 = deserialize_array_command(&test_cmd1);
        assert_eq!(expect_array1, cmd_array1.unwrap());

        let expect_array2: Vec<Option<String>> = vec![Some(String::from("PING"))];
        let test_cmd2 = String::from("*1\r\n$4\r\nPING\r\n");
        let cmd_array2 = deserialize_array_command(&test_cmd2);
        assert_eq!(expect_array2, cmd_array2.unwrap());
    }
}
