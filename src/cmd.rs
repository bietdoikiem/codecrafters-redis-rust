use std::sync::{Arc, Mutex};
use crate::store::Store;

const CARRIAGE_RETURN: char = '\r';
const ARRAY_DENOTE: char = '*';
const BULK_STRING_DENOTE: char = '$';
const SIMPLE_STRING_DENOTE: char = '+';
const ERROR_DENOTE: char = '-';
const NULL_DENOTE: &str = "-1";
const CRLF: &str = "\r\n";

const ERROR_UNKNOWN_COMMAND: &str = "ERR unknown command";

pub enum RespValue {
    SimpleString(String),
    BulkString(String),
    Error(String),
}

impl RespValue {
    pub fn encode(self) -> String {
        match &self {
            RespValue::SimpleString(val) => format!("{SIMPLE_STRING_DENOTE}{val}{CRLF}"),
            RespValue::BulkString(val) => format!("{BULK_STRING_DENOTE}{val}{CRLF}"),
            RespValue::Error(msg) => format!("{ERROR_DENOTE}{msg}{CRLF}")
        }
    }
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
pub fn deserialize_command_into_array(cmd: &String) -> Option<Vec<Option<String>>> {
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


pub struct Command {
    pub cmd: String,
    pub args: Vec<String>,
}

/// Parse simple command with 1 argument only
///
/// # Arguments
///
/// * `cmd_array` - Command array (including argument)
pub fn parse_cmd(cmd_array: Vec<Option<String>>) -> Command {
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



/// Handle command RESP-format response
///
/// # Arguments
///
/// * `cmd`: Command
pub fn handle_command_response(command: Command, client_store: &Arc<Mutex<Store>>) -> String {
    let cmd = command.cmd;
    match cmd.to_ascii_uppercase().as_str() {
        "PING" => {
            RespValue::SimpleString("PONG".to_string()).encode()
        },
        "ECHO" => {
            let args = command.args;
            if let Some(echo_arg) = args.get(0) {
                RespValue::SimpleString(echo_arg.to_string()).encode()
            } else {
                RespValue::SimpleString("".to_string()).encode()
            }
        }
        "SET" => {
            // SET [key] [value]
            let args = command.args;
            if let (Some(key), Some(value)) = (args.get(0), args.get(1)) {

                if let (Some(_), Some(expiry)) = (args.get(2), args.get(3)) {
                    client_store.lock().unwrap().set_px(key.clone(), value.clone(), expiry.parse::<u64>().unwrap());
                } else {
                    client_store.lock().unwrap().set(key.clone(), value.clone());
                }

                RespValue::SimpleString("OK".to_string()).encode()
            } else {
                RespValue::Error("SET requires exactly two arguments".to_string()).encode()
            }
        }
        "GET" => {
            // GET [key]
            let args = command.args;
            if let Some(key) = args.get(0) {
                if let Some(value) = client_store.lock().unwrap().get(key.clone()) {
                    RespValue::SimpleString(value).encode()
                } else {
                    RespValue::BulkString(NULL_DENOTE.to_string()).encode()
                }
            } else {
                RespValue::Error("GET requires exactly one argument".to_string()).encode()
            }
        }
        _ => RespValue::Error(format!("{ERROR_UNKNOWN_COMMAND} '{cmd}'")).encode()
    }
}


// --- TESTING ---

#[cfg(test)]
mod cmd_tests {
    use super::deserialize_command_into_array;

    #[test]
    fn test_deserialize_array_command_successfully() {
        let test_cmd = String::from("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n");
        let expect_array: Vec<Option<String>> =
            vec![Some(String::from("PING")), Some(String::from("PONG"))];
        let cmd_array = deserialize_command_into_array(&test_cmd);
        assert_eq!(expect_array, cmd_array.unwrap());
    }

    #[test]
    fn test_deserialize_array_2_commands_successfully() {
        let test_cmd1 = String::from("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n");
        let expect_array1: Vec<Option<String>> =
            vec![Some(String::from("PING")), Some(String::from("PONG"))];
        let cmd_array1 = deserialize_command_into_array(&test_cmd1);
        assert_eq!(expect_array1, cmd_array1.unwrap());

        let expect_array2: Vec<Option<String>> = vec![Some(String::from("PING"))];
        let test_cmd2 = String::from("*1\r\n$4\r\nPING\r\n");
        let cmd_array2 = deserialize_command_into_array(&test_cmd2);
        assert_eq!(expect_array2, cmd_array2.unwrap());
    }
}
