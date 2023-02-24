use std::io::Read;
use std::net::TcpStream;

const CARRIAGE_RETURN: char = '\r';
const ARRAY_DENOTE: char = '*';
const BULK_STRING_DENOTE: char = '$';

/// Get input string from TCP stream
///
/// # Arguments
///
/// * `stream` - TCP Stream
///
/// # Returns
///
/// The string from stream's buffer
pub fn get_stream_input_str(stream: &mut TcpStream) -> Result<String, &str> {
    let mut buffer: [u8; 512] = [0; 512];
    match stream.read(&mut buffer) {
        Ok(size) => {
            if size == 0 {
                return Err("client closed the connection");
            }
            let input_cow_str = String::from_utf8_lossy(&buffer[..size]);
            let input_owned_str = input_cow_str.into_owned();
            return Ok(input_owned_str);
        }
        Err(e) => {
            return Err("error parsing input");
        }
    }
}

/// Deserialize array command
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

#[cfg(test)]
mod utils_tests {
    use super::deserialize_array_command;

    #[test]
    fn test_deserialize_array_command_successfully() {
        let test_cmd = String::from("*3\r\n$4\r\nPING\r\n$4\r\nPONG\r\n");
        let expect_array: Vec<Option<String>> =
            vec![Some(String::from("PING")), Some(String::from("PONG"))];
        let cmd_array = deserialize_array_command(&test_cmd);
        assert_eq!(expect_array, cmd_array.unwrap());
    }
}
