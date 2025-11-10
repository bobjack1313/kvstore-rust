//! # kvstore
//! A simple persistent key-value store built for the midterm project.
//!
//! ## Features
//! - Minimal command API: `SET <key> <value>`, `GET <key>`, `EXIT` / `QUIT`
//! - Append-only log for durability and crash recovery
//! - In-memory index with "last write wins" semantics
//! - Command parsing with case-insensitive commands
//!
//! ## Usage
//! This crate is primarily consumed by the binary in `main.rs`,
//! which provides the REPL interface. All reusable logic and
//! unit tests live here so the project can be tested with `cargo test`.
// =====================================================================
// File: lib.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Sept 20, 2025
//
//   This module implements the command-line interface (CLI)
//   that accepts the following commands:
//
//     `SET <key> <value>` -> Store a key-value pair
//     `GET <key>`         -> Retrieve the value for a key
//     `DEL <key>`         -> Deletes key entry: 1 if removed, 0 if not found
//     `EXISTS <key>`      -> Indicated presence of key: 1 if present and not expired, else 0
//     `MSET <k1> <v1> [<k2> <v2> ...]` -> Sets multiple keys: OK if valid
//     `MGET <k1> [<k2> ...]` -> Gets multiple keys: one line per key: the value or nil
//     `BEGIN`             -> To start a transaction (no nesting): OK if valid
//     `COMMIT`            -> Apply atomically buffered writes: OK if valid
//     `ABORT`             -> Discard buffer writes: OK if valid
//     `EXPIRE` <key> <milliseconds> -> Expires key: 1 if TTL set, 0 if key missing
//     `TTL <key>`         -> Remaining milliseconds (integer): -1 if no TTL, -2 if missing/expired
//     `PERSIST <key>`     -> Sets persist for key: 1 if TTL cleared, 0 otherwise
//     `RANGE <start> <end>` -> List keys in lexicographic order (inclusive):
//                              empty string means open bound; print one key per line then a final END
//     `EXIT`                -> Terminate the program
// =====================================================================
mod storage;
pub use storage::{append_write, replay_log, DATA_FILE};
pub mod index;
pub use index::{BTreeNode, BTreeIndex};
use std::io::{self, BufRead};


/// Result of handling a single user command.
///
/// - `Continue` means the REPL should keep running.
/// - `Exit` means the REPL should break out and terminate.
pub enum CommandResult {
    Continue,
    Exit,
}


/// Load persisted log data into a `BTreeIndex`.
///
/// Reads all entries from the log file and replays them into the
/// provided B-tree, so the in-memory state matches the persisted state.
///
/// # Arguments
///
/// * `index` - A mutable reference to the `BTreeIndex` that will be populated.
///
/// # Behavior
///
/// - Uses [`replay_log`](crate::replay_log) to read the log file.
/// - Inserts each `SET` entry into the B-tree.
/// - Ignores malformed lines.
///
/// # Example
/// ```
/// use kvstore::{BTreeIndex, load_data};
///
/// let mut index = BTreeIndex::new(2);
/// // Simulate persisted data
/// std::fs::write("data.db", "SET dog bark\nSET cat meow\n").unwrap();
///
/// load_data(&mut index);
///
/// assert_eq!(index.search("dog"), Some("bark"));
/// assert_eq!(index.search("cat"), Some("meow"));
/// ```
pub fn load_data(index: &mut BTreeIndex) {

    // Replay log before starting program
    if let Ok(data_records) = storage::replay_log(storage::DATA_FILE) {
        //println!("Replayed {} records from {}", data_records.len(), storage::DATA_FILE);

        for (idx, record) in data_records.iter().enumerate() {

            // Split "SET key value" into up to 3 parts
            let mut segments = record.splitn(3, char::is_whitespace);
            let cmd = segments.next().unwrap_or("");

            if cmd == "SET" {
                // Handle improper SET lines
                match (segments.next(), segments.next()) {
                    // Values exist, insert into tree
                    (Some(key), Some(value)) => {
                        index.insert(key.to_string(), value.to_string());
                    }
                    _ => {
                        eprintln!("Warning: malformed SET command at line {}: {}", idx + 1, record);
                    }
                }
            // Currently only SET should be in logs, NOTE: this must change if more logged cmds are added
            } else if !cmd.is_empty() {
                // Ignore blank lines silently, warn on unexpected command
                eprintln!("Warning: unknown command '{}' at line {}: {}", cmd, idx + 1, record);
            }
        }

    // Function replay_log failed to read the file
    } else {
        eprintln!(
            "Warning: could not read log file '{}'. Starting with empty index.",
            storage::DATA_FILE
        );
    }
}


/// Read, evaluate, and print loop to handle command line instructions.
///
/// Continuously reads commands from standard input, executes them against
/// the provided `BTreeIndex`, and prints results back to the user.
///
/// # Supported Commands
/// - `SET <key> <value>` — Insert or update a key-value pair.
/// - `GET <key>` — Retrieve the value for a key.
/// - `EXIT` — Quit the REPL.
///
/// # Arguments
///
/// * `index` - A mutable reference to the `BTreeIndex` that stores key–value data.
///
/// # Example
/// ```no_run
/// use kvstore::{BTreeIndex, repl_loop};
///
/// let mut index = BTreeIndex::new(2);
/// repl_loop(&mut index); // <- waits for user input interactively
///
pub fn repl_loop(index: &mut BTreeIndex) {
    let stdin = io::stdin();
    let proper_syntax = "Syntax Usage: GET <key>, SET <key> <value>, EXIT";

    // Form a loop to iterate over each input line; lock mutex
    for input_line in stdin.lock().lines() {
        // Unwrap because input_line is Result<String, std::io::Error>
        let full_command = input_line.unwrap();
        let (cmd, args) = parse_command(&full_command);

        // Process command and arguments
        match handle_command(&cmd, &args, proper_syntax, index) {
            CommandResult::Exit => break,
            CommandResult::Continue => (),
        }
    }
}


/// Parses a raw input line into a command and its arguments.
///
/// The first token is treated as the command (normalized to uppercase),
/// and the remaining tokens are collected as arguments. Leading and
/// trailing whitespace is ignored.
fn parse_command(line: &str) -> (String, Vec<String>) {
    let trimmed_line = line.trim();
    // Segment the command segments in a Vec[Str}] - handles whitespaces
    let mut command_segments = trimmed_line.split_whitespace();
    // Pulling out the command to nornmalize if lowercase is used
    let cmd = command_segments.next().unwrap_or("").to_uppercase();
    // Remaining arguments
    let args: Vec<String> = command_segments.map(|s| s.to_string()).collect();

    // Returning
    (cmd, args)
}


/// Handles a single user command and returns whether the REPL should continue or exit.
///
/// - Only supported commands will operate - Any other input: Prints an error and redisplays the syntax.
///
/// Returns:
/// - `CommandResult::Continue` if the loop should keep running.
/// - `CommandResult::Exit` if the user requested termination.
///
/// The `proper_syntax` argument is displayed in error messages to guide the user.
fn handle_command(cmd: &str, args: &[String], proper_syntax: &str, index: &mut BTreeIndex) -> CommandResult {
    // Watch - cmd is ref here
    match cmd.as_ref() {

        // Get command format:  GET <key>
        "GET" => {

            if let Some(key) = args.get(0) {
                match index.search(key) {
                    Some(value) => println!("{}", value),
                    None => println!("NULL"),
                }

            } else {
                println!("ERROR: GET requires a key");
            }
            CommandResult::Continue
        }

        // Set command format:  SET <key> <value>
        "SET" => {
            // Going larger than 2 for now
            if args.len() >= 2 {
                // Piece the segments together again
                let data_entry = format!("{} {} {}", cmd, args[0], args[1]);

                // Try to write to file
                if let Err(e) = append_write(storage::DATA_FILE, &data_entry) {
                    eprintln!("ERROR: failed to write to log file: {}", e);

               } else {
                    // Success log write - now store in mem
                    index.insert(args[0].clone(), args[1].clone());
                    //println!("Setting key: {} - value: {}", args[0], args[1]);
                }

            } else {
                // Error for not enough arguments for SET
                println!("ERROR: SET requires a key and value");
            }
            CommandResult::Continue
        }

        // Delete command format:  DEL <key>
        "DEL" => {
            if args.len() < 1 {
                // Error for not enough arguments for DEL
                println!("ERR: DEL requires a key");
            } else if args.len() > 1 {
                // Error for not enough arguments for DEL
                println!("ERR: Too many arguments for DEL");
            } else {
                // 1 Arg is correct
                if let Some(key) = args.get(0) {
                    match index.search(key) {
                        Some(_) => {
                            // Sucessful delete
                            index.delete(key);
                            println!("1");
                        }
                        // Key doesn't exist
                        None => println!("0"),
                    }
                } else {
                    println!("ERR: No Key found");
                }
            }
            CommandResult::Continue
        }

        // Exists command format:  EXISTS <key>
        "EXISTS" => {
            if args.len() < 1 {
                // Error for not enough arguments for EXISTS
                println!("ERR: EXISTS requires a key");
            } else if args.len() > 1 {
                // Error for not enough arguments for EXISTS
                println!("ERR: Too many arguments for EXISTS");
            } else {
                // 1 Arg is correct
                if let Some(key) = args.get(0) {
                    match index.search(key) {
                        // Key exists
                        Some(_) => {
                            // TODO: Implement command


                            //println!("1");
                            // Returning 0 for stub
                            println!("0");

                        }
                        // No key found
                        None => println!("0"),
                    }
                } else {
                    println!("ERR: No Key found");
                }
            }
            CommandResult::Continue
        }

        // MSET command format: MSET <k1> <v1> [<k2> <v2> ...]
        "MSET" => {
            // Could have many args
            if args.len() >= 2 {
                // TODO: Implement command

                    // Success log write - now store in mem
                 //   index.insert(args[0].clone(), args[1].clone());
                    //println!("Setting key: {} - value: {}", args[0], args[1]);

            } else {
                // Error for not enough arguments for MSET
                println!("ERROR: MSET requires a key and multiple values");
            }
            CommandResult::Continue
        }

        // MGET command <k1> [<k2> ...]
        "MGET" => {
            // Could have many args
            if args.len() >= 2 {
                // TODO: Implement command

                    // Success log write - now store in mem

            //        index.insert(args[0].clone(), args[1].clone());
                    //println!("Setting key: {} - value: {}", args[0], args[1]);


            } else {
                // Error for not enough arguments for MGET
                println!("ERROR: MGET requires a key and multiple values");
            }
            CommandResult::Continue
        }

        // BEGIN command
        "BEGIN" => {
            // Could have many args
            if args.len() > 1 {
                // TODO: Implement command

            } else {
                // Error for added arguments
                println!("ERROR: Too many arguments for BEGIN");
            }
            CommandResult::Continue
        }

        // COMMIT command
        "COMMIT" => {
            // Could have many args
            if args.len() > 1 {
                // TODO: Implement command
            } else {
                // Error for added arguments
                println!("ERROR: Too many arguments for COMMIT");
            }
            CommandResult::Continue
        }

        // ABORT command
        "ABORT" => {
            // Could have many args
            if args.len() > 1 {
                // TODO: Implement command
            } else {
                // Error for added arguments
                println!("ERROR: Too many arguments for ABORT");
            }
            CommandResult::Continue
        }

        "EXPIRE" => {
            if args.len() == 2 {

            // TODO: Implement command
            } else {
                // Error for not enough arguments for EXPIRE
                println!("ERROR: EXPIRE requires a key and millisecond value");
            }
            CommandResult::Continue
        }

        "TTL" => {
            if args.len() >= 2 {

            // TODO: Implement command
            //     `TTL <key>`         -> Remaining milliseconds (integer): -1 if no TTL, -2 if missing/expired
            } else {
                // Error for not enough arguments for TTL
                println!("ERROR: TTL requires a key and millisecond value");
            }
            CommandResult::Continue
        }

        "PERSIST" => {
            if args.len() >= 2 {

            // TODO: Implement command

            } else {
                // Error for not enough arguments for PERSIST
                println!("ERR: PERSIST requires a key");
            }
            CommandResult::Continue
        }

        "RANGE" => {
            if args.len() >= 2 {

            // TODO: Implement command
//     `RANGE <start> <end>` -> List keys in lexicographic order (inclusive):
//                              empty string means open bound; print one key per line then a final END

            } else {
                // Error for not enough arguments for RANGE
                println!("ERR: RANGE requires a start and end");
            }
            CommandResult::Continue
        }

        // Exit command
        "EXIT" => {
            println!("Exiting...");
            CommandResult::Exit
        }

        // Empty input
        "" => {
            println!("Enter a command.");
            CommandResult::Continue
        }

        // Everything else will be noted and returned as an error
        _ => {

            // Unrecognized commands
            println!("ERROR: command '{}' not handled", cmd);
            println!("{}", proper_syntax);
            CommandResult::Continue
        }
    }
}



// =================================================================
// lib.rs Unit tests
// =================================================================

#[cfg(test)]
mod main_lib_tests {
    use crate::BTreeIndex;
    use super::*;

    #[test]
    fn test_parse_exit_command() {
        let (cmd, args) = parse_command("EXIT");
        assert_eq!(cmd, "EXIT");
        assert!(args.is_empty());
    }

    #[test]
    fn test_exit_command() {
        let (cmd, args) = parse_command("EXIT");
        let mut tree = BTreeIndex::new(2);
        let result = handle_command(&cmd, &args, "Usage", &mut tree);
        assert!(matches!(result, CommandResult::Exit));
    }

    #[test]
    fn test_parse_get_command() {
        let (cmd, args) = parse_command("GET dog");
        assert_eq!(cmd, "GET");
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], "dog");
    }

    #[test]
    fn test_parse_set_command() {
        let (cmd, args) = parse_command("SET frankenstein wobble");
        assert_eq!(cmd, "SET");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "frankenstein");
        assert_eq!(args[1], "wobble");
    }

    #[test]
    fn test_parse_invalid_command() {
        let (cmd, args) = parse_command("FLY away");
        assert_eq!(cmd, "FLY");
        assert_eq!(args[0], "away");

        let mut tree = BTreeIndex::new(2);
        let result = handle_command(&cmd, &args, "Usage", &mut tree);
        // Should not exit on bad command
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_get_missing_key() {
        let (cmd, args) = parse_command("GET");
        assert_eq!(cmd, "GET");
        assert!(args.is_empty());
        let mut tree = BTreeIndex::new(2);
        let result = handle_command(&cmd, &args, "Usage", &mut tree);
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_set_missing_value() {
        let (cmd, args) = parse_command("SET justonekey");
        assert_eq!(cmd, "SET");
        assert_eq!(args.len(), 1);
        let mut tree = BTreeIndex::new(2);
        let result = handle_command(&cmd, &args, "Usage", &mut tree);
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_whitespace_command() {
        let (cmd, args) = parse_command("   SET   allthis         space      ");
        assert_eq!(cmd, "SET");
        assert_eq!(args, vec!["allthis", "space"]);
    }

    #[test]
    fn test_lower_upper_commands() {
        let (cmd, args) = parse_command("seT anykey goats");
        assert_eq!(cmd, "SET");
        assert_eq!(args, vec!["anykey", "goats"]);
    }

    #[test]
    fn test_del_command() {
        let mut tree = BTreeIndex::new(2);

        // First, insert a key to delete
        handle_command("SET", &vec!["mykey".to_string(), "myvalue".to_string()], "Usage", &mut tree);

        // Delete existing key (expect success = 1)
        let (cmd, args) = parse_command("DEL mykey");
        assert_eq!(cmd, "DEL");
        assert_eq!(args.len(), 1);
        let result = handle_command(&cmd, &args, "Usage", &mut tree);
        assert!(matches!(result, CommandResult::Continue));

        // Delete non-existing key (expect fail = 0)
        let (cmd2, args2) = parse_command("DEL notfound");
        assert_eq!(cmd2, "DEL");
        assert_eq!(args2.len(), 1);
        let result2 = handle_command(&cmd2, &args2, "Usage", &mut tree);
        assert!(matches!(result2, CommandResult::Continue));
    }
}
