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

pub mod ttl;
pub use ttl::TTLManager;

pub mod transaction;
pub use transaction::Transaction;

pub mod session;
pub use session::Session;

use std::io::{self, BufRead};
use std::time::Instant;
use std::collections::HashMap;

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


/// Read–Evaluate–Print Loop (REPL) to handle interactive command input.
///
/// Continuously reads user commands from standard input, executes them
/// against the current [`Session`] (which includes the B-tree index,
/// TTL manager, and optional transaction state), and prints responses
/// back to standard output.
///
/// # Arguments
/// * `session` - A mutable reference to the active [`Session`],
///   which manages the key–value index, TTL expirations, and
///   transaction state.
///
/// # Example
/// ```no_run
/// use kvstore::{Session, repl_loop};
///
/// let mut session = Session::new();
/// repl_loop(&mut session); // <- waits for user input interactively
/// ```
pub fn repl_loop(session: &mut Session) {
    let stdin = io::stdin();
    let proper_syntax = "Syntax Usage: GET <key>, SET <key> <value>, EXIT";

    // Form a loop to iterate over each input line; lock mutex
    for input_line in stdin.lock().lines() {
        // Unwrap because input_line is Result<String, std::io::Error>
        let full_command = input_line.unwrap();
        let (cmd, args) = parse_command(&full_command);

        // Process command and arguments
        match handle_command(&cmd, &args, proper_syntax, session) {
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
fn handle_command(cmd: &str, args: &[String], proper_syntax: &str, session: &mut Session) -> CommandResult {
    // Watch - cmd is ref here
    match cmd.as_ref() {

        // Get command format:  GET <key>
        "GET" => {

            if let Some(key) = args.get(0) {
                match session.index.search(key) {
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
                    session.index.insert(args[0].clone(), args[1].clone());
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
                    match session.index.search(key) {
                        Some(_) => {
                            // Sucessful delete
                            session.index.delete(key);
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
                    match session.index.search(key) {
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
            // Must be even number of args: pairs of key/value
            if args.len() < 2 || args.len() % 2 != 0 {
                println!("ERR: MSET requires key/value pairs (even number of arguments)");
                return CommandResult::Continue;
            }

            // Build a single log line for persistence
            let mut log_entry = String::from(cmd);
            for arg in args {
                log_entry.push(' ');
                log_entry.push_str(arg);
            }

            // Append to log file first
            if let Err(e) = append_write(storage::DATA_FILE, &log_entry) {
                eprintln!("ERR: failed to write MSET batch to log file: {}", e);
            } else {
                // Apply all kv pairs to memory
                for pair in args.chunks(2) {
                    let key = pair[0].clone();
                    let value = pair[1].clone();
                    session.index.insert(key, value);
                }
                println!("OK");
            }
            CommandResult::Continue
        }

        // MGET command <k1> [<k2> ...]
        "MGET" => {
            if args.is_empty() {
                println!("ERR MGET requires at least one key");
                return CommandResult::Continue;
            }

            for key in args {
                // Check TTL first (treat expired as absent)
                if session.ttl.is_expired(key) {
                    println!("nil");
                    continue;
                }

                match session.index.search(key) {
                    Some(value) => println!("{}", value),
                    None => println!("nil"),
                }
            }
            CommandResult::Continue
        }

        // BEGIN command — start a new transaction session
        "BEGIN" => {
            if !args.is_empty() {
                println!("ERR: BEGIN does not take any arguments");
            } else {
                // Check if we have one
                if session.in_transaction() {
                    println!("ERR: Transaction already active");
                } else {
                    // Create a new trans
                    session.begin_transaction();
                    println!("OK: Transaction started");
                }
            }
            CommandResult::Continue
        }

        // COMMIT command — finalize an active transaction
        "COMMIT" => {
            if !args.is_empty() {
                println!("ERR: COMMIT does not take any arguments");
            } else {
                // Check if a trans exists
                if session.in_transaction() {
                    session.commit_transaction();
                    println!("OK: Transaction committed");
                } else {
                    // No trans in play
                    println!("ERR: No active transaction to commit");
                }
            }
            CommandResult::Continue
        }

        // ABORT command — discard any active transaction
        "ABORT" => {
            if !args.is_empty() {
                println!("ERR: ABORT does not take any arguments");
            } else {
                // Check if a trans exists
                if session.in_transaction() {
                    session.abort_transaction();
                    println!("OK: Transaction aborted");
                } else {
                    // No trans in play
                    println!("ERR: No active transaction to abort");
                }
            }
            CommandResult::Continue
        }

        // EXPIRE command — assign a TTL to a key
        "EXPIRE" => {
            if args.len() == 2 {
                let key = &args[0];
                let ms_str = &args[1];

                // Parse milliseconds argument
                match ms_str.parse::<i64>() {
                    Ok(ms) if ms > 0 => {
                        // Check if key exists in the index before applying TTL
                        if session.index.search(key).is_some() {
                            session.ttl.set_expiration(key, ms);
                            println!("OK: Expiration set for key '{}'", key);
                        } else {
                            println!("ERR: Key '{}' does not exist", key);
                        }
                    }
                    Ok(_) => println!("ERR: Expiration time must be greater than zero"),
                    Err(_) => println!("ERR: Invalid millisecond value '{}'", ms_str),
                }
            } else {
                println!("ERR: EXPIRE requires a key and millisecond value");
            }
            CommandResult::Continue
        }

        // TTL command — report remaining time to live for a key
        "TTL" => {
            if args.len() == 1 {
                let key = &args[0];

                // First, check if key exists at all
                if session.index.search(key).is_none() {
                    println!("-2"); // Missing key
                } else {
                    // Query TTL remaining (lazy cleanup inside function)
                    let ttl_ms = session.ttl.ttl_remaining(key);

                    // Print result according to contract
                    println!("{}", ttl_ms);
                }
            } else {
                println!("ERR: TTL requires exactly one argument <key>");
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
        let mut session = Session::new();
        let result = handle_command(&cmd, &args, "Usage", &mut session);
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

        let mut session = Session::new();
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        // Should not exit on bad command
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_get_missing_key() {
        let (cmd, args) = parse_command("GET");
        assert_eq!(cmd, "GET");
        assert!(args.is_empty());
        let mut session = Session::new();
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_set_missing_value() {
        let (cmd, args) = parse_command("SET justonekey");
        assert_eq!(cmd, "SET");
        assert_eq!(args.len(), 1);
        let mut session = Session::new();
        let result = handle_command(&cmd, &args, "Usage", &mut session);
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
        let mut session = Session::new();

        // First, insert a key to delete
        handle_command("SET", &vec!["mykey".to_string(), "myvalue".to_string()], "Usage", &mut session);

        // Delete existing key (expect success = 1)
        let (cmd, args) = parse_command("DEL mykey");
        assert_eq!(cmd, "DEL");
        assert_eq!(args.len(), 1);
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Delete non-existing key (expect fail = 0)
        let (cmd2, args2) = parse_command("DEL notfound");
        assert_eq!(cmd2, "DEL");
        assert_eq!(args2.len(), 1);
        let result2 = handle_command(&cmd2, &args2, "Usage", &mut session);
        assert!(matches!(result2, CommandResult::Continue));
    }

    #[test]
    fn test_mset_inserts_multiple_keys() {
        let mut session = Session::new();

        // Issue MSET command with multiple pairs
        let (cmd, args) = parse_command("MSET dog bark cat meow cow moo");
        assert_eq!(cmd, "MSET");
        assert_eq!(args.len(), 6); // 3 key–value pairs

        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Verify keys were inserted
        assert_eq!(session.index.search("dog"), Some("bark"));
        assert_eq!(session.index.search("cat"), Some("meow"));
        assert_eq!(session.index.search("cow"), Some("moo"));
    }

    #[test]
    fn test_mget_retrieves_multiple_keys() {
        let mut session = Session::new();

        // Prepopulate data
        handle_command("SET", &vec!["dog".into(), "bark".into()], "Usage", &mut session);
        handle_command("SET", &vec!["cat".into(), "meow".into()], "Usage", &mut session);
        handle_command("SET", &vec!["cow".into(), "moo".into()], "Usage", &mut session);

        // Retrieve with MGET
        let (cmd, args) = parse_command("MGET dog cat horse");
        assert_eq!(cmd, "MGET");
        assert_eq!(args.len(), 3);

        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Confirm correct state of index — horse should not exist
        assert_eq!(session.index.search("dog"), Some("bark"));
        assert_eq!(session.index.search("cat"), Some("meow"));
        assert_eq!(session.index.search("horse"), None);
    }

    #[test]
    fn test_mget_with_expired_key() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut session = Session::new();

        // Insert two keys and expire one
        handle_command("SET", &vec!["temp".into(), "123".into()], "Usage", &mut session);
        handle_command("SET", &vec!["perm".into(), "456".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["temp".into(), "50".into()], "Usage", &mut session);

        sleep(Duration::from_millis(60)); // Allow TTL to expire

        let (cmd, args) = parse_command("MGET temp perm");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Only "perm" should still exist
        assert!(!session.ttl.has_entry("temp"), "Expired key should have been removed");
        assert_eq!(session.index.search("perm"), Some("456"));
    }

    #[test]
    fn test_begin_starts_new_transaction() {
        let mut session = Session::new();

        // Ensure no transaction at start
        assert!(!session.in_transaction());

        // Execute BEGIN command
        let (cmd, args) = parse_command("BEGIN");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // The REPL should continue after BEGIN
        assert!(matches!(result, CommandResult::Continue));

        // Verify session now has an active transaction
        assert!(session.in_transaction(), "BEGIN should create a transaction");
    }

    #[test]
    fn test_begin_rejects_arguments() {
        let mut session = Session::new();

        // BEGIN should not take arguments
        let (cmd, args) = parse_command("BEGIN extra_arg");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // It should not start a transaction
        assert!(matches!(result, CommandResult::Continue));
        assert!(!session.in_transaction(), "BEGIN with arguments should be ignored");
    }

    #[test]
    fn test_begin_prevents_nested_transactions() {
        let mut session = Session::new();

        // Start the first transaction
        handle_command("BEGIN", &vec![], "Usage", &mut session);
        assert!(session.in_transaction());

        // Try to start another one — should be ignored or error
        handle_command("BEGIN", &vec![], "Usage", &mut session);

        // Still only one transaction should exist
        assert!(session.in_transaction());
    }

    #[test]
    fn test_commit_with_active_transaction() {
        let mut session = Session::new();

        // Start a transaction and perform a write
        handle_command("BEGIN", &vec![], "Usage", &mut session);
        assert!(session.in_transaction());

        if let Some(tx) = &mut session.transaction {
            tx.set("color".into(), "blue".into());
        }

        // Commit the transaction
        let (cmd, args) = parse_command("COMMIT");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // Command should continue after commit
        assert!(matches!(result, CommandResult::Continue));

        // Verify that the transaction was cleared and the index updated
        assert!(!session.in_transaction(), "Transaction should clear after COMMIT");
        assert_eq!(session.index.search("color"), Some("blue"));
    }

    #[test]
    fn test_commit_without_active_transaction() {
        let mut session = Session::new();

        // Attempt to commit when none is active
        let (cmd, args) = parse_command("COMMIT");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // Command should not panic or exit
        assert!(matches!(result, CommandResult::Continue));

        // State should remain unchanged
        assert!(!session.in_transaction());
        assert!(session.index.search("color").is_none());
    }

    #[test]
    fn test_commit_rejects_arguments() {
        let mut session = Session::new();

        // Begin a transaction to ensure valid context
        handle_command("BEGIN", &vec![], "Usage", &mut session);
        assert!(session.in_transaction());

        // Attempt COMMIT with extra arguments
        let (cmd, args) = parse_command("COMMIT now");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // Command should still continue but reject input
        assert!(matches!(result, CommandResult::Continue));
        // Transaction should remain active because commit failed
        assert!(session.in_transaction());
    }

    #[test]
    fn test_abort_discards_active_transaction() {
        let mut session = Session::new();

        // Begin a transaction and add some data
        handle_command("BEGIN", &vec![], "Usage", &mut session);
        assert!(session.in_transaction());

        if let Some(tx) = &mut session.transaction {
            tx.set("temp".into(), "data".into());
            assert_eq!(tx.pending_count(), 1);
        }

        // Abort the transaction
        let (cmd, args) = parse_command("ABORT");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // Command should continue
        assert!(matches!(result, CommandResult::Continue));

        // Transaction should be cleared
        assert!(!session.in_transaction(), "Transaction should be cleared after ABORT");

        // Index should not have been modified
        assert!(session.index.search("temp").is_none());
    }

    #[test]
    fn test_abort_without_active_transaction() {
        let mut session = Session::new();

        // Ensure no active transaction
        assert!(!session.in_transaction());

        // Try to abort anyway
        let (cmd, args) = parse_command("ABORT");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        assert!(matches!(result, CommandResult::Continue));

        // State should remain unchanged
        assert!(!session.in_transaction());
        assert_eq!(session.index.search("ghost"), None);
    }

    #[test]
    fn test_abort_rejects_arguments() {
        let mut session = Session::new();

        // Begin a transaction for valid context
        handle_command("BEGIN", &vec![], "Usage", &mut session);
        assert!(session.in_transaction());

        // Try to abort with extra argument
        let (cmd, args) = parse_command("ABORT now");
        let result = handle_command(&cmd, &args, "Usage", &mut session);

        // Command continues but should not process abort
        assert!(matches!(result, CommandResult::Continue));
        assert!(session.in_transaction(), "Transaction should remain active when args are invalid");
    }

        #[test]
    fn test_expire_sets_ttl_on_existing_key() {
        let mut session = Session::new();

        // Create key first
        handle_command("SET", &vec!["dog".into(), "bark".into()], "Usage", &mut session);
        assert_eq!(session.ttl.active_count(), 0);

        // Apply EXPIRE command
        let (cmd, args) = parse_command("EXPIRE dog 200");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL entry should now exist
        assert_eq!(session.ttl.active_count(), 1);
        assert!(session.ttl.has_entry("dog"));
    }

    #[test]
    fn test_expire_rejects_missing_key() {
        let mut session = Session::new();

        // Try to expire a key that doesn’t exist
        let (cmd, args) = parse_command("EXPIRE ghost 1000");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL manager should still be empty
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_expire_rejects_non_numeric_value() {
        let mut session = Session::new();

        handle_command("SET", &vec!["temp".into(), "data".into()], "Usage", &mut session);

        let (cmd, args) = parse_command("EXPIRE temp abc");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL manager should not be modified
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_expire_rejects_zero_or_negative_duration() {
        let mut session = Session::new();

        handle_command("SET", &vec!["x".into(), "y".into()], "Usage", &mut session);

        // Zero duration
        let (cmd, args) = parse_command("EXPIRE x 0");
        handle_command(&cmd, &args, "Usage", &mut session);
        assert_eq!(session.ttl.active_count(), 0);

        // Negative duration
        let (cmd, args) = parse_command("EXPIRE x -100");
        handle_command(&cmd, &args, "Usage", &mut session);
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_expire_requires_two_arguments() {
        let mut session = Session::new();

        // Missing duration
        let (cmd, args) = parse_command("EXPIRE dog");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Too many arguments
        let (cmd, args) = parse_command("EXPIRE dog 1000 extra");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL manager remains empty in both cases
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_expire_key_expires_after_delay() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut session = Session::new();

        // Create key and set short TTL
        handle_command("SET", &vec!["temp".into(), "123".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["temp".into(), "50".into()], "Usage", &mut session);
        assert!(session.ttl.has_entry("temp"));

        // Wait until key should expire
        sleep(Duration::from_millis(60));

        // Verify TTL has expired (lazy cleanup)
        assert!(session.ttl.is_expired("temp"));
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_ttl_reports_positive_remaining_time() {
        let mut session = Session::new();

        // Create a key and set a TTL
        handle_command("SET", &vec!["dog".into(), "bark".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["dog".into(), "500".into()], "Usage", &mut session);

        // Query TTL
        let (cmd, args) = parse_command("TTL dog");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Remaining TTL should be positive
        let ttl_remaining = session.ttl.ttl_remaining("dog");
        assert!(ttl_remaining > 0, "TTL should be positive while active");
    }

    #[test]
    fn test_ttl_returns_minus_one_when_no_ttl_set() {
        let mut session = Session::new();

        // Key exists but no TTL
        handle_command("SET", &vec!["cat".into(), "meow".into()], "Usage", &mut session);

        let (cmd, args) = parse_command("TTL cat");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Should return -1 (no TTL)
        assert_eq!(session.ttl.ttl_remaining("cat"), -1);
    }

    #[test]
    fn test_ttl_returns_minus_two_for_missing_or_expired_key() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut session = Session::new();

        // Key doesn't exist
        let (cmd, args) = parse_command("TTL ghost");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // NOTE: handle_command prints -2 for missing keys (based on index check),
        // but ttl_remaining() itself only returns -1 because TTLManager doesn’t know key existence

        // TTL manager doesn’t track missing keys, so it should report -1 (no entry)
        assert_eq!(session.ttl.ttl_remaining("ghost"), -1);

        // Key that expires
        handle_command("SET", &vec!["temp".into(), "123".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["temp".into(), "50".into()], "Usage", &mut session);
        sleep(Duration::from_millis(60));

        // Lazy cleanup happens inside ttl_remaining()
        assert_eq!(session.ttl.ttl_remaining("temp"), -2);
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_ttl_rejects_incorrect_argument_counts() {
        let mut session = Session::new();

        // Too few args (none)
        let (cmd, args) = parse_command("TTL");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Too many args
        let (cmd, args) = parse_command("TTL dog extra");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));
    }

}
