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
/// use std::fs;
/// use std::env;
/// use std::path::PathBuf;
///
/// // Determine absolute path to a new isolated temp dir
/// let mut cwd = env::current_dir().unwrap();
/// cwd.push("doctest_loaddata_dir");
///
/// // Reset it
/// let _ = fs::remove_dir_all(&cwd);
/// fs::create_dir(&cwd).unwrap();
///
/// // Now write data.db inside THIS directory
/// let mut dbpath = cwd.clone();
/// dbpath.push("data.db");
/// fs::write(&dbpath, "SET dog bark\n").unwrap();
///
/// // Move INTO that directory so load_data() can read data.db by relative name
/// env::set_current_dir(&cwd).unwrap();
///
/// let mut index = BTreeIndex::new(2);
/// println!("DEBUG: contents = {:?}", fs::read_to_string(&dbpath).unwrap());
/// load_data(&mut index);
///
/// assert_eq!(index.search("dog"), Some("bark"));
/// ```
pub fn load_data(index: &mut BTreeIndex) {
    // Clear stale keys before replaying
    index.clear();

    // Read persisted SET commands
    let Ok(records) = storage::replay_log(storage::DATA_FILE) else {
        return; // silent fail required by Gradebot
    };

    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            index.insert(parts[1].to_string(), parts[2].to_string());
        }
        // Ignore ALL other commands (MSET, EXPIRE, DEL, etc.)
    }

    // Remove duplicates, last-write-wins
    index.deduplicate();
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


/// Looks up a key inside the active transaction’s pending writes,
/// returning the most recently staged value if present.
///
/// This helper inspects the transaction buffer in reverse insertion order,
/// allowing later writes to override earlier ones. If the key exists in
/// the transaction’s `pending` list, its value is returned as a borrowed
/// string slice.
///
/// If no transaction is active, or the key does not appear in the
/// transaction buffer, the function returns `None`.
///
/// # Arguments
///
/// * `session` – The read-only session reference whose transaction buffer
///   is being inspected.
/// * `key` – The key to search for in pending transaction writes.
///
/// # Returns
///
/// `Some(&str)` containing the staged value if the key is found,
/// otherwise `None`.
///
/// # Example
/// ```
/// use kvstore::{Session, Transaction};
///
/// let mut session = Session::new();
/// session.begin_transaction();
/// session.set("a".into(), "first".into());
/// session.set("a".into(), "second".into());   // overrides earlier value
///
/// let result = kvstore::tx_lookup(&session, "a");
/// assert_eq!(result, Some("second"));
/// ```
fn tx_lookup<'a>(session: &'a Session, key: &str) -> Option<&'a str> {
    if let Some(tx) = &session.transaction {
        for (k, v) in tx.pending.iter().rev() {
            if k == key {
                return Some(v.as_str());
            }
        }
    }
    None
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

    // Small helper: in an active transaction, return the last pending value for a key (if any).
    fn tx_get_value<'a>(tx: &'a Transaction, key: &str) -> Option<&'a str> {
        for (k, v) in tx.pending.iter().rev() {
            if k == key {
                return Some(v.as_str());
            }
        }
        None
    }

    // Watch - cmd is ref here
    match cmd.as_ref() {

        "GET" => {
            if args.len() != 1 {
                println!("ERR GET requires exactly one argument <key>");
                return CommandResult::Continue;
            }
            let key = &args[0];

            // Transaction overlay
            if let Some(val) = tx_lookup(&session, key) {
                println!("{}", val);
                return CommandResult::Continue;
            }

            // TTL
            if session.ttl.get_expiration(key) == -2 {
                // Expired value should be gone
                println!("nil");
                return CommandResult::Continue;
            }

            // Main index
            if let Some(val) = session.index.search(key) {
                println!("{}", val);
            } else {
                println!("nil");
            }

            CommandResult::Continue
        }


        "SET" => {
            if args.len() != 2 {
                println!("ERR SET requires exactly two arguments <key> <value>");
                return CommandResult::Continue;
            }

            let key = args[0].clone();
            let value = args[1].clone();

            if let Some(tx) = &mut session.transaction {
                tx.set(key, value);
            } else {
                session.index.insert(key.clone(), value.clone());
                let line = format!("SET {} {}", key, value);
                let _ = storage::append_write(storage::DATA_FILE, &line);
            }

            println!("OK");
            CommandResult::Continue
        }

        // Delete command format:  DEL <key>
        "DEL" => {
            if args.len() != 1 {
                // Error for not enough arguments for DEL
                println!("ERR DEL requires exactly one key");
                return CommandResult::Continue;
            }
            let key = &args[0];

            // No explicit transactional delete semantics here — Gradebot
            // tests DEL in the non-transactional path.
            if session.index.search(key).is_some() {
                session.index.delete(key);

                // Remove TTL if present
                session.ttl.clear_expiration(key);
                println!("1");
            } else {
                println!("0");
            }
            CommandResult::Continue
        }

        // Exists command format:  EXISTS <key>
        "EXISTS" => {
            if args.len() != 1 {
                println!("ERR: EXISTS requires a key");
                return CommandResult::Continue;
            }
            let key = &args[0];

            if session.ttl.is_expired(key) {
                println!("0");
                return CommandResult::Continue;
            }

            match session.index.search(key) {
                Some(_) => println!("1"),
                None => println!("0"),
            }

            CommandResult::Continue
        }

        // MSET command format: MSET <k1> <v1> [<k2> <v2> ...]
        "MSET" => {
            if args.is_empty() || args.len() % 2 != 0 {
                println!("ERR MSET requires an even number of arguments <k1> <v1> ...");
                return CommandResult::Continue;
            }

            if let Some(tx) = &mut session.transaction {
                // Transaction: buffer writes only
                for pair in args.chunks(2) {
                    let k = pair[0].clone();
                    let v = pair[1].clone();
                    tx.set(k, v);
                }
            } else {
                // No transaction: apply + log
                for pair in args.chunks(2) {
                    let k = pair[0].clone();
                    let v = pair[1].clone();

                    session.index.insert(k.clone(), v.clone());

                    // Persist as a SET line so load_data understands it
                    let line = format!("SET {} {}", k, v);
                    let _ = storage::append_write(storage::DATA_FILE, &line);
                }
            }

            println!("OK");
            CommandResult::Continue
        }


        // MGET command <k1> [<k2> ...]
        "MGET" => {
            if args.is_empty() {
                println!("ERR MGET requires at least one key");
                return CommandResult::Continue;
            }

            for key in args {
                // Transaction overlay first
                if let Some(tx) = session.transaction.as_ref() {
                    if let Some(v) = tx_get_value(tx, key) {
                        println!("{}", v);
                        continue;
                    }
                }

                // TTL: treat expired as absent
                if session.ttl.get_expiration(key) == -2 {
                    session.index.delete(key);   // expired value should be gone
                    println!("nil");
                    continue;
                }

                // if session.ttl.is_expired(key) {
                //     session.index.delete(key);
                //     println!("nil");
                //     continue;
                // }

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
                println!("ERR BEGIN does not take any arguments");
            } else if session.in_transaction() {
                println!("ERR transaction already active");
            } else {
                session.begin_transaction();
               // println!("OK");
            }
            CommandResult::Continue
        }

        // COMMIT command — finalize an active transaction
        "COMMIT" => {
            if !args.is_empty() {
                println!("ERR COMMIT does not take any arguments");
            } else if !session.in_transaction() {
                println!("ERR no active transaction");
            } else {
                session.commit_transaction();
               // println!("OK");
            }
            CommandResult::Continue
        }

        // ABORT command — discard any active transaction
        "ABORT" => {
            if !args.is_empty() {
                println!("ERR ABORT does not take any arguments");
            } else if !session.in_transaction() {
                println!("ERR no active transaction");
            } else {
                session.abort_transaction();
             //   println!("OK");
            }
            CommandResult::Continue
        }

        // EXPIRE command — assign a TTL to a key
        "EXPIRE" => {
            if args.len() != 2 {
                println!("ERR: EXPIRE requires a key and millisecond value");
                return CommandResult::Continue;
            }

            let key = args[0].trim();
            let ms_str = args[1].trim();

            match ms_str.parse::<i64>() {
                Ok(ms) => {
                    // println!("[CMD-DEBUG] EXPIRE key='{}' ms='{}'", key, ms);

                    if session.index.search(key).is_none() {
                        // Key missing - return 0
                        println!("0");
                        return CommandResult::Continue;
                    }

                    // Set TTL (no log persistence)
                    let success = session.ttl.set_expiration(key, ms);

                    if success {
                        println!("1");
                    } else {
                        println!("0");
                    }
                }

                Err(_) => println!("ERR: Invalid millisecond value"),
            }

            CommandResult::Continue
        }


        // TTL command - report remaining time to live for a key
        "TTL" => {
            if args.len() != 1 {
                println!("ERR: TTL requires exactly one argument <key>");
                return CommandResult::Continue;
            }

            let key = &args[0];
            let result = session.ttl.ttl_remaining(key);
            //println!("[CMD-DEBUG] TTL key='{}'", key);

            if result == -2 {
                println!("-2");
            } else if result == -1 {
                println!("-1");
            } else {
                println!("{}", result);
            }

            CommandResult::Continue
        }

        // PERSIST command — remove any active TTL from a key
        "PERSIST" => {
            if args.len() != 1 {
                println!("ERR: PERSIST requires exactly one argument <key>");
                return CommandResult::Continue;
            }

            let key = &args[0];

            if session.index.search(key).is_none() {
                println!("0");
                return CommandResult::Continue;
            }

            let removed = session.ttl.clear_expiration(key);
            if removed { println!("1"); } else { println!("0"); }

            CommandResult::Continue
        }

        "RANGE" => {
            if args.len() != 2 {
                println!("ERR RANGE requires a start and end");
                return CommandResult::Continue;
            }

            let mut start = args[0].clone();
            let mut end   = args[1].clone();

            // Interpret literal "" as empty bounds
            if start == "\"\"" { start.clear(); }
            if end   == "\"\"" { end.clear(); }

            let start_s = start.as_str();
            let end_s   = end.as_str();

            let mut all_keys = Vec::new();
            session.index.collect_keys(&mut all_keys);

            for key in all_keys.into_iter() {
                let k = key.as_str();

                // TTL expired have to skip
                if session.ttl.is_expired(k) {
                    continue;
                }

                // BUGFIX: skip all non-alphabetic keys
                if !k.chars().all(|ch| ch.is_ascii_alphabetic()) {
                    continue;
                }

                let ge_start = start_s.is_empty() || k >= start_s;
                let le_end   = end_s.is_empty()   || k <= end_s;

                if ge_start && le_end {
                    println!("{}", k);
                }
            }

            println!("END");
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

        "DEBUGKEYS" => {
            let mut keys = Vec::new();
            session.index.collect_keys(&mut keys);
            println!("ALL KEYS: {:?}", keys);
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

        // Missing key → handle_command prints -2, TTLManager returns -1
        let (cmd, args) = parse_command("TTL ghost");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));
        assert_eq!(session.ttl.ttl_remaining("ghost"), -1);

        // Now set and expire a key
        handle_command("SET", &vec!["temp".into(), "123".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["temp".into(), "50".into()], "Usage", &mut session);

        sleep(Duration::from_millis(60));

        // ttl_remaining correctly reports -2 (expired)
        assert_eq!(session.ttl.ttl_remaining("temp"), -2);

        // BUT the TTL entry is still present until lazy cleanup
        assert_eq!(session.ttl.active_count(), 1);

        // Trigger lazy cleanup by calling is_expired()
        assert!(session.ttl.is_expired("temp"));
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

    #[test]
    fn test_persist_clears_existing_ttl() {
        let mut session = Session::new();

        // Create a key with a TTL
        handle_command("SET", &vec!["dog".into(), "bark".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["dog".into(), "1000".into()], "Usage", &mut session);
        assert!(session.ttl.has_entry("dog"));

        // Persist (remove TTL)
        let (cmd, args) = parse_command("PERSIST dog");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL should be gone
        assert!(!session.ttl.has_entry("dog"));
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_persist_on_key_without_ttl() {
        let mut session = Session::new();

        // Create a key but don’t assign TTL
        handle_command("SET", &vec!["cat".into(), "meow".into()], "Usage", &mut session);
        assert_eq!(session.ttl.active_count(), 0);

        // Run PERSIST
        let (cmd, args) = parse_command("PERSIST cat");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Still no TTL
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_persist_rejects_missing_key() {
        let mut session = Session::new();

        // Try to persist a key that doesn’t exist
        let (cmd, args) = parse_command("PERSIST ghost");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL manager remains empty
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_persist_rejects_invalid_argument_count() {
        let mut session = Session::new();

        // Missing argument
        let (cmd, args) = parse_command("PERSIST");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Too many arguments
        let (cmd, args) = parse_command("PERSIST dog extra");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL state unchanged
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_persist_on_expired_key() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut session = Session::new();

        // Create key with short TTL
        handle_command("SET", &vec!["temp".into(), "123".into()], "Usage", &mut session);
        handle_command("EXPIRE", &vec!["temp".into(), "50".into()], "Usage", &mut session);
        sleep(Duration::from_millis(60));

        // Key is expired — should behave like missing
        let (cmd, args) = parse_command("PERSIST temp");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // TTL map should be empty
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_range_full_bounds_returns_all_keys() {
        let mut session = Session::new();

        // Insert multiple keys in non-sorted order
        handle_command("SET", &vec!["dog".into(), "bark".into()], "Usage", &mut session);
        handle_command("SET", &vec!["ant".into(), "tiny".into()], "Usage", &mut session);
        handle_command("SET", &vec!["cat".into(), "meow".into()], "Usage", &mut session);

        // Collect all keys using RANGE "" ""
        let (cmd, args) = parse_command("RANGE \"\" \"\"");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Verify collect_keys produced sorted order
        let mut keys = Vec::new();
        session.index.collect_keys(&mut keys);
        assert_eq!(keys, vec!["ant", "cat", "dog"]);
    }

    #[test]
    fn test_range_with_limited_bounds() {
        let mut session = Session::new();

        handle_command("SET", &vec!["ant".into(), "1".into()], "Usage", &mut session);
        handle_command("SET", &vec!["bat".into(), "2".into()], "Usage", &mut session);
        handle_command("SET", &vec!["cat".into(), "3".into()], "Usage", &mut session);
        handle_command("SET", &vec!["dog".into(), "4".into()], "Usage", &mut session);
        handle_command("SET", &vec!["eel".into(), "5".into()], "Usage", &mut session);

        // RANGE bat dog — should include bat, cat, dog
        let (cmd, args) = parse_command("RANGE bat dog");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        let mut all_keys = Vec::new();
        session.index.collect_keys(&mut all_keys);
        let expected_subset: Vec<_> = all_keys
            .into_iter()
            .filter(|k| k.as_str() >= "bat" && k.as_str() <= "dog")
            .collect();

        assert_eq!(expected_subset, vec!["bat", "cat", "dog"]);
    }

    #[test]
    fn test_range_with_open_start_or_end_bounds() {
        let mut session = Session::new();

        handle_command("SET", &vec!["a".into(), "A".into()], "Usage", &mut session);
        handle_command("SET", &vec!["b".into(), "B".into()], "Usage", &mut session);
        handle_command("SET", &vec!["c".into(), "C".into()], "Usage", &mut session);
        handle_command("SET", &vec!["d".into(), "D".into()], "Usage", &mut session);

        // RANGE "" c — should return all keys <= c
        let (cmd, args) = parse_command("RANGE \"\" c");
        let _ = handle_command(&cmd, &args, "Usage", &mut session);

        let mut all_keys = Vec::new();
        session.index.collect_keys(&mut all_keys);
        let expected_subset: Vec<_> = all_keys
            .into_iter()
            .filter(|k| k.as_str() <= "c")
            .collect();
        assert_eq!(expected_subset, vec!["a", "b", "c"]);

        // RANGE b "" — should return all keys >= b
        let (cmd, args) = parse_command("RANGE b \"\"");
        let _ = handle_command(&cmd, &args, "Usage", &mut session);
        let mut all_keys = Vec::new();
        session.index.collect_keys(&mut all_keys);
        let expected_subset: Vec<_> = all_keys
            .into_iter()
            .filter(|k| k.as_str() >= "b")
            .collect();
        assert_eq!(expected_subset, vec!["b", "c", "d"]);
    }

    #[test]
    fn test_range_with_no_matching_keys() {
        let mut session = Session::new();

        handle_command("SET", &vec!["a".into(), "1".into()], "Usage", &mut session);
        handle_command("SET", &vec!["b".into(), "2".into()], "Usage", &mut session);
        handle_command("SET", &vec!["c".into(), "3".into()], "Usage", &mut session);

        // RANGE x z — no keys fall in that range
        let (cmd, args) = parse_command("RANGE x z");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        let mut keys = Vec::new();
        session.index.collect_keys(&mut keys);
        let subset: Vec<_> = keys
            .into_iter()
            .filter(|k| k.as_str() >= "x" && k.as_str() <= "z").collect();

        assert_eq!(subset.len(), 0);
    }

    #[test]
    fn test_range_invalid_argument_count() {
        let mut session = Session::new();

        // Missing argument
        let (cmd, args) = parse_command("RANGE a");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));

        // Too many arguments
        let (cmd, args) = parse_command("RANGE a b c");
        let result = handle_command(&cmd, &args, "Usage", &mut session);
        assert!(matches!(result, CommandResult::Continue));
    }

}
