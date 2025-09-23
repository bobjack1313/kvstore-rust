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
// Midterm/Final Project Part 1
// Date: Sept 20, 2025
//
//   This module implements the command-line interface (CLI)
//   that accepts the following commands:
//
//     `SET <key> <value>`   -> Store a key-value pair
//     `GET <key>`           -> Retrieve the value for a key
//     `EXIT`                -> Terminate the program
// =====================================================================
mod storage;
use storage::{append_write, replay_log};
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


/// Loads data from file into active storage
pub fn load_data() {

    // Replay log before starting program
    if let Ok(data_records) = storage::replay_log(storage::DATA_FILE) {
        println!("Replayed {} records from {}", data_records.len(), storage::DATA_FILE);


        // TODO: feed records into your index here
    }
}


/// Read, evaluate, and print loop to handle command line instructions.
pub fn repl_loop() {
    let stdin = io::stdin();
    let proper_syntax = "Syntax Usage: GET <key>, SET <key> <value>, EXIT";

    // Form a loop to iterate over each input line; lock mutex
    for input_line in stdin.lock().lines() {
        // Unwrap because input_line is Result<String, std::io::Error>
        let full_command = input_line.unwrap();
        let (cmd, args) = parse_command(&full_command);

        // Process command and arguments
        match handle_command(&cmd, &args, proper_syntax) {
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
/// Supported commands:
/// - `GET <key>`: Attempts to retrieve a value by key (currently placeholder logic).
/// - `SET <key> <value>`: Stores a key-value pair and appends it to the log file.
/// - `EXIT`: Terminates the REPL loop.
/// - Any other input: Prints an error and redisplays the syntax.
///
/// Returns:
/// - `CommandResult::Continue` if the loop should keep running.
/// - `CommandResult::Exit` if the user requested termination.
///
/// The `proper_syntax` argument is displayed in error messages to guide the user.
fn handle_command(cmd: &str, args: &[String], proper_syntax: &str) -> CommandResult {
    // Watch - cmd is ref here
    match cmd.as_ref() {

        // Get command format:  GET <key>
        "GET" => {
            // Perform actions here

            if let Some(cmd_key) = args.get(0) {
                // Placeholder acknowledgement
                println!("Getting {}", cmd_key);
                println!("NULL");
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
                    // Placeholder acknowledgement
                    println!("Setting {} for {}", args[1], args[0]);
                    println!("OK");
                }

            } else {
                // Error for not enough arguments for SET
                println!("ERROR: SET requires a key and value");
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
        let result = handle_command(&cmd, &args, "Usage");
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

        let result = handle_command(&cmd, &args, "Usage");
        // Should not exit on bad command
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_get_missing_key() {
        let (cmd, args) = parse_command("GET");
        assert_eq!(cmd, "GET");
        assert!(args.is_empty());

        let result = handle_command(&cmd, &args, "Usage");
        assert!(matches!(result, CommandResult::Continue));
    }

    #[test]
    fn test_set_missing_value() {
        let (cmd, args) = parse_command("SET justonekey");
        assert_eq!(cmd, "SET");
        assert_eq!(args.len(), 1);

        let result = handle_command(&cmd, &args, "Usage");
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
}
