// ============================================================
// File: main.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 19, 2025
//
// Description:
//   Entry point for the simple key-value store program.
//   This module implements the command-line interface (CLI)
//   that accepts the following commands:
//
//     SET <key> <value>   -> Store a key-value pair
//     GET <key>           -> Retrieve the value for a key
//     EXIT                -> Terminate the program
//
//   The CLI reads from standard input and writes responses
//   to standard output, which allows automated black-box
//   testing (Gradebot). In this initial version,
//   storage and indexing logic are simplified placeholders.
//
// Notes:
//   - Persistence and indexing are implemented in separate
//     modules (to be added later).
// ============================================================
mod storage;
use storage::{append_write, replay_log};
use std::io::{self, BufRead};


/// Entry point for the key-value store assignment.
fn main() {
    println!("Key Value Store");
    let proper_syntax = "Syntax Usage: GET <key>, SET <key> <value>, EXIT";
    println!("{}", proper_syntax);

    let stdin = io::stdin();

    // Form a loop to iterate over each input line; lock mutex
    for input_line in stdin.lock().lines() {
        // Unwrap the line and store on stack
        let full_command = input_line.unwrap();
        // Segment the command parts in a Vec[Str}] - handles whitespaces
        let mut command_segments = full_command.splitn(3, char::is_whitespace)
            .filter(|s| !s.is_empty());
        // Pulling out the command to nornmalize if lowercase is used
        let cmd = command_segments.next().unwrap_or("").to_uppercase();
        // Remaining arguments
        let args: Vec<&str> = command_segments.collect();

        // Handles each input command
        match cmd.as_str() {

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
            }

            // Set command format:  SET <key> <value>
            "SET" => {
                // Going larger than 2 for now
                if args.len() >= 2 {
                    // Piece the segments together again
                    let data_entry = format!("{} {} {}", cmd, args[0], args[1]);

                    // Try to write to file
                    if let Err(e) = append_write(&data_entry) {
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
            }

            // Exit command
            "EXIT" => {
                println!("Exiting...");
                break;
            }

            // Empty input
            "" => {
                println!("Enter a command.");
            }

            // Everything else will be noted and returned as an error
            _ => {

                // Unrecognized commands
                println!("ERROR: command '{}' not handled", cmd);
                println!("{}", proper_syntax);
            }
        }
    }
}
