// ============================================================
// File: storage.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 19, 2025
//
// Description:
//   This module provides file persistence for the key-value store.
//   It implements append-only logging for durability, and replay
//   functionality to rebuild the in-memory index on startup.
//
// Goal:
// To fulfill the requirements from assignment regarding persistence
// 1) All writes must be persisted to disk immediately using
//    append-only writes to a file named data.db.
// 2) Data must remain consistent after restarting the program.
// 3) On startup, replay the log to rebuild the in-memory index.
// ============================================================
use std::fs::{OpenOptions, File};
use std::io::{self, Write, BufRead, BufReader};


/// File name from assignment requirements for persistent storage.
pub const DATA_FILE_NAME: &str = "data.db";

/// We need to append a line at the end of the file
pub fn append_write(input_data: &str) -> io::Result<()> {
    println!("append_write entered with: {}", input_data);

    // Access the data file, create if needed
    let mut data_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(DATA_FILE_NAME)?;

    // This will write the line and add a newline
    writeln!(data_file, "{}", input_data)?;
    // Flushing will write data - reduces data loss
    data_file.flush()?;
    // data_file.sync_all()); - Could be better

    Ok(())
}


/// Replay the log file and return a Vec of commands. Each line is assumed to be "SET key value"
pub fn replay_log() -> io::Result<Vec<String>> {
    println!("replay_log entered.");
    let mut data_records = Vec::new();

    // Verify the file is there and can be opened
    if let Ok(data_file_retrieved) = File::open(DATA_FILE_NAME) {
        let buf_reader = BufReader::new(data_file_retrieved);

        // Loop each line in file pushing to heap
        for line in buf_reader.lines() {
            // Check for valid entry
            if let Ok(data_entry) = line {
                data_records.push(data_entry);
            }
        }
    }
    // Complete - Remember returned values in Rust have no semicolon
    Ok(data_records)
}
