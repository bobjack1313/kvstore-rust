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
pub const DATA_FILE: &str = "data.db";


/// We need to append a line at the end of the file
pub fn append_write(filename: &str, input_data: &str) -> io::Result<()> {
    println!("append_write entered with: {}", input_data);

    // Access the data file, create if needed
    let mut data_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    // This will write the line and add a newline
    writeln!(data_file, "{}", input_data)?;
    // Flushing will write data - reduces data loss
    //data_file.flush()?;
    data_file.sync_all()?;

    Ok(())
}


pub fn replay_log(filename: &str) -> io::Result<Vec<String>> {
    let mut data_records = Vec::new();

    if let Ok(data_file_retrieved) = File::open(filename) {
        let buf_reader = BufReader::new(data_file_retrieved);

        for line in buf_reader.lines() {
            if let Ok(data_entry) = line {
                let trimmed = data_entry.trim();
                if !trimmed.is_empty() {
                    data_records.push(trimmed.to_string());
                }
            }
        }
    }
    Ok(data_records)
}


// =================================================================
// storage.rs Unit tests
// =================================================================
#[cfg(test)]
mod storage_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    // Tests are run in parallel, so using a single test file is bad
    fn test_file(name: &str) -> String {
        let mut p: PathBuf = std::env::temp_dir();
        // Unique filename per test
        p.push(format!("kvstore_{}.db", name));
        p.to_string_lossy().into_owned()
    }

    // Helper for resetting file for tests. Run before to make sure file
    // doesnt exist and after to del the file from dir
    fn clean(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_append_and_replay_single_entry() {
        let file = test_file("append_single");
        clean(&file);

        append_write(&file, "SET kennel tickle").unwrap();
        let records = replay_log(&file).unwrap();
        assert_eq!(records, vec!["SET kennel tickle"]);

        clean(&file);
    }

    #[test]
    fn test_append_and_replay_multiple_entries() {
        let file = test_file("append_multiple");
        clean(&file);

        append_write(&file, "SET a 1").unwrap();
        append_write(&file, "SET b 2").unwrap();
        append_write(&file, "SET c 3").unwrap();
        let records = replay_log(&file).unwrap();
        assert_eq!(records, vec!["SET a 1", "SET b 2", "SET c 3"]);

        clean(&file);
    }

    #[test]
    fn test_replay_empty_file() {
        let file = test_file("empty");
        clean(&file);

        let records = replay_log(&file).unwrap();
        assert!(records.is_empty());

        clean(&file);
    }

    #[test]
    fn test_append_persists_between_calls() {
        let file = test_file("persist");
        clean(&file);

        append_write(&file, "SET animal crotch").unwrap();
        append_write(&file, "SET 412 zootsuit").unwrap();

        // Simulate restart: replay log
        let records = replay_log(&file).unwrap();
        assert_eq!(records, vec!["SET animal crotch", "SET 412 zootsuit"]);

        append_write(&file, "SET cookie monster").unwrap();
        append_write(&file, "SET bath 44556633").unwrap();

        // Check for additions
        let records = replay_log(&file).unwrap();
        assert_eq!(records, vec!["SET animal crotch", "SET 412 zootsuit",
            "SET cookie monster", "SET bath 44556633" ]);

        clean(&file);
    }

    #[test]
    fn test_replay_with_trailing_newlines() {
        let file = test_file("trailing_newline");
        clean(&file);

        // Write a file manually with extra newlines
        fs::write(&file, "SET one 1\nSET two 2\n\n").unwrap();

        let records = replay_log(&file).unwrap();
        assert_eq!(records, vec!["SET one 1", "SET two 2"]);

        clean(&file);
    }
}
