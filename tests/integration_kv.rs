// =====================================================================
// File: integration_kv.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 23, 2025
//
// Description:
//   Integration tests for the key-value store. These tests exercise the
//   full end-to-end flow of the system, including:
//
//   - Appending commands to the persistent log (`data.db` or test files)
//   - Replaying the log to rebuild an in-memory B-tree index
//   - Verifying that SET/GET semantics, overwrites, and persistence
//     across restarts behave as expected
//   - Validating error handling for nonexistent keys and case-insensitive
//     command parsing
//
// Goal:
//   To confirm that the storage layer, indexing layer, and REPL command
//   handling work correctly together, simulating how the professor’s
//   Gradebot will interact with the program.
// =====================================================================
#[allow(unused_imports)]
use kvstore::{BTreeIndex, append_write, replay_log};

/// Helper - create a fresh in-memory tree and clean log file
fn setup() -> BTreeIndex {
    // Clean test log file
    let test_file = "integration_test.db";
    std::fs::write(test_file, "").unwrap();
    BTreeIndex::new(2)
}


/// Produces a clean file for integration tests.
fn setup_file(file: &str) {
    std::fs::write(file, "").unwrap();
}


#[test]
fn test_set_and_get_persisted() {
    let mut tree = setup();
    let file = "integration_test.db";

    // SET dog bark
    append_write(file, "SET dog bark").unwrap();
    tree.insert("dog".into(), "bark".into());

    // SET cat meow
    append_write(file, "SET cat meow").unwrap();
    tree.insert("cat".into(), "meow".into());

    // Search should succeed
    assert_eq!(tree.search("dog"), Some("bark"));
    assert_eq!(tree.search("cat"), Some("meow"));

    // Reload from log to simulate restart
    let records = replay_log(file).unwrap();
    let mut replay_tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            replay_tree.insert(parts[1].into(), parts[2].into());
        }
    }
    assert_eq!(replay_tree.search("dog"), Some("bark"));
    assert_eq!(replay_tree.search("cat"), Some("meow"));
}


#[test]
fn test_overwrite_persists() {
    let file = "integration_overwrite.db";
    setup_file(file);

    append_write(file, "SET dog bark").unwrap();
    append_write(file, "SET dog woof").unwrap();

    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            tree.insert(parts[1].into(), parts[2].into());
        }
    }

    assert_eq!(tree.search("dog"), Some("woof"));
}


#[test]
fn test_nonexistent_get() {
    let file = "integration_missing.db";
    setup_file(file);

    append_write(file, "SET cat meow").unwrap();

    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            tree.insert(parts[1].into(), parts[2].into());
        }
    }

    assert_eq!(tree.search("dog"), None); // key never set
}


#[test]
fn test_case_insensitive_commands() {
    let file = "integration_case.db";
    setup_file(file);

    // Mixed casing in commands, but we'll normalize to uppercase
    append_write(file, "set CAT meow").unwrap();
    append_write(file, "SeT gold fish").unwrap();
    append_write(file, "SET dog bark").unwrap();

    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 {
            // Normalize command and key to uppercase
            let command = parts[0].to_uppercase();
            let key = parts[1].to_uppercase();
            let value = parts[2].to_string();

            if command == "SET" {
                tree.insert(key, value);
            }
        }
    }

    // Search also uses uppercase since that's how keys are stored
    assert_eq!(tree.search("CAT"), Some("meow"));
    assert_eq!(tree.search("GOLD"), Some("fish"));
    assert_eq!(tree.search("DOG"), Some("bark"));
}
