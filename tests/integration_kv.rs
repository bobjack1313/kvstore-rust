// =====================================================================
// File: integration_kv.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
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


#[test]
fn test_delete_persists() {
    let file = "integration_delete.db";
    setup_file(file);

    // Write a SET and a DEL to the log
    append_write(file, "SET cat meow").unwrap();
    append_write(file, "DEL cat").unwrap();

    // Rebuild index from the log
    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        match parts[0] {
            "SET" if parts.len() == 3 => tree.insert(parts[1].into(), parts[2].into()),
            "DEL" if parts.len() == 2 => { tree.delete(parts[1]); },
            _ => {}
        }
    }

    // After replaying, the deleted key should no longer exist
    assert_eq!(tree.search("cat"), None);
}

#[test]
fn test_ttl_does_not_persist_across_restart() {
    let file = "integration_ttl_persist.db";
    setup_file(file);

    append_write(file, "SET temp 123").unwrap();
    append_write(file, "EXPIRE temp 5000").unwrap(); // Not persisted logically

    // Replay simulates restart
    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0].eq_ignore_ascii_case("SET") {
            tree.insert(parts[1].into(), parts[2].into());
        }
    }

    // TTLs vanish on restart, but value remains
    assert_eq!(tree.search("temp"), Some("123"));
}

#[test]
fn test_transaction_commit_persists() {
    let file = "integration_commit.db";
    setup_file(file);

    // Simulate a user session that begins, sets, commits
    append_write(file, "BEGIN").unwrap();
    append_write(file, "SET bird tweet").unwrap();
    append_write(file, "COMMIT").unwrap();

    // Rebuild index
    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["SET", key, val] => tree.insert((*key).into(), (*val).into()),
            ["DEL", key] => { tree.delete(key); },
            _ => {} // BEGIN/COMMIT lines safely ignored
        }
    }

    assert_eq!(tree.search("bird"), Some("tweet"));
}

#[test]
fn test_mset_replay_correctly_restores_last_values() {
    let file = "integration_mset.db";
    // Force delete any stale file
    std::fs::remove_file(file).ok();
    setup_file(file);

    append_write(file, "MSET a 1 b 2 c 3").unwrap();
    append_write(file, "MSET b 9 c 8").unwrap();

    let records = replay_log(file).unwrap();

    println!("==== FILE CONTENTS ====");
    println!("{}", std::fs::read_to_string(file).unwrap());
    println!("==== REPLAYED RECORDS ====");
    println!("{:?}", records);

    let mut tree = BTreeIndex::new(2);
    for line in records.iter() {
        println!("REPLAYING: {}", line);
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts[0].eq_ignore_ascii_case("MSET") {
            for chunk in parts[1..].chunks(2) {
                if chunk.len() == 2 {
                    println!(" -> inserting key bytes: {:?} = {}", chunk[0].as_bytes(), chunk[1]);
                    let key = chunk[0].trim().to_lowercase();
                    let value = chunk[1].trim().to_string();
                    tree.insert(key, value);
                }
            }
        }
    }
    // Used for debugging dups in inserts
    //tree.deduplicate();
    println!("=== BTree structure after replay ===");
    tree.debug_dump();

    assert_eq!(tree.search("a"), Some("1"));
    assert_eq!(tree.search("b"), Some("9"));
    assert_eq!(tree.search("c"), Some("8"));
}

#[test]
fn test_range_persists_ordered_keys() {
    let file = "integration_range.db";
    setup_file(file);

    append_write(file, "SET cat meow").unwrap();
    append_write(file, "SET ant tiny").unwrap();
    append_write(file, "SET dog bark").unwrap();

    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            tree.insert(parts[1].into(), parts[2].into());
        }
    }

    let mut keys = Vec::new();
    tree.collect_keys(&mut keys);
    assert_eq!(keys, vec!["ant", "cat", "dog"]);
}

#[test]
fn test_delete_then_set_sequence_persists_final_value() {
    let file = "integration_delset.db";
    setup_file(file);

    append_write(file, "SET frog ribbit").unwrap();
    append_write(file, "DEL frog").unwrap();
    append_write(file, "SET frog croak").unwrap();

    let records = replay_log(file).unwrap();
    let mut tree = BTreeIndex::new(2);
    for line in records {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["SET", key, val] => tree.insert((*key).into(), (*val).into()),
            ["DEL", key] => { tree.delete(key); },
            _ => {}
        }
    }

    // Final state: frog should exist, last value kept
    assert_eq!(tree.search("frog"), Some("croak"));
}


