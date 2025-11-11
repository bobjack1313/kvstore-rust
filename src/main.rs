// ============================================================
// File: main.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Sept 19, 2025 - Updated Nov. 9, 2025
//
// Description:
//   Entry point for the key–value store program.
//
//   This executable initializes the full in-memory session, including
//   the B-tree index, TTL manager, and optional transaction layer.
//   It then loads existing records from the append-only data file
//   before entering an interactive REPL loop.
//
//   The program communicates exclusively through standard input
//   and output to support automated black-box testing (Gradebot).
//
//   Data persistence is append-only, and all commands—SET, GET, DEL,
//   MSET, MGET, EXPIRE, TTL, RANGE, and transaction controls—are
//   processed via the session context for modular, testable behavior.
// =====================================================================
use std::fs::OpenOptions;
use kvstore::{load_data, repl_loop, Session};

/// Entry point for the key-value store assignment.
fn main() {

    // Initialize a new in-memory session (includes BTree index and TTL manager)
    let mut session = Session::new();

    // Ensure the data file exists for append-only persistence
    let _ = OpenOptions::new()
        .create(true)
        .append(true)
        .open("data.db");

    // Replay existing records into the in-memory index
    load_data(&mut session.index);

    // Hand off to the main REPL loop, which handles commands
    repl_loop(&mut session);
}
