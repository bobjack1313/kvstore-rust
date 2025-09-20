// ============================================================
// File: main.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 19, 2025
//
// Description:
//   Entry point for the simple key-value store program.
//
//   The CLI reads from standard input and writes responses
//   to standard output, which allows automated black-box
//   testing (Gradebot). In this initial version,
//   storage and indexing logic are simplified placeholders.
// ============================================================


/// Entry point for the key-value store assignment.
fn main() {
    println!("Key Value Store");
    // Load data from file
    kvstore::load_data();
    // Hand off to the main command loop
    kvstore::repl_loop();
}
