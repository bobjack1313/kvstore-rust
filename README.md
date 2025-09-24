# KVStore (Rust)

## Overview
This project is a simple key-value store written in **Rust**. This is part of a midterm/final 
project for CSCE 5350 Fundamentals of Database Systems at UNT.  

The database store accepts commands line input and prints rsults to standard output. It is designed to 
interact with a grading program (Gradebot) located at https://github.com/jh125486/CSCE5350_gradebot/releases  

The implementation follows an append-only storage model, with an in-memory index to enforce "last write wins" semantics. The long-term goal is to evolve this into a B+Tree-based system in later project phases.  

---

## Current Features (Part 1 Completed)
- **Command-line interface (CLI)** with support for:
  - `SET <key> <value>` - Store a key-value pair (acknowledges with `OK` and the entry)
  - `GET <key>` - Retrieve a value (currently placeholder, prints entered key and `NULL`)
  - `EXIT` - Terminate the program
- **Input parsing**:
  - Commands are normalized to uppercase (`set` / `Set` also work)
  - Keys and values remain case-sensitive
- **Error handling**:
  - Detects unrecognized commands
  - Reports missing arguments (`SET` or `GET` without enough args)
  - Prints a usage guide for incorrect inputs
- **Rust best practices**:
  - Modular design in progress (`main.rs`, with future `storage.rs` and `index.rs`)
  - Clear commenting and file headers
  - Case normalization limited to commands (keys/values preserved)

## Roadmap

Part 1 (Complete)
- REPL loop
- Persistent logging
- B-Tree insert/search/delete

Part 2 (Planned)
- Convert to B+ Tree (values only in leaves, linked leaf layer).
- Enhanced range queries (SCAN key1 key2).
- More advanced integration tests.

Future Improvements

Configurable degree t via command-line flag.
Benchmarking and performance tuning.
Optional crash recovery simulation.

## Requirements
- Rust (edition 2021 or later).  
  If not installed, visit [rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).

## Usage

### Build
```bash
cargo build
```

### Run
```bash
cargo run
```

### Test
```bash
cargo test
```
