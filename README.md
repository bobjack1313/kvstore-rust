# KVStore (Rust)

## Overview
This project is a simple key-value store written in **Rust**. This is part of a midterm/final 
project for CSCE 5350 Fundamentals of Database Systems at UNT.  

The database store accepts commands line input and prints rsults to standard output. It is designed to 
interact with a grading program (Gradebot) located at https://github.com/jh125486/CSCE5350_gradebot/releases  

The implementation follows an append-only storage model, with an in-memory index to enforce "last write wins" semantics. The long-term goal is to evolve this into a B+Tree-based system in later project phases.  

---

## Current Features (Phase 1 Completed)
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

---

## Planned Features
1. **Persistence (Phase 2)**  
   - On `SET`, append to an append-only log file (`data.db`)  
   - Flush to disk for crash durability  

2. **Log Replay on Startup**  
   - On program start, replay all records from `data.db` into an in-memory index  

3. **In-Memory Index**  
   - Begin with a simple vector-based index (linear scan, last-write-wins)  
   - Later replace with a B+Tree index for efficiency (Project 2)  

---

## Usage

### Build
```bash
cargo build
