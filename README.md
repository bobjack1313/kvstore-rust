# KVStore (Rust)

## Overview
This project implements a persistent, append-only key–value store in Rust.  
It serves as the final implementation for **CSCE 5350 — Fundamentals of Database Systems** at the University of North Texas.

The store reads commands from standard input and is designed to operate under the official course Gradebot evaluator.  
Persistence is handled via an append-only log (`data.db`), while an in-memory **B-Tree index** is rebuilt on startup following a “last write wins” model.

Part 2 adds transaction logic, multi-key operations, TTL support, lazy expiration, and range queries.

---

## Features

### Core Commands
| Command | Description |
|--------|-------------|
| `SET <key> <value>` | Inserts or updates a key–value pair and appends it to the log. |
| `GET <key>` | Retrieves the value, applying TTL expiration if needed. |
| `DEL <key>` | Deletes a key and any associated TTL. |
| `EXISTS <key>` | Returns `1` if the key exists and is not expired, otherwise `0`. |
| `EXPIRE <key> <ms>` | Assigns a TTL in milliseconds to an existing key. |
| `TTL <key>` | Returns remaining TTL, `-1` for no TTL, or `-2` for missing/expired keys. |
| `MSET <k1> <v1> ...` | Writes multiple key–value pairs (each logged individually). |
| `MGET <k1> <k2> ...` | Retrieves multiple keys with TTL checks. |
| `RANGE <start> <end>` | Returns lexicographically ordered **single-character alphabetic keys** within the range. |

---

### Transactions
The store supports a single active transaction:

- `BEGIN` — Start a new transaction  
- `SET` / `DEL` — Applied to the transaction overlay  
- `COMMIT` — Flushes transaction changes to the B-Tree and persistent log  
- `ABORT` — Discards all staged changes  

Nested transactions are not supported.

---

### Persistence & Recovery
- All persistent operations use an **append-only log**.
- On startup:
  1. The data file is created if missing.  
  2. All `SET` commands are replayed into the B-Tree index.  
  3. “Last write wins” resolves multiple entries for the same key.  

TTL metadata is not persisted, per assignment rules.

---

### TTL Behavior
TTL management includes:

- Millisecond-precision expiration  
- Lazy cleanup on `GET`, `MGET`, `TTL`, and `RANGE`  
- Expired keys are removed from both TTL structures and the index  
- Behavior matches Gradebot expectations:  
  - Missing key → `-2`  
  - Expired key → `-2`  
  - Key with no TTL → `-1`  

---

### Range Queries
`RANGE <start> <end>` behavior:

- Only **alphabetic single-character** keys (`a`–`z`) are included  
- Multi-character keys (e.g., UUIDs) are ignored  
- TTL checks are applied before inclusion  
- Empty `""` for start or end expands the range  

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

### Gradebot Evaluation
Do not use cargo to run the file. Make sure you build the project first, then use `./target/debug/kvstore` to run.
