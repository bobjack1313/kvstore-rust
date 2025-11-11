// =====================================================================
// File: transaction/mod.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Final Project Part 2
// Date: Nov. 10, 2025
//
// Description:
//   Module entry point for the transaction system. Re-exports both
//   the `Transaction` struct, which manages uncommitted key–value
//   writes, and the `Session` struct, which coordinates the
//   database’s runtime state (index, TTL, and active transaction).
//
// =====================================================================
pub mod transaction;

pub use self::transaction::Transaction;

#[cfg(test)]
mod tests;
