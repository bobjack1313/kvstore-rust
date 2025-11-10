// =====================================================================
// File: ttl/mod.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Nov. 9, 2025
//
//! The `ttl` module provides functionality for handling Time-to-Live (TTL)
//! metadata associated with keys in the key–value store.
//!
//! Structure:
//! - `manager.rs` : Defines the [`TTLManager`] structure and its methods
//!                  (`set_expiry`, `is_expired`, `ttl_remaining`, `clear_expiry`).
//! - `tests.rs`   : Unit tests for TTL behavior and command interactions.
//!
//! This organization separates TTL logic from the core index and persistence
//! layers to maintain modularity and simplify future extensions (e.g. persistence
//! of TTLs or background cleanup threads).
// =====================================================================

pub mod manager;

pub use self::manager::TTLManager;

#[cfg(test)]
pub mod tests;
