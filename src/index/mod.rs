// =====================================================================
// File: index/mod.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Sept 22, 2025
//
//! The `index` module contains the B-tree implementation used
//! for in-memory indexing of the key-value store.
//!
//! Structure:
//! - `node.rs`  : Defines the [`BTreeNode`] structure and its helpers.
//! - `tree.rs`  : Defines the [`BTreeIndex`] and its algorithms
//!                (insert, search, delete).
//! - `tests.rs` : Unit tests for the B-tree (compiled only in test mode).
//!
//! This organization separates the small `BTreeNode` definition from
//! the larger `BTreeIndex` implementation for readability, while tests
//! are isolated to avoid cluttering the main code paths.
// =====================================================================

pub mod node;
pub mod tree;

pub use self::node::BTreeNode;
pub use self::tree::BTreeIndex;

#[cfg(test)]
pub mod tests;
