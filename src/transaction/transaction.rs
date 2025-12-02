// =====================================================================
// File: transaction/transaction.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Final Project Part 2
// Date: Nov. 10, 2025
//
// Description:
//   Implements the Transaction struct, which represents a single
//   in-progress transaction. Each transaction maintains a list of
//   pending writes (key–value pairs) that have not yet been committed
//   to disk or applied to the in-memory index.
//
//   Used by the Session layer to provide atomic BEGIN / COMMIT /
//   ABORT behavior.
//
// =====================================================================
use crate::{BTreeIndex, TTLManager};
use crate::storage;

/// Represents a single active transaction session.
/// Holds all pending writes and their temporary TTL metadata.
pub struct Transaction {
    /// List of uncommitted key-value pairs (write buffer).
    pub pending: Vec<(String, String)>,

    /// Per-transaction TTL manager (for temporary expirations).
    pub ttl_manager: TTLManager,
}


impl Transaction {
    /// Creates a new, empty transaction session.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            ttl_manager: TTLManager::new(),
        }
    }

    /// Adds a pending write to the transaction buffer.
    ///
    /// # Example
    /// ```
    /// use kvstore::Transaction;
    /// let mut tx = Transaction::new();
    /// tx.set("user1".into(), "active".into());
    /// assert_eq!(tx.pending.len(), 1);
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.pending.push((key, value));
    }


    /// Commits all pending writes into the main BTree index.
    ///
    /// Writes are applied in insertion order, and also appended to
    /// the persistent log as plain SET commands so they survive
    /// process restarts.
    pub fn commit(&mut self, index: &mut BTreeIndex) {
        for (k, v) in &self.pending {
            // Apply to in-memory index
            index.insert(k.clone(), v.clone());

            // Also append to disk log as a SET command
            let line = format!("SET {} {}", k, v);
            let _ = storage::append_write(&storage::get_data_file(), &line);
        }

        // Clear transaction buffers
        self.pending.clear();
        self.ttl_manager.clear();
    }


    /// Clears all pending writes (used for ABORT logic).
    ///
    /// This discards the transaction’s current state without
    /// affecting the global index or TTL manager.
    pub fn clear(&mut self) {
        self.pending.clear();
        self.ttl_manager.clear();
    }

    /// Returns the number of pending writes in the buffer.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns `true` if the transaction currently has no changes.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

}
