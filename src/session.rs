// =====================================================================
// File: session.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Project Part 2
// Date: Nov. 10, 2025
//
// Description:
// Defines the `Session` struct, which represents a running database
// context containing the active in-memory index, TTL manager, and
// optional transaction session.
//
// Responsibilities:
// - Maintain a single runtime context for command execution.
// - Contain references to the BTreeIndex (key-value store).
// - Manage TTL expiration logic through the TTLManager.
// - Optionally track an in-progress transaction for atomic operations.
//
// Each client session corresponds to a single REPL or Gradebot run,
// ensuring isolated transaction and TTL states.
// =====================================================================

use crate::{BTreeIndex, TTLManager, Transaction};

/// Represents a single in-memory database session.
/// Holds the live index, TTL manager, and optional transaction state.
pub struct Session {
    /// The main persistent in-memory key-value index (B-tree).
    pub index: BTreeIndex,

    /// Global TTL manager handling key expirations.
    pub ttl: TTLManager,

    /// Optional active transaction session (`None` if not in BEGIN/COMMIT mode).
    pub transaction: Option<Transaction>,
}


impl Session {
    /// Creates a new, empty session with its own index and TTL manager.
    ///
    /// # Example
    /// ```
    /// use kvstore::Session;
    /// let mut session = Session::new();
    /// assert!(session.transaction.is_none());
    /// ```
    pub fn new() -> Self {
        Self {
            index: BTreeIndex::new(2),
            ttl: TTLManager::new(),
            transaction: None,
        }
    }

    /// Returns `true` if a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Starts a new transaction, overwriting any previous uncommitted session.
    pub fn begin_transaction(&mut self) {
        self.transaction = Some(Transaction::new());
    }

    /// Commits an active transaction into the main index and clears it.
    pub fn commit_transaction(&mut self) {
        if let Some(tx) = &mut self.transaction {
            tx.commit(&mut self.index);
        }
        self.transaction = None;
    }

    /// Aborts (clears) an active transaction, discarding pending changes.
    pub fn abort_transaction(&mut self) {
        if let Some(tx) = &mut self.transaction {
            tx.clear();
        }
        self.transaction = None;
    }
}


// =====================================================================
// Unit Tests for Session
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Basic Session Creation
    #[test]
    fn test_new_session_initial_state() {
        let session = Session::new();

        // Session should start empty and without a transaction
        assert!(session.index.search("nothing").is_none());
        assert!(session.transaction.is_none());
        assert_eq!(session.ttl.active_count(), 0);
    }


    // Transaction Lifecycle
    #[test]
    fn test_begin_transaction_creates_new_tx() {
        let mut session = Session::new();
        session.begin_transaction();
        assert!(session.transaction.is_some());
        assert!(session.in_transaction());
    }

    #[test]
    fn test_commit_transaction_writes_to_index() {
        let mut session = Session::new();
        session.begin_transaction();

        // Add something to the pending transaction
        if let Some(tx) = &mut session.transaction {
            tx.set("color".into(), "blue".into());
        }

        // Commit and confirm index update
        session.commit_transaction();
        assert_eq!(session.index.search("color"), Some("blue"));
        assert!(session.transaction.is_none(), "Transaction should clear after commit");
    }

    #[test]
    fn test_abort_transaction_discards_changes() {
        let mut session = Session::new();
        session.begin_transaction();

        // Add temporary data
        if let Some(tx) = &mut session.transaction {
            tx.set("temp".into(), "data".into());
            assert_eq!(tx.pending_count(), 1);
        }

        // Abort discards it
        session.abort_transaction();

        assert!(session.transaction.is_none());
        assert!(session.index.search("temp").is_none(), "Index should not be modified");
    }

    // TTL Manager Integration
    #[test]
    fn test_set_and_clear_ttl_in_session() {
        let mut session = Session::new();
        session.ttl.set_expiration("dog", 1000);
        assert_eq!(session.ttl.active_count(), 1);

        session.ttl.clear_expiration("dog");
        assert_eq!(session.ttl.active_count(), 0);
    }

    #[test]
    fn test_expired_key_removal_lifecycle() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut session = Session::new();
        session.ttl.set_expiration("temp", 50);
        assert_eq!(session.ttl.active_count(), 1);

        // Wait for TTL to expire
        sleep(Duration::from_millis(60));
        assert!(session.ttl.is_expired("temp"));
        assert_eq!(session.ttl.active_count(), 0);
    }

    // Nested Transactions and TTL interaction
    #[test]
    fn test_transaction_with_ttl_manager_present() {
        let mut session = Session::new();
        session.begin_transaction();

        assert!(session.transaction.is_some());
        if let Some(tx) = &mut session.transaction {
            tx.ttl_manager.set_expiration("temp_tx", 500);
            assert_eq!(tx.ttl_manager.active_count(), 1);
        }

        session.abort_transaction();
        assert!(session.transaction.is_none());
    }

    // Multi-transaction overwrite behavior
    #[test]
    fn test_multiple_transactions_replace_previous() {
        let mut session = Session::new();
        session.begin_transaction();

        // Create a transaction and set data
        if let Some(tx) = &mut session.transaction {
            tx.set("fruit".into(), "apple".into());
        }

        // Begin again — should overwrite the old transaction
        session.begin_transaction();
        assert!(session.transaction.is_some());
        if let Some(tx) = &mut session.transaction {
            assert!(tx.is_empty(), "New transaction should not carry over old data");
        }
    }
}
