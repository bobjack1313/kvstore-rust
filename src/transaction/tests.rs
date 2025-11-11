// =====================================================================
// File: transaction/tests.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Final Project Part 2
// Date: Nov. 10, 2025
//
// Description:
//   Unit tests for the transaction subsystem, covering creation,
//   BEGIN / COMMIT / ABORT operations, and the correct integration
//   of Transaction with the Session runtime context.
//
// =====================================================================



// =================================================================
// Unit tests for Transaction
// =================================================================
#[cfg(test)]
mod transaction_tests {

    use super::super::transaction::Transaction;
    use crate::{BTreeIndex, TTLManager};

    // -------------------------------------------------------------
    // Basic construction and initial state
    // -------------------------------------------------------------
    #[test]
    fn test_new_transaction_is_empty() {
        let tx = Transaction::new();
        assert!(tx.pending.is_empty(), "Transaction should start with an empty pending list");
        assert_eq!(tx.pending_count(), 0);
        assert!(tx.is_empty());
    }

    // -------------------------------------------------------------
    // Adding writes to the transaction
    // -------------------------------------------------------------
    #[test]
    fn test_set_adds_pending_write() {
        let mut tx = Transaction::new();
        tx.set("key1".into(), "value1".into());

        assert_eq!(tx.pending.len(), 1);
        assert_eq!(tx.pending[0].0, "key1");
        assert_eq!(tx.pending[0].1, "value1");
        assert_eq!(tx.pending_count(), 1);
        assert!(!tx.is_empty());
    }

    #[test]
    fn test_multiple_sets_accumulate() {
        let mut tx = Transaction::new();
        tx.set("k1".into(), "v1".into());
        tx.set("k2".into(), "v2".into());
        tx.set("k3".into(), "v3".into());

        assert_eq!(tx.pending_count(), 3);
        assert_eq!(tx.pending[2].1, "v3");
    }

    // -------------------------------------------------------------
    // Commit behavior
    // -------------------------------------------------------------
    #[test]
    fn test_commit_inserts_all_pending_writes() {
        let mut tx = Transaction::new();
        tx.set("dog".into(), "bark".into());
        tx.set("cat".into(), "meow".into());

        let mut index = BTreeIndex::new(2);
        tx.commit(&mut index);

        assert_eq!(index.search("dog"), Some("bark"));
        assert_eq!(index.search("cat"), Some("meow"));
        assert_eq!(tx.pending_count(), 0, "Pending list should clear after commit");
        assert!(tx.is_empty());
    }

    #[test]
    fn test_commit_overwrites_existing_keys() {
        let mut index = BTreeIndex::new(2);
        index.insert("color".into(), "red".into());

        let mut tx = Transaction::new();
        tx.set("color".into(), "blue".into());
        tx.commit(&mut index);

        assert_eq!(index.search("color"), Some("blue"));
        assert!(tx.is_empty());
    }

    // -------------------------------------------------------------
    // Clear behavior
    // -------------------------------------------------------------
    #[test]
    fn test_clear_discards_pending_changes() {
        let mut tx = Transaction::new();
        tx.set("x".into(), "y".into());
        tx.set("foo".into(), "bar".into());
        assert_eq!(tx.pending_count(), 2);

        tx.clear();

        assert_eq!(tx.pending_count(), 0);
        assert!(tx.is_empty());
    }

    #[test]
    fn test_clear_does_not_affect_index() {
        let mut index = BTreeIndex::new(2);
        index.insert("keep".into(), "true".into());

        let mut tx = Transaction::new();
        tx.set("drop".into(), "temp".into());
        tx.clear();

        // Index should remain unaffected by transaction clear
        assert_eq!(index.search("keep"), Some("true"));
        assert_eq!(index.search("drop"), None);
    }

    // -------------------------------------------------------------
    // TTL Manager integration (basic presence check)
    // -------------------------------------------------------------
    #[test]
    fn test_transaction_has_ttl_manager() {
        let tx = Transaction::new();
        assert_eq!(tx.ttl_manager.active_count(), 0);
    }
}
