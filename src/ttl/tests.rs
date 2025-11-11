// =====================================================================
// File: ttl/tests.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Nov. 9, 2025
//
// Description:
//   Unit tests TTL Manager
//
// Notes:
//   * Only compiled when running `cargo test`.
//   * Does not affect release builds.
// =====================================================================


// =====================================================================
// TTL Manager Unit Tests
// =====================================================================
#[cfg(test)]
mod ttl_manager_tests {
    use crate::TTLManager;
    use std::thread::sleep;
    use std::time::Duration;

    fn make_manager() -> TTLManager {
        TTLManager::new()
    }

    #[test]
    fn set_and_get_expiration_basic() {
        let mut ttl = make_manager();
        assert!(ttl.set_expiration("dog", 200));
        let remaining = ttl.get_expiration("dog");
        assert!(remaining > 0 && remaining <= 200);
    }

    #[test]
    fn zero_or_invalid_ttl_returns_false() {
        let mut ttl = make_manager();
        assert!(!ttl.set_expiration("cat", 0));
        assert_eq!(ttl.get_expiration("cat"), -1);
    }

    #[test]
    fn key_expires_after_delay() {
        let mut ttl = make_manager();
        ttl.set_expiration("bird", 100);
        sleep(Duration::from_millis(120));
        assert!(ttl.is_expired("bird"));
    }

    #[test]
    fn get_expiration_returns_negative_for_expired() {
        let mut ttl = make_manager();
        ttl.set_expiration("fish", 80);
        sleep(Duration::from_millis(100));
        assert_eq!(ttl.get_expiration("fish"), -2);
    }

    #[test]
    fn persist_removes_existing_ttl() {
        let mut ttl = make_manager();
        ttl.set_expiration("frog", 300);
        assert!(ttl.persist("frog"));
        assert_eq!(ttl.get_expiration("frog"), -1);
    }

    #[test]
    fn persist_on_missing_key_returns_false() {
        let mut ttl = make_manager();
        assert!(!ttl.persist("nope"));
    }

    #[test]
    fn cleanup_expired_removes_stale_keys() {
        let mut ttl = make_manager();
        ttl.set_expiration("x", 50);
        ttl.set_expiration("y", 200);
        sleep(Duration::from_millis(80));
        ttl.cleanup_expired();
        assert_eq!(ttl.active_count(), 1);
        assert!(ttl.get_expiration("y") > 0);
    }

    #[test]
    fn multiple_keys_independent_expiry() {
        let mut ttl = make_manager();
        ttl.set_expiration("a", 100);
        ttl.set_expiration("b", 300);
        sleep(Duration::from_millis(150));
        assert!(ttl.is_expired("a"));
        assert!(!ttl.is_expired("b"));
        assert_eq!(ttl.active_count(), 1);
    }

    #[test]
    fn clear_resets_all_state() {
        let mut ttl = make_manager();
        ttl.set_expiration("key1", 500);
        ttl.set_expiration("key2", 500);
        ttl.clear();
        assert_eq!(ttl.active_count(), 0);
    }

    #[test]
    fn ttl_map_can_handle_many_entries() {
        let mut ttl = make_manager();
        for i in 0..1000 {
            ttl.set_expiration(&format!("k{i}"), 5000);
        }
        assert_eq!(ttl.active_count(), 1000);
    }

    #[test]
    fn expired_key_is_removed_on_check() {
        let mut ttl = make_manager();
        ttl.set_expiration("zebra", 50);
        sleep(Duration::from_millis(100));
        assert!(ttl.is_expired("zebra"));
        assert_eq!(ttl.active_count(), 0);
    }

    #[test]
    fn get_expiration_returns_minus_one_for_no_ttl() {
        let ttl = make_manager();
        assert_eq!(ttl.get_expiration("none"), -1);
    }
}
