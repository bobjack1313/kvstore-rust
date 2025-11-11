// =====================================================================
// File: ttl/manager.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Nov. 9, 2025
//
//! The [`TTLManager`] structure manages expiration times for keys in the
//! key–value store. It provides operations to set, check, and remove TTLs,
//! and calculates remaining lifespan on demand.
//!
//! Expiration is handled lazily — keys are only considered expired
//! at read time.
// =====================================================================

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Manages TTL metadata for keys in the key–value store.
///
/// This structure stores expiration timestamps for each key.
/// Expired entries are removed lazily when accessed.
#[derive(Debug, Default)]
pub struct TTLManager {
    expirations: HashMap<String, Instant>,
}


impl TTLManager {
    /// Create a new, empty TTL manager.
    pub fn new() -> Self {
        Self {
            expirations: HashMap::new(),
        }
    }


    /// Set an expiration time (milliseconds) for a given key.
    ///
    /// Associates the specified key with a future expiration timestamp,
    /// after which it is considered expired. The expiration time is stored
    /// internally as a monotonic `Instant` calculated from the current time.
    ///
    /// # Arguments
    /// * `key` - The key to apply the expiration to.
    /// * `time_ms` - Time-to-live duration in milliseconds.
    ///
    /// # Returns
    /// * `true` if the expiration was successfully set.
    /// * `false` if the provided duration was zero or negative (the key
    ///   is treated as immediately expired and any existing TTL is removed).
    ///
    /// # Notes
    /// - This operation does **not** remove the key’s value from the
    ///   main index; it only records an expiration timestamp.
    /// - Expiration is handled lazily. Key will be purged when
    ///   it is next accessed if the TTL has elapsed.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::TTLManager;
    /// let mut ttl = TTLManager::new();
    /// assert!(ttl.set_expiration("dog", 100));  // 100 ms TTL
    /// assert!(!ttl.set_expiration("cat", 0));   // Invalid TTL
    /// ```
    pub fn set_expiration(&mut self, key: &str, time_ms: i64) -> bool {
        // Reject negative durations and remove existing expirations
        if time_ms <= 0 {
            self.expirations.remove(key);
            return false;
        }

        // Compute exp timestamp using current time.
        let expiration_time = Instant::now() + Duration::from_millis(time_ms as u64);

        // Record/update the expiration entry
        self.expirations.insert(key.to_string(), expiration_time);

        // Indicate success
        true
    }


    /// Retrieve the remaining time-to-live (TTL) for a given key, in milliseconds.
    ///
    /// # Behavior
    /// * Returns the number of milliseconds remaining before expiration if the key has an active TTL.
    /// * Returns **-1** if the key exists but has no associated TTL.
    /// * Returns **-2** if the key’s TTL has expired or the key is not tracked.
    ///
    /// This function performs a *lazy expiration check* — expired keys are detected at
    /// read time and reported as `-2` but are not automatically removed.
    ///
    /// # Arguments
    /// * `key` – The key whose TTL should be queried.
    ///
    /// # Returns
    /// An `i64` integer:
    /// * `> 0` → remaining milliseconds
    /// * `-1` → key exists without TTL
    /// * `-2` → missing or expired key
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::manager::TTLManager;
    /// use std::thread::sleep;
    /// use std::time::Duration;
    ///
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("dog", 100);
    /// let remaining = ttl.get_expiration("dog");
    /// assert!(remaining > 0);
    ///
    /// sleep(Duration::from_millis(120));
    /// assert_eq!(ttl.get_expiration("dog"), -2);
    /// ```
    pub fn get_expiration(&self, key: &str) -> i64 {
        // Check for key
        if let Some(&expiration_time) = self.expirations.get(key) {
            let time_now = Instant::now();

            // Exit for expired key
            if time_now >= expiration_time {
                return -2;
            }
            // Return remaining
            let remaining = expiration_time.duration_since(time_now).as_millis() as i64;
            return remaining;
        }
        -1
    }


    /// Remove the expiration time associated with a key.
    ///
    /// Deletes any stored TTL metadata for the specified key, allowing
    /// it to persist indefinitely unless a new expiration is later applied.
    ///
    /// # Arguments
    /// * `key` - The key whose expiration entry should be removed.
    ///
    /// # Returns
    /// * `true` if an expiration entry existed and was successfully removed.
    /// * `false` if no expiration was set for the key.
    ///
    /// # Notes
    /// - This operation does not alter the key’s value in the main index.
    /// - The function only affects the in-memory expiration map.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::TTLManager;
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("dog", 500);
    /// assert!(ttl.clear_expiration("dog")); // TTL cleared
    /// assert!(!ttl.clear_expiration("cat")); // No TTL existed
    /// ```
    pub fn clear_expiration(&mut self, key: &str) -> bool {
        // Remove the key’s expiration entry from the map
        self.expirations.remove(key).is_some()
    }


    /// Determine whether a key’s expiration time has elapsed.
    ///
    /// Checks the current time with the stored expiration timestamp
    /// for the given key. An expred key will be removed from the
    /// the internal expiration map.
    ///
    /// # Arguments
    /// * `key` - The key to evaluate for expiration.
    ///
    /// # Returns
    /// * `true` if the key’s TTL has expired and its entry was removed.
    /// * `false` if the key either has not expired or has no TTL assigned.
    ///
    /// # Notes
    /// - This function performs *lazy expiration*: the key is not
    ///   automatically deleted from the main index until it is next read.
    /// - Expiration times are evaluated using a monotonic clock (`Instant`)
    ///   to ensure consistency regardless of system clock changes.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::TTLManager;
    /// use std::thread::sleep;
    /// use std::time::Duration;
    ///
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("session", 50);
    /// sleep(Duration::from_millis(60));
    /// assert!(ttl.is_expired("session"));
    /// ```
    pub fn is_expired(&mut self, key: &str) -> bool {
        // Check whether the key has an associated expiration timestamp.
        if let Some(&expiration_time) = self.expirations.get(key) {
            // If the current time exceeds the expiration timestamp, remove it
            if Instant::now() >= expiration_time {
                self.expirations.remove(key);
                return true;
            }
        }

        // Key is either untracked or still within its valid TTL window
        false
    }


    /// Retrieve the remaining time-to-live (TTL) for a key, in milliseconds.
    ///
    /// Calculates how much time remains before a key’s expiration.
    /// If the key has no TTL or is already expired, this function
    /// returns a negative value.
    ///
    /// # Arguments
    /// * `key` - The key whose TTL should be queried.
    ///
    /// # Returns
    /// * **Positive integer** — Remaining time (milliseconds) until expiration.
    /// * **-1** — The key exists but has no expiration set.
    /// * **-2** — The key is missing or its expiration has already passed.
    ///
    /// # Notes
    /// - Expired entries are removed from the internal map when encountered.
    /// - For reliability, monotonic time is used.
    /// - This function does not verify whether the key exists in the
    ///   main index; it only reports TTL metadata.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::TTLManager;
    /// use std::thread::sleep;
    /// use std::time::Duration;
    ///
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("temp", 100);
    /// assert!(ttl.ttl_remaining("temp") > 0); // Has TTL
    ///
    /// sleep(Duration::from_millis(120));
    /// assert_eq!(ttl.ttl_remaining("temp"), -2); // Expired
    /// ```
    pub fn ttl_remaining(&mut self, key: &str) -> i64 {
        // Attempt to retrieve the stored expiration timestamp for the key.
        if let Some(&expiration_time) = self.expirations.get(key) {
            let now = Instant::now();

            // If the expiration time has passed, clean up and return -2.
            if now >= expiration_time {
                self.expirations.remove(key);
                return -2;
            }

            // Compute the remaining duration in milliseconds.
            let remaining = expiration_time.duration_since(now).as_millis();
            remaining as i64
        } else {
            // Key has no TTL entry in the map.
            -1
        }
    }


    /// Return the total number of currently active TTL entries.
    ///
    /// Used primarily for diagnostics and testing.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::manager::TTLManager;
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("k1", 500);
    /// ttl.set_expiration("k2", 1000);
    /// assert_eq!(ttl.active_count(), 2);
    /// ```
    pub fn active_count(&self) -> usize {
        self.expirations.len()
    }


    /// Remove all tracked TTLs.
    ///
    /// Typically used during transaction aborts or full database resets.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::manager::TTLManager;
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("key", 1000);
    /// ttl.clear();
    /// assert_eq!(ttl.active_count(), 0);
    /// ```
    pub fn clear(&mut self) {
        self.expirations.clear();
    }


    /// Remove an active TTL for a given key, making it persistent again.
    ///
    /// # Returns
    /// * `true` if a TTL existed and was cleared.
    /// * `false` if no TTL was set for the key.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::manager::TTLManager;
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("k", 100);
    /// assert!(ttl.persist("k"));
    /// assert_eq!(ttl.get_expiration("k"), -1);
    /// ```
    pub fn persist(&mut self, key: &str) -> bool {
        self.expirations.remove(key).is_some()
    }


    /// Remove all expired keys from the TTL map.
    ///
    /// This performs lazy cleanup of any entries whose expiration time
    /// has already passed. Safe to call periodically.
    ///
    /// # Example
    /// ```
    /// use kvstore::ttl::manager::TTLManager;
    /// use std::thread::sleep;
    /// use std::time::Duration;
    ///
    /// let mut ttl = TTLManager::new();
    /// ttl.set_expiration("temp", 50);
    /// sleep(Duration::from_millis(60));
    /// ttl.cleanup_expired();
    /// assert_eq!(ttl.active_count(), 0);
    /// ```
    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();
        self.expirations.retain(|_, &mut exp| exp > now);
    }


    /// Returns `true` if a TTL entry currently exists for the given key.
    ///
    /// This does not trigger expiration checks; it simply reports
    /// whether the key is tracked in the internal expiration map.
    pub fn has_entry(&self, key: &str) -> bool {
        self.expirations.contains_key(key)
    }
}
