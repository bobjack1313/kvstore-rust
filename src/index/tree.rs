// =====================================================================
// File: index/tree.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 21, 2025 - Refactored Sept. 22, 2025
//
// Description:
//   Implements the B-tree index (`BTreeIndex`) that manages insertion,
//   search, and deletion operations over `BTreeNode` structures. This
//   index serves as the in-memory data structure backing the key-value
//   store, ensuring efficient lookups and ordered key management.
//
// Features:
//   - `insert`: Adds or overwrites key–value pairs (last write wins).
//   - `search`: Standard B-tree search; returns the value for a key.
//   - `delete`: Removes keys while preserving B-tree invariants.
//   - Split/merge helpers: Maintain balance during inserts and deletes.
//
// Notes:
//   * Relies on `node.rs` for the `BTreeNode` definition.
//   * The minimum degree `t` determines the branching factor and the
//     number of keys per node.
//   * Internal helpers (`insert_internal`, `delete_internal`, etc.)
//     implement the recursive B-tree algorithms.
// =====================================================================
use super::BTreeNode;

/// BTree Index, interfaces with lib to index the db with the nodes and leafs.
/// Contains the branching factor (t) and root node.
#[derive(Debug)]
pub struct BTreeIndex {
    pub t: usize,
    pub root: Box<BTreeNode>,
}


// BTree Referencing:
// https://build-your-own.org/database/
// https://www.geeksforgeeks.org/dsa/introduction-of-b-tree-2/
impl BTreeIndex {
    /// Create a new empty B-tree with minimum degree t greather than 2.
    pub fn new(t: usize) -> Self {
        assert!(t >= 2, "B-tree minimum degree t must be >= 2");
        Self {
            t,
            root: Box::new(BTreeNode::new(true)),
        }
    }

    /// Search for a key in the B-tree.
    ///
    /// Traverses the tree from the root, descending into child nodes as needed,
    /// to locate the target key.
    ///
    /// # Arguments
    /// * `key` - The key to search for.
    ///
    /// # Returns
    /// * `Some(&str)` containing a reference to the associated value if the key exists.
    /// * `None` if the key is not found in the tree.
    ///
    /// # Notes
    /// - Keys are compared in sorted order using `lower_bound`.
    /// - Search runs in **O(log n)** time due to B-tree height guarantees.
    ///
    /// # Example
    /// ```
    /// use kvstore::BTreeIndex;
    /// let mut t = BTreeIndex::new(2);
    /// t.insert("dog".into(), "bark".into());
    /// assert_eq!(t.search("dog"), Some("bark"));
    /// assert_eq!(t.search("cat"), None);
    /// ```
    pub fn search(&self, key: &str) -> Option<&str> {

        // Recursive function declaration for node search
        fn search_node<'a>(node: &'a BTreeNode, key: &str) -> Option<&'a str> {
            // Find the position in this node where the key would belong
            let idx = node.lower_bound(key);

            // Base Case - Successfully found the key in the current node
            if idx < node.kv_pairs.len() && node.kv_pairs[idx].0 == key {
                return Some(node.kv_pairs[idx].1.as_str());
            }

            // No key here, base case fails - search ends
            if node.is_leaf {
                None

            // No key here, there are children, so recursive search
            } else {
                search_node(&node.children[idx], key)
            }
        }
        // Call search
        search_node(&self.root, key)
    }

    /// Insert a key-value pair into the B-tree.
    ///
    /// - If the key already exists anywhere in the tree, its value is updated
    ///   (last write wins).
    /// - If the key does not exist, it is inserted at the correct position to
    ///   hold sorted order and balance the B-tree.
    /// - If the root node is full, the tree grows in height by splitting the root.
    ///
    /// # Arguments
    /// * `key`   - The key to insert.
    /// * `value` - The value to associate with the key.
    ///
    /// # Notes
    /// This is the primary public interface for modifying the B-tree.
    /// Internally, it calls [`insert_inside`] and may trigger [`split_child`].
    ///
    /// # Example
    /// ```
    /// use kvstore::BTreeIndex;
    ///
    /// // Create a B-tree with minimum degree 2
    /// let mut index = BTreeIndex::new(2);
    ///
    /// // Insert key–value pairs
    /// index.insert("dog".into(), "bark".into());
    /// index.insert("cat".into(), "meow".into());
    ///
    /// // Verify values can be retrieved
    /// assert_eq!(index.search("dog"), Some("bark"));
    /// assert_eq!(index.search("cat"), Some("meow"));
    ///
    /// // Overwrite existing key
    /// index.insert("dog".into(), "woof".into());
    /// assert_eq!(index.search("dog"), Some("woof"));
    /// ```
    pub fn insert(&mut self, key: String, value: String) {
        let t = self.t;

        if self.root.kv_pairs.len() == 2 * t - 1 {
            // Create a new root and hang the old root under it
            let mut new_root = Box::new(BTreeNode::new(false));
            new_root.children.push(std::mem::replace(
                &mut self.root,
                Box::new(BTreeNode::new(true)),
            ));

            // Split old root (now child 0 of new_root)
            Self::split_child(&mut new_root, t, 0);

            // Choose which child to descend into
            let idx = if key > new_root.kv_pairs[0].0 { 1 } else { 0 };
            Self::insert_internal(&mut new_root.children[idx], t, key, value);

            // Replace the tree's root
            self.root = new_root;
        } else {
            // Root not full — normal descent - Assiociative func call
            Self::insert_internal(&mut self.root, t, key, value);
        }
    }


    /// Deletes a key and value) from the B-tree if present.
    ///
    /// This follows the standard B-tree deletion algorithm:
    /// - If the key is in a leaf node, it is removed directly.
    /// - If the key is in an internal node:
    ///   - Replace it with its predecessor or successor key, then delete recursively.
    ///   - If necessary, borrow from a sibling or merge children to maintain B-tree properties.
    ///
    /// # Arguments
    /// * `key` - The key to be deleted, as a string slice.
    ///
    /// # Behavior
    /// - Maintains the B-tree invariants after deletion.
    /// - If the key does not exist, the tree is unchanged.
    ///
    /// # Example
    /// ```
    /// use kvstore::index::BTreeIndex;
    /// let mut index = BTreeIndex::new(2);
    /// index.insert("dog".into(), "bark".into());
    /// index.delete("dog");
    /// assert_eq!(index.search("dog"), None);
    /// ```
    pub fn delete(&mut self, key: &str) {
        let t = self.t;

        // Call inside delete - recurse - Use associative call - less borrow headaches
        Self::delete_internal(&mut self.root, t, key);

        // If the root became empty and is internal - shrink height
        if !self.root.is_leaf && self.root.kv_pairs.is_empty() {
            self.root = self.root.children.remove(0);
        }
    }


    // =========================
    // Insertion helpers
    // =========================

    /// Inserts a key-value pair into the subtree rooted at `node`.
    ///
    /// This function handles both the base case (insertion into a leaf node)
    /// and the recursive case (descent into an internal node). If the key
    /// already exists at the current level, its value is updated to satisfy
    /// the "last write wins" requirement.
    ///
    /// # Arguments
    /// * `node`  - Mutable reference to the current subtree root.
    /// * `key`   - The key to insert (String).
    /// * `value` - The value to associate with the key (String).
    ///
    /// # Behavior
    /// - **Leaf node**:
    ///   - If the key exists, overwrite its value.
    ///   - Otherwise, insert `(key, value)` at the correct sorted position.
    /// - **Internal node**:
    ///   - If the key exists, overwrite its value.
    ///   - Otherwise, split a full child before descending to valid space,
    ///     then recurse into the correct child.
    ///
    /// # Notes
    /// - Uses `lower_bound` to maintain sorted order of keys.
    /// - Checks that no child is full before recursion.
    /// - Does not return a value; modifies the tree in place.
    ///
    /// # Call outs
    /// Will call out if there is a violation like attempting to split a
    /// non-full child. Should not happend if properly working.
    fn insert_internal(node: &mut BTreeNode, t: usize, key: String, value: String) {
        let mut idx = node.lower_bound(&key);

        // Base case - leaf insert
        if node.is_leaf {
            // Overwrite if key exists (last write wins)
            if idx < node.kv_pairs.len() && node.kv_pairs[idx].0 == key {
                node.kv_pairs[idx].1 = value;
            } else {
                node.kv_pairs.insert(idx, (key, value));
            }
            return;
        }
        // If key exists in internal node - overwrite value and stop
        if idx < node.kv_pairs.len() && node.kv_pairs[idx].0 == key {
            node.kv_pairs[idx].1 = value;
            return;
        }
        // Recurse case (inside node): Check index child is not full
        if node.children[idx].kv_pairs.len() == 2 * t - 1 {
            Self::split_child(node, t, idx);

            // After split decide which child to descend into
            if key > node.kv_pairs[idx].0 {
                idx += 1;
            } else if key == node.kv_pairs[idx].0 {
                node.kv_pairs[idx].1 = value;
                return;
            }
        }
        // Recurse into the appropriate child
        Self::insert_internal(&mut node.children[idx], t, key, value);
    }


    /// Split a full child node during insertion.
    ///
    /// When a child at `node.children[i]` contains the maximum number of keys
    /// (`2t - 1`), this function splits it into two nodes and bumps the middle
    /// key into the parent. Check that there is no node overflows and maintains
    /// B-tree balance.
    ///
    /// # Arguments
    /// * `node` - The parent node containing the full child.
    /// * `i`    - The index of the full child to split.
    ///
    /// # Behavior
    /// - The left child keeps the first `t - 1` keys.
    /// - The right child receives the last `t - 1` keys.
    /// - The median key is moved up into the parent at position `i`.
    /// - If the full child is an internal node, its children are split as well.
    ///
    /// # Call outs
    /// Will call out when called on a child that is not actually full.
    fn split_child(node: &mut BTreeNode, t: usize, i: usize) {
        // We are here because child node is full
        let full_child = &mut node.children[i];
        let mut right = Box::new(BTreeNode::new(full_child.is_leaf));

        // Right node gets t-1 largest kv_pairs
        right.kv_pairs = full_child.kv_pairs.split_off(t);
        // Grab  the middle node
        let middle = full_child.kv_pairs.pop().expect("full child must have middle");

        // If internal, split children too: left keeps [0..t), right takes [t..]
        if !full_child.is_leaf {
            right.children = full_child.children.split_off(t);
        }
        // Insert middle into parent and link new right child
        node.kv_pairs.insert(i, middle);
        node.children.insert(i + 1, right);
    }


    // =========================
    // Deletion helpers
    // =========================

    /// Recursive helper for deleting a key from a B-tree node.
    ///
    /// # Arguments
    /// * `node` - A mutable reference to the current B-tree node being examined.
    /// * `t` - The minimum degree of the B-tree (controls branching factor).
    /// * `key` - The key to delete.
    ///
    /// # Behavior
    /// This function implements the standard B-tree deletion algorithm:
    ///
    /// 1. **Key found in this node**
    ///    - If the node is a leaf: remove the key directly.
    ///    - If the node is internal:
    ///       * Replace with predecessor if left child has ≥ `t` keys.
    ///       * Replace with successor if right child has ≥ `t` keys.
    ///       * Otherwise merge the two children and recurse into the merged node.
    ///
    /// 2. **Key not found in this node**
    ///    - If the node is a leaf: the key is not present, nothing is done.
    ///    - If the node is internal:
    ///       * Checks if the child about to be descended into has ≥ `t` keys
    ///         (borrowing/merging if needed).
    ///       * Recurse into the correct child to continue searching.
    ///
    /// # Notes
    /// * The `t` parameter helps check that all nodes (except root)
    ///   maintain the minimum space property of a B-tree.
    /// * This function assumes helper functions (`max_kvs`, `min_kvs`,
    ///   `merge_children`, `check_min_kvs) handle the details of
    ///   maintaining balance and invariants.
    /// * Used internally by `delete` to perform the actual recursive traversal.
    fn delete_internal(node: &mut BTreeNode, t: usize, key: &str) {
        let idx = node.lower_bound(key);

        // First case - key is in this node
        if idx < node.kv_pairs.len() && node.kv_pairs[idx].0 == key {
            if node.is_leaf {
                // Leaf node - just remove
                node.kv_pairs.remove(idx);

            } else {
                // Internal node
                if node.children[idx].kv_pairs.len() >= t {
                    // Replace with predecessor
                    let (pred_k, pred_v) = Self::max_kvs(&mut node.children[idx]);
                    node.kv_pairs[idx] = (pred_k.clone(), pred_v.clone());
                    Self::delete_internal(&mut node.children[idx], t, &pred_k);

                } else if node.children[idx + 1].kv_pairs.len() >= t {
                    // Replace with successor
                    let (succ_k, succ_v) = Self::min_kvs(&mut node.children[idx + 1]);
                    node.kv_pairs[idx] = (succ_k.clone(), succ_v.clone());
                    Self::delete_internal(&mut node.children[idx + 1], t, &succ_k);

                } else {
                    // Merge children[idx] + key + children[idx+1], then recurse
                    Self::merge_children(node, idx);
                    Self::delete_internal(&mut node.children[idx], t, key);
                }
            }
            return;
        }

        // Next case - key is not in this node - no op
        if node.is_leaf {
            return;
        }

        // Check child[idx] has at least t kv_pairs before descending
        Self::check_min_kvs(node, t, idx);

        // Descend (idx might shift after borrow/merge - watch for it)
        let next_idx = idx.min(node.kv_pairs.len());
        Self::delete_internal(&mut node.children[next_idx], t, key);
    }


    /// Checks that the child at index `idx` has at least `t` kv_pairs before descending.
    ///
    /// # Arguments
    /// * `node` - The parent node containing the child at index `idx`.
    /// * `idx` - The index of the child to check.
    ///
    /// # Behavior
    /// - If the child already has ≥ `t` kv_pairs, nothing is done.
    /// - Otherwise:
    ///   * Try borrowing a key from the left sibling (if it exists and has ≥ `t` kv_pairs).
    ///   * Else try borrowing from the right sibling.
    ///   * If neither sibling can donate, merge the child with one of its siblings.
    fn check_min_kvs(node: &mut BTreeNode, t: usize, idx: usize) {

        // If child already has enough kv_pairs, nothing to do
        if node.children[idx].kv_pairs.len() >= t {
            return;
        }

        // Try to borrow from left sibling
        if idx > 0 && node.children[idx - 1].kv_pairs.len() >= t {
            Self::borrow_from_prev(node, idx);
        }
        // Else try to borrow from right sibling
        else if idx + 1 < node.children.len() && node.children[idx + 1].kv_pairs.len() >= t {
            Self::borrow_from_next(node, idx);
        }
        // Else merge with a sibling
        else {
            if idx + 1 < node.children.len() {
                Self::merge_children(node, idx);
            } else {
                Self::merge_children(node, idx - 1);
            }
        }
    }


    /// Borrows a kv_pair from the left sibling of `node.children[idx]`.
    ///
    /// # Arguments
    /// * `node` - The parent node that holds the key separating the two siblings.
    /// * `idx` - The index of the child that is underflowing (has < t kv_pairs).
    ///
    /// # Behavior
    /// - Takes the separator key from the parent (`node.kv_pairs[idx - 1]`)
    ///   and inserts it as the first key of the underflowing child.
    /// - Moves the last key from the left sibling up into the parent
    ///   (replacing the separator).
    /// - If the nodes are internal:
    ///   * Moves the last child pointer of the left sibling into the beginning
    ///     of the underflowing child’s children.
    ///
    /// This maintains the B-tree invariants during deletion by redistributing
    /// kv_pairs so that the underflowing child regains at least `t` keys.
    fn borrow_from_prev(node: &mut BTreeNode, idx: usize) {
        // Child idx borrows one kv_pair from child idx-1 via parent
        let (left_slice, right_slice) = node.children.split_at_mut(idx);
        let left = &mut left_slice[idx - 1];
        let child = &mut right_slice[0];

        // Move parent kv_pair down to child (as first)
        let parent_kvs = node.kv_pairs[idx - 1].clone();
        child.kv_pairs.insert(0, parent_kvs);

        // Move left's last kv_pair up to parent
        let left_last = left.kv_pairs.pop().expect("left sibling has kv_pairs");
        node.kv_pairs[idx - 1] = left_last;

        // If internal, move a child pointer
        if !left.is_leaf {
            let moved = left.children.pop().expect("left child has a child to move");
            child.children.insert(0, moved);
        }
    }


    /// Borrows a kv_pair from the right sibling of `node.children[idx]`.
    ///
    /// # Arguments
    /// * `node` - The parent node that holds the key separating the two siblings.
    /// * `idx` - The index of the child that is underflowing (has < t kv_pairs).
    ///
    /// # Behavior
    /// - Takes the separator key from the parent (`node.kv_pairs[idx + 1]`)
    ///   and inserts it as the first key of the underflowing child.
    /// - Moves the last key from the right sibling up into the parent
    ///   (replacing the separator).
    /// - If the nodes are internal:
    ///   * Moves the last child pointer of the right sibling into the beginning
    ///     of the underflowing child’s children.
    ///
    /// This maintains the B-tree invariants during deletion by redistributing
    /// kv_pairs so that the underflowing child regains at least `t` keys.
    fn borrow_from_next(node: &mut BTreeNode, idx: usize) {
        // Child idx borrows one kv_pair from child idx+1 via parent
        let (left_slice, right_slice) = node.children.split_at_mut(idx + 1);
        let right = &mut right_slice[0];
        let child = &mut left_slice[idx];

        // Move parent kv_pair down to child (as last)
        let parent_kvs = node.kv_pairs[idx].clone();
        child.kv_pairs.push(parent_kvs);

        // Move right's first kv_pair up to parent
        let right_first = right.kv_pairs.remove(0);
        node.kv_pairs[idx] = right_first;

        // If internal, move a child pointer
        if !right.is_leaf {
            let moved = right.children.remove(0);
            child.children.push(moved);
        }
    }


    /// Merge `node.children[idx]`, the separating parent key,
    /// and `node.children[idx+1]` into a single child at `idx`.
    fn merge_children(node: &mut BTreeNode, idx: usize) {
        // Merge child idx, parent kv_pairs idx, and child idx+1 into child idx
        let mut right = node.children.remove(idx + 1);
        let parent_kvs = node.kv_pairs.remove(idx);
        let left = &mut node.children[idx];

        // Bring parent key down and append right child’s kv_pairs
        left.kv_pairs.push(parent_kvs);
        left.kv_pairs.append(&mut right.kv_pairs);

        // If internal, also merge child pointers
        if !left.is_leaf {
            left.children.append(&mut right.children);
        }
    }


    /// Return the minimum key–value pair in the given subtree.
    /// Descends left until reaching a leaf.
    fn min_kvs(node: &mut BTreeNode) -> (String, String) {
        let mut current_node = node;
        while !current_node.is_leaf {
            current_node = &mut current_node.children[0];
        }
        current_node.kv_pairs.first().expect("non-empty").clone()
    }


    /// Return the maximum key–value pair in the given subtree.
    /// Descends right until reaching a leaf.
    fn max_kvs(node: &mut BTreeNode) -> (String, String) {
        let mut current_node = node;
        while !current_node.is_leaf {
            let last = current_node.children.len() - 1;
            current_node = &mut current_node.children[last];
        }
        current_node.kv_pairs.last().expect("non-empty").clone()
    }
}
