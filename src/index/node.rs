// =====================================================================
// File: index/node.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 21, 2025 - Refactored Sept 22, 2025
//
// Description:
//   Defines the core B-tree node structure (`BTreeNode`) used by the
//   in-memory index of the key-value store. Each node maintains:
//
//   - `kv_pairs`: Ordered key–value pairs stored within the node.
//   - `children`: References to child nodes (empty if this node is a leaf).
//   - `is_leaf` : Boolean flag indicating whether the node is a leaf.
//
// Notes:
//   * A B-tree node can contain multiple key–value pairs, with children
//     linking to subtrees that maintain the B-tree ordering invariants.
//   * This file contains only the node representation and helpers.
//     Higher-level operations (insert, search, delete) are implemented
//     in `tree.rs`.
// =====================================================================


// BTree Referencing:
// https://build-your-own.org/database/
// https://www.geeksforgeeks.org/dsa/introduction-of-b-tree-2/
/// Basic Foundational BTree Node
#[derive(Debug)]
pub struct BTreeNode {
    pub kv_pairs: Vec<(String, String)>,
    /// Box allows Rust to recursivley move through values and nodes - Heap
    pub children: Vec<Box<BTreeNode>>,
    pub is_leaf: bool,
}


impl BTreeNode {
    // Creates a new empty B-tree node.
    ///
    /// # Arguments
    ///
    /// * `is_leaf` - A boolean flag indicating whether this node
    ///   is a leaf (has no children) or an internal node (may have children).
    ///
    /// # Returns
    ///
    /// A `BTreeNode` instance with empty keys-values, and children vectors.
    ///
    /// # Example
    /// ```
    /// use kvstore::index::BTreeNode;
    /// let leaf = BTreeNode::new(true);
    /// assert!(leaf.kv_pairs.is_empty());
    /// assert!(leaf.is_leaf);
    /// ```
    pub fn new(is_leaf: bool) -> Self {
        Self {
            kv_pairs: Vec::new(),
            children: Vec::new(),
            is_leaf,
        }
    }


    /// Binary search helper: returns the index of the key if found,
    /// or the position where it should be inserted otherwise.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to compare against the node’s stored keys.
    ///
    /// # Returns
    ///
    /// The index of the first key that is >= `key`.
    /// If all stored keys are less than `key`, returns the length of the vector
    /// (i.e., the "end" position).
    ///
    /// # Example
    /// ```
    /// use kvstore::BTreeNode;
    ///
    /// let mut node = BTreeNode::new(true);
    /// node.kv_pairs.push(("cat".to_string(), "meow".to_string()));
    /// node.kv_pairs.push(("dog".to_string(), "bark".to_string()));
    ///
    /// assert_eq!(node.lower_bound("ant"), 0);
    /// assert_eq!(node.lower_bound("dog"), 1);
    /// assert_eq!(node.lower_bound("elephant"), 2);
    /// ```
    pub fn lower_bound(&self, key: &str) -> usize {
        self.kv_pairs
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .unwrap_or_else(|pos| pos)
    }
}
