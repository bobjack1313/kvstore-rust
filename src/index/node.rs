// =====================================================================
// File: index/node.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
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


    /// Collects all keys stored in this subtree and appends them to the
    /// provided output vector in sorted (in-order) order.
    ///
    /// This method performs an in-order traversal of the B-tree:
    /// - If the node is a leaf, it simply pushes all keys in their
    ///   stored order.
    /// - If the node is internal, it recursively visits each child,
    ///   inserting the key that separates the children between those visits.
    ///
    /// # Arguments
    ///
    /// * `out` - A mutable vector that will be appended with the keys
    ///   discovered during traversal.
    ///
    /// # Behavior
    ///
    /// Keys are cloned and appended to `out`. The traversal guarantees that
    /// the resulting vector is globally sorted across the entire subtree.
    ///
    /// # Example
    /// ```
    /// use kvstore::BTreeNode;
    ///
    /// // Build a simple leaf node
    /// let mut node = BTreeNode::new(true);
    /// node.kv_pairs.push(("a".to_string(), "1".to_string()));
    /// node.kv_pairs.push(("b".to_string(), "2".to_string()));
    ///
    /// let mut out = Vec::new();
    /// node.collect_keys(&mut out);
    ///
    /// assert_eq!(out, vec!["a".to_string(), "b".to_string()]);
    /// ```
    pub fn collect_keys(&self, out: &mut Vec<String>) {
        if self.is_leaf {
            // Push ONLY keys
            for (k, _) in &self.kv_pairs {
                out.push(k.clone());
            }
        } else {
            // Internal node: in-order traversal
            for i in 0..self.kv_pairs.len() {
                // Left subtree
                self.children[i].collect_keys(out);

                // Key at index i
                out.push(self.kv_pairs[i].0.clone());
            }

            // Last child (rightmost subtree)
            self.children[self.kv_pairs.len()].collect_keys(out);
        }
    }

}
