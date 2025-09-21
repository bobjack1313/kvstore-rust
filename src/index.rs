// =====================================================================
// File: index.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 21, 2025
//
// =====================================================================


// BTree Referencing:
// https://build-your-own.org/database/
// https://www.geeksforgeeks.org/dsa/introduction-of-b-tree-2/
/// Basic Foundational BTree Node
pub struct BTreeNode {
    /// Key–value pairs stored in this node.
    /// Keys are kept sorted so we can binary search efficiently.
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
    pub fn lower_bound(&self, key: &str) -> usize {
        self.kv_pairs
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .unwrap_or_else(|pos| pos)
    }
}


/// BTree Index, interfaces with lib to index the db with the nodes and leafs.
pub struct BTreeIndex {
    pub t: usize,
    pub root: Box<BTreeNode>,
}


impl BTreeIndex {
    /// Create a new empty B-tree with minimum degree t greather than 2.
    pub fn new(t: usize) -> Self {
        assert!(t >= 2, "B-tree minimum degree t must be >= 2");
        Self {
            t,
            root: Box::new(BTreeNode::new(true)),
        }
    }

    /// Standard B-tree search: returns value if present.
    pub fn search(&self, key: &str) -> Option<&str> {

        // Recursive function declaration for node search
        fn search_node<'a>(node: &'a BTreeNode, key: &str) -> Option<&'a str> {
            // Find the position in this node where the key would belong
            let i = node.lower_bound(key);

            // Base Case - Successfully found the key in the current node
            if i < node.kv_pairs.len() && node.kv_pairs[i].0 == key {
                return Some(node.kv_pairs[i].1.as_str());
            }

            // No key here, base case fails - search ends
            if node.is_leaf {
                None

            // No key here, there are children, so recursive search
            } else {
                search_node(&node.children[i], key)
            }
        }
        // Call search
        search_node(&self.root, key)
    }



}


// =================================================================
// index.rs Unit tests
// =================================================================
#[cfg(test)]
mod index_tests {
    use super::*;

    #[test]
    fn test_new_leaf_node() {
        let node = BTreeNode::new(true);
        assert!(node.kv_pairs.is_empty());
        assert!(node.children.is_empty());
        assert!(node.is_leaf);
    }

    #[test]
    fn test_new_internal_node() {
        let node = BTreeNode::new(false);
        assert!(!node.is_leaf);
    }

    #[test]
    fn test_new_internal_index() {
        let index = BTreeIndex::new(2);
        assert!(index.t >= 2);
        assert!(index.root.kv_pairs.is_empty());
        assert!(index.root.children.is_empty());
        assert!(index.root.is_leaf);
    }

    #[test]
    // Initial search testing without using inserts
    fn search_in_single_leaf_node() {
        // Create a leaf with two entries
        let mut root = BTreeNode::new(true);
        root.kv_pairs.push(("cat".into(), "meow".into()));
        root.kv_pairs.push(("dog".into(), "bark".into()));
        // println!("{:?}", root.kv_pairs);
        let tree = BTreeIndex { t: 2, root: Box::new(root) };

        // Should find exact matches
        assert_eq!(tree.search("dog"), Some("bark"));
        assert_eq!(tree.search("cat"), Some("meow"));

        // This will miss - key not in tree
        assert_eq!(tree.search("fish"), None);
    }

    #[test]
    // Tests how search performs recursively - not using insert to build
    fn search_in_internal_node() {
        // Root is internal (is_leaf = false)
        let mut root = BTreeNode::new(false);
        // Make a split
        root.kv_pairs.push(("m".into(), "middle".into()));

        // Left child: [a -> "A", f -> "F"]
        let mut left = BTreeNode::new(true);
        left.kv_pairs.push(("a".into(), "A".into()));
        left.kv_pairs.push(("f".into(), "F".into()));

        // Right child: [z -> "Z"]
        let mut right = BTreeNode::new(true);
        right.kv_pairs.push(("z".into(), "Z".into()));

        // Attach children
        root.children.push(Box::new(left));
        root.children.push(Box::new(right));

        let tree = BTreeIndex { t: 2, root: Box::new(root) };

        // These require descending into children
        assert_eq!(tree.search("a"), Some("A"));
        assert_eq!(tree.search("f"), Some("F"));
        assert_eq!(tree.search("z"), Some("Z"));

        // Key not present
        assert_eq!(tree.search("x"), None);
    }
}
