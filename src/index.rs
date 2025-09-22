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
            let i = if key > new_root.kv_pairs[0].0 { 1 } else { 0 };
            Self::insert_inside(&mut new_root.children[i], t, key, value);

            // Replace the tree's root
            self.root = new_root;
        } else {
            // Root not full — normal descent
            Self::insert_inside(&mut self.root, t, key, value);
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
    ///   - Otherwise, split a full child before descending to ensure space,
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
    fn insert_inside(node: &mut BTreeNode, t: usize, key: String, value: String) {
        let mut i = node.lower_bound(&key);

        // Base case - leaf insert
        if node.is_leaf {
            // Overwrite if key exists (last write wins)
            if i < node.kv_pairs.len() && node.kv_pairs[i].0 == key {
                node.kv_pairs[i].1 = value;
            } else {
                node.kv_pairs.insert(i, (key, value));
            }
            return;
        }
        // If key exists in internal node - overwrite value and stop
        if i < node.kv_pairs.len() && node.kv_pairs[i].0 == key {
            node.kv_pairs[i].1 = value;
            return;
        }
        // Recurse case - inside node
        // Check i child is not full
        if node.children[i].kv_pairs.len() == 2 * t - 1 {
            Self::split_child(node, t, i);

            // After split decide which child to descend into
            if key > node.kv_pairs[i].0 {
                i += 1;
            } else if key == node.kv_pairs[i].0 {
                node.kv_pairs[i].1 = value;
                return;
            }
        }
        // Recurse into the appropriate child
        Self::insert_inside(&mut node.children[i], t, key, value);
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
        // keep [0..t) in child ------ [t..] to right
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

    #[test]
    fn insert_and_search_basic() {
        let mut t = BTreeIndex::new(2);
        t.insert("dog".into(), "bark".into());
        t.insert("cat".into(), "meow".into());
        t.insert("fish".into(), "splash".into());
        assert_eq!(t.search("dog"), Some("bark"));
        assert_eq!(t.search("cat"), Some("meow"));
        assert_eq!(t.search("bird"), None);
    }


}
