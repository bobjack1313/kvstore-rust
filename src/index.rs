// =====================================================================
// File: index.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project Part 1
// Date: Sept 21, 2025
//
// =====================================================================



// struct Node {
//     int n;
//     int key[MAX_KEYS];
//     Node* child[MAX_CHILDREN];
//     bool leaf;
// };

// Node* BtreeSearch(Node* x, int k) {
//     int i = 0;
//     while (i < x->n && k > x->key[i]) {
//         i++;
//     }
//     if (i < x->n && k == x->key[i]) {
//         return x;
//     }
//     if (x->leaf) {
//         return nullptr;
//     }
//     return BtreeSearch(x->child[i], k);
// }


// BTree Referencing:
// https://build-your-own.org/database/
// https://www.geeksforgeeks.org/dsa/introduction-of-b-tree-2/
/// Basic Foundational BTree Node
pub struct BTreeNode {
    pub keys: Vec<String>,
    pub values: Vec<String>,
    pub children: Vec<BTreeNode>,
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
    /// A `BTreeNode` instance with empty keys, values, and children vectors.
    ///
    /// # Example
    /// ```
    /// use kvstore::index::BTreeNode;
    /// let leaf = BTreeNode::new(true);
    /// assert!(leaf.keys.is_empty());
    /// assert!(leaf.is_leaf);
    /// ```
    pub fn new(is_leaf: bool) -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf,
        }
    }
}



#[cfg(test)]
mod index_tests {
    use super::*;

    #[test]
    fn test_new_leaf_node() {
        let node = BTreeNode::new(true);
        assert!(node.keys.is_empty());
        assert!(node.values.is_empty());
        assert!(node.children.is_empty());
        assert!(node.is_leaf);
    }

    #[test]
    fn test_new_internal_node() {
        let node = BTreeNode::new(false);
        assert!(!node.is_leaf);
    }
}



