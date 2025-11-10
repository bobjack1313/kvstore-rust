// =====================================================================
// File: index/tests.rs
// Author: Bob Jack
// Course: CSCE 5350: Fundamentals of Database Systems
// Midterm/Final Project
// Date: Sept. 21, 2025 - Refactored Sept. 22, 2025
//
// Description:
//   Unit tests for the B-tree implementation (`BTreeNode` and
//   `BTreeIndex`). Covers insert, search, delete, and structural tests
//
// Notes:
//   * Only compiled when running `cargo test`.
//   * Does not affect release builds.
// =====================================================================


// =================================================================
// Unit tests cover basic Tree structure and simple search
// =================================================================
#[cfg(test)]
mod index_tests {
    use crate::BTreeNode;
    use crate::BTreeIndex;

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
        // Create a leaf with two kv_pairs
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


// =================================================================
// Unit tests cover node insertion into tree.
// =================================================================
#[cfg(test)]
mod index_insertion_tests {
    use crate::BTreeIndex;

    #[test]
    // Simple test for inserting
    fn insert_and_search_basic() {
        let mut t = BTreeIndex::new(2);
        t.insert("dog".into(), "bark".into());
        t.insert("cat".into(), "meow".into());
        t.insert("fish".into(), "splash".into());
        assert_eq!(t.search("dog"), Some("bark"));
        assert_eq!(t.search("cat"), Some("meow"));
        assert_eq!(t.search("bird"), None);
    }

   #[test]
    fn insert_overwrites_value() {
        let mut t = BTreeIndex::new(2);
        t.insert("dod".into(), "bark".into());
        t.insert("dog".into(), "woofwoof".into());
        assert_eq!(t.search("dog"), Some("woofwoof"));
    }

    #[test]
    fn insert_causes_root_split() {
        let mut t = BTreeIndex::new(2);
        t.insert("a".into(), "1".into());
        t.insert("b".into(), "2".into());
        t.insert("c".into(), "3".into());
        // This one creates split
        t.insert("d".into(), "4".into());

        assert_eq!(t.search("a"), Some("1"));
        assert_eq!(t.search("d"), Some("4"));
    }

    #[test]
    fn search_nonexistent_key() {
        let mut t = BTreeIndex::new(2);
        t.insert("cat".into(), "meow".into());
        assert_eq!(t.search("dog"), None);
    }

    #[test]
    fn consistent_key_sorting() {
        let mut t = BTreeIndex::new(2);
        t.insert("dog".into(), "bark".into());
        t.insert("cat".into(), "meow".into());
        t.insert("apple".into(), "fruit".into());

        let root = &t.root;
        assert!(root.kv_pairs.windows(2).all(|w| w[0].0 < w[1].0));
    }
}

// =================================================================
// Unit tests cover more detailed searching.
// =================================================================
#[cfg(test)]
mod index_expanded_search_tests {
    use crate::BTreeIndex;

    #[test]
    fn multiple_splits() {
        let mut t = BTreeIndex::new(2);
        for (k, v) in [("a","1"),("b","2"),("c","3"),("d","4"),("e","5"),("f","6")] {
            t.insert(k.into(), v.into());
        }
        assert_eq!(t.search("e"), Some("5"));
        assert_eq!(t.search("f"), Some("6"));
    }

    #[test]
    fn search_misses_in_leaf() {
        let mut tree = BTreeIndex::new(2);
        tree.insert("fish".into(), "splash".into());

        assert_eq!(tree.search("bird"), None);
    }

    #[test]
    fn search_descends_into_child() {
        let mut tree = BTreeIndex::new(2);
        // Insert enough keys to cause a split
        for (k, v) in [("a","A"),("b","B"),("c","C"),("d","D"),("e","E")] {
            tree.insert(k.into(), v.into());
        }

        // Keys before split
        assert_eq!(tree.search("a"), Some("A"));
        assert_eq!(tree.search("c"), Some("C"));
        // Keys after split (forces recursion)
        assert_eq!(tree.search("e"), Some("E"));
    }

    #[test]
    fn search_after_overwrite() {
        let mut tree = BTreeIndex::new(2);
        tree.insert("x".into(), "old".into());
        tree.insert("x".into(), "new".into());

        assert_eq!(tree.search("x"), Some("new"));
    }

    #[test]
    fn search_many_keys() {
        let mut tree = BTreeIndex::new(2);
        for i in 0..50 {
            tree.insert(format!("k{:02}", i), format!("v{:02}", i));
        }

        // Spot-check a few
        assert_eq!(tree.search("k00"), Some("v00"));
        assert_eq!(tree.search("k25"), Some("v25"));
        assert_eq!(tree.search("k49"), Some("v49"));
        // Null case
        assert_eq!(tree.search("k99"), None);
    }
}


// =================================================================
// Unit tests for deleting from tree
// =================================================================
#[cfg(test)]
mod index_delete_tests {
    use crate::BTreeIndex;

    /// Helper to make a tree with degree 2 and some inserts
    fn sample_tree() -> BTreeIndex {
        let mut t = BTreeIndex::new(2);
        t.insert("dog".into(), "bark".into());
        t.insert("cat".into(), "meow".into());
        t.insert("dinosaur".into(), "raaawr".into());
        t.insert("bird".into(), "chirp".into());
        t.insert("frog".into(), "ribbet".into());
        t.insert("elephant".into(), "honkhonk".into());
        t.insert("fox".into(), "fraka-kaka-kaka-kaka-kow!".into());
        t
    }

    #[test]
    fn delete_leaf_key() {
        let mut t = sample_tree();
        assert_eq!(t.search("frog"), Some("ribbet"));
        t.delete("frog");
        assert_eq!(t.search("frog"), None);
    }

    #[test]
    fn delete_non_existent_key() {
        let mut t = sample_tree();
        t.delete("unicorn");
        // Nothing should change
        assert_eq!(t.search("dog"), Some("bark"));
        assert_eq!(t.search("cat"), Some("meow"));
    }

    #[test]
    fn delete_internal_key_with_predecessor() {
        let mut t = sample_tree();
        assert_eq!(t.search("cat"), Some("meow"));
        // "cat" will be replaced with predecessor
        t.delete("cat");
        assert_eq!(t.search("cat"), None);
        // Other entries still intact
        assert_eq!(t.search("dog"), Some("bark"));
    }

    #[test]
    fn delete_internal_key_with_successor() {
        let mut t = sample_tree();
        assert_eq!(t.search("dinosaur"), Some("raaawr"));
        // "dinosaur" replaced with successor
        t.delete("dinosaur");
        assert_eq!(t.search("dinosaur"), None);
        // Tree still contains other values
        assert_eq!(t.search("dog"), Some("bark"));
        assert_eq!(t.search("fox"), Some("fraka-kaka-kaka-kaka-kow!"));
    }

    #[test]
    fn delete_until_empty() {
        let mut t = sample_tree();
        let keys = vec![
            "bird", "cat", "dinosaur", "dog", "elephant", "fox", "frog",
        ];
        for k in &keys {
            assert!(t.search(k).is_some(), "missing before delete: {}", k);
            t.delete(k);
            assert_eq!(t.search(k), None, "still present after delete: {}", k);
        }
        // Root should now be empty leaf
        assert!(t.root.is_leaf);
        assert!(t.root.kv_pairs.is_empty());
    }

    #[test]
    fn delete_causes_merge_case() {
        let mut t = BTreeIndex::new(2);
        // Insert a sequence designed to trigger merging on deletion
        for k in &["a", "b", "c", "d", "e", "f", "g"] {
            t.insert(k.to_string(), format!("val{}", k));
        }
        t.delete("c"); // should trigger internal restructuring
        assert_eq!(t.search("c"), None);
        assert_eq!(t.search("a"), Some("vala"));
        assert_eq!(t.search("g"), Some("valg"));
    }
}

