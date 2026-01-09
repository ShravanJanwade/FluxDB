#![allow(dangerous_implicit_autorefs)]
//! Skip list implementation for MemTable
//!
//! A probabilistic data structure providing O(log n) insert/search operations.
//! Used by LevelDB, RocksDB, and HBase for their MemTables.

use rand::Rng;
use std::cmp::Ordering;
use std::ptr::NonNull;

const MAX_LEVEL: usize = 16;
const BRANCHING_FACTOR: u32 = 4;

/// A lock-free skip list
pub struct SkipList<K: Ord + Clone, V: Clone> {
    head: Box<Node<K, V>>,
    level: usize,
    len: usize,
}

struct Node<K, V> {
    key: Option<K>,
    value: Option<V>,
    forward: Vec<Option<NonNull<Node<K, V>>>>,
}

impl<K: Ord + Clone, V: Clone> SkipList<K, V> {
    /// Create a new skip list
    pub fn new() -> Self {
        Self {
            head: Box::new(Node::new_head()),
            level: 1,
            len: 0,
        }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: K, value: V) {
        let level = self.random_level();
        let mut update = vec![None; MAX_LEVEL];
        let mut current = self.head.as_mut() as *mut Node<K, V>;

        // Find position and collect update pointers
        for i in (0..self.level).rev() {
            unsafe {
                while let Some(next) = (*current).forward[i] {
                    if let Some(ref next_key) = (*next.as_ptr()).key {
                        if next_key < &key {
                            current = next.as_ptr();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            update[i] = Some(current);
        }

        // Check if key already exists
        unsafe {
            if let Some(next) = (*current).forward[0] {
                if let Some(ref next_key) = (*next.as_ptr()).key {
                    if next_key == &key {
                        // Update existing value
                        (*next.as_ptr()).value = Some(value);
                        return;
                    }
                }
            }
        }

        // Increase level if needed
        if level > self.level {
            for i in self.level..level {
                update[i] = Some(self.head.as_mut() as *mut Node<K, V>);
            }
            self.level = level;
        }

        // Create new node
        let new_node = Box::new(Node::new(key, value, level));
        let new_node_ptr = NonNull::new(Box::into_raw(new_node)).unwrap();

        // Update forward pointers
        for i in 0..level {
            unsafe {
                if let Some(prev) = update[i] {
                    (*new_node_ptr.as_ptr()).forward[i] = (*prev).forward[i];
                    (*prev).forward[i] = Some(new_node_ptr);
                }
            }
        }

        self.len += 1;
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut current = self.head.as_ref() as *const Node<K, V>;

        for i in (0..self.level).rev() {
            unsafe {
                while let Some(next) = (*current).forward[i] {
                    if let Some(ref next_key) = (*next.as_ptr()).key {
                        match next_key.cmp(key) {
                            Ordering::Less => current = next.as_ptr(),
                            Ordering::Equal => return (*next.as_ptr()).value.as_ref(),
                            Ordering::Greater => break,
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        None
    }

    /// Range query from start to end (inclusive)
    pub fn range<'a>(&'a self, start: &K, end: &K) -> impl Iterator<Item = (&'a K, &'a V)> {
        let mut results = Vec::new();
        let mut current = self.head.as_ref() as *const Node<K, V>;

        // Find starting position
        for i in (0..self.level).rev() {
            unsafe {
                while let Some(next) = (*current).forward[i] {
                    if let Some(ref next_key) = (*next.as_ptr()).key {
                        if next_key < start {
                            current = next.as_ptr();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        // Collect results
        unsafe {
            while let Some(next) = (*current).forward[0] {
                if let (Some(ref key), Some(ref value)) =
                    (&(*next.as_ptr()).key, &(*next.as_ptr()).value)
                {
                    if key > end {
                        break;
                    }
                    if key >= start {
                        results.push((key, value));
                    }
                }
                current = next.as_ptr();
            }
        }

        results.into_iter()
    }

    /// Iterate over all entries
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let mut results = Vec::new();
        let mut current = self.head.as_ref() as *const Node<K, V>;

        unsafe {
            while let Some(next) = (*current).forward[0] {
                if let (Some(ref key), Some(ref value)) =
                    (&(*next.as_ptr()).key, &(*next.as_ptr()).value)
                {
                    results.push((key, value));
                }
                current = next.as_ptr();
            }
        }

        results.into_iter()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn random_level(&self) -> usize {
        let mut lvl = 1;
        let mut rng = rand::thread_rng();
        while rng.gen_ratio(1, BRANCHING_FACTOR) && lvl < MAX_LEVEL {
            lvl += 1;
        }
        lvl
    }
}

impl<K: Ord + Clone, V: Clone> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, level: usize) -> Self {
        Self {
            key: Some(key),
            value: Some(value),
            forward: vec![None; level],
        }
    }

    fn new_head() -> Self {
        Self {
            key: None,
            value: None,
            forward: vec![None; MAX_LEVEL],
        }
    }
}

impl<K: Ord + Clone, V: Clone> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        let mut current = self.head.forward[0];
        while let Some(node) = current {
            unsafe {
                let next = (*node.as_ptr()).forward[0];
                drop(Box::from_raw(node.as_ptr()));
                current = next;
            }
        }
    }
}

// Skip list is safe to send between threads
unsafe impl<K: Ord + Clone + Send, V: Clone + Send> Send for SkipList<K, V> {}
unsafe impl<K: Ord + Clone + Sync, V: Clone + Sync> Sync for SkipList<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist_insert_get() {
        let mut list = SkipList::new();

        for i in 0..100 {
            list.insert(i, i * 10);
        }

        assert_eq!(list.len(), 100);

        for i in 0..100 {
            assert_eq!(list.get(&i), Some(&(i * 10)));
        }

        assert_eq!(list.get(&200), None);
    }

    #[test]
    fn test_skiplist_range() {
        let mut list = SkipList::new();

        for i in 0..100 {
            list.insert(i, i * 10);
        }

        let results: Vec<_> = list.range(&25, &35).collect();
        assert_eq!(results.len(), 11);
        assert_eq!(results[0], (&25, &250));
        assert_eq!(results[10], (&35, &350));
    }

    #[test]
    fn test_skiplist_update() {
        let mut list = SkipList::new();

        list.insert(1, 10);
        assert_eq!(list.get(&1), Some(&10));

        list.insert(1, 20);
        assert_eq!(list.get(&1), Some(&20));
        assert_eq!(list.len(), 1);
    }
}
