//! Bloom filter for fast existence checks

use crate::Result;
use std::hash::{Hash, Hasher};

/// Bloom filter implementation
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new bloom filter
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let num_bits = num_keys * bits_per_key;
        let num_bytes = (num_bits + 7) / 8;
        
        // Optimal number of hash functions
        let num_hashes = ((bits_per_key as f64) * 0.69).round() as usize;
        let num_hashes = num_hashes.clamp(1, 30);
        
        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    /// Create from existing data
    pub fn from_bytes(data: Vec<u8>, num_hashes: usize) -> Self {
        let num_bits = data.len() * 8;
        Self {
            bits: data,
            num_bits,
            num_hashes,
        }
    }

    /// Add a key to the filter
    pub fn add<K: Hash>(&mut self, key: &K) {
        let (h1, h2) = self.hash_key(key);
        
        for i in 0..self.num_hashes {
            let bit = self.bit_position(h1, h2, i);
            self.set_bit(bit);
        }
    }

    /// Check if a key may be in the set
    pub fn may_contain<K: Hash>(&self, key: &K) -> bool {
        let (h1, h2) = self.hash_key(key);
        
        for i in 0..self.num_hashes {
            let bit = self.bit_position(h1, h2, i);
            if !self.get_bit(bit) {
                return false;
            }
        }
        
        true
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    /// Get number of hash functions
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Estimated false positive rate
    pub fn false_positive_rate(&self, num_keys: usize) -> f64 {
        let k = self.num_hashes as f64;
        let m = self.num_bits as f64;
        let n = num_keys as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    fn hash_key<K: Hash>(&self, key: &K) -> (u64, u64) {
        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher1);
        let h1 = hasher1.finish();
        
        // Use a different seed for second hash
        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
        h1.hash(&mut hasher2);
        let h2 = hasher2.finish();
        
        (h1, h2)
    }

    fn bit_position(&self, h1: u64, h2: u64, i: usize) -> usize {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }

    fn set_bit(&mut self, bit: usize) {
        let byte = bit / 8;
        let offset = bit % 8;
        if byte < self.bits.len() {
            self.bits[byte] |= 1 << offset;
        }
    }

    fn get_bit(&self, bit: usize) -> bool {
        let byte = bit / 8;
        let offset = bit % 8;
        if byte < self.bits.len() {
            (self.bits[byte] >> offset) & 1 == 1
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(100, 10);
        
        for i in 0..100 {
            filter.add(&format!("key-{}", i));
        }
        
        // All added keys should be found
        for i in 0..100 {
            assert!(filter.may_contain(&format!("key-{}", i)));
        }
        
        // Count false positives for non-existent keys
        let mut false_positives = 0;
        for i in 100..1000 {
            if filter.may_contain(&format!("key-{}", i)) {
                false_positives += 1;
            }
        }
        
        // False positive rate should be around 1%
        let fp_rate = false_positives as f64 / 900.0;
        assert!(fp_rate < 0.05, "False positive rate too high: {}", fp_rate);
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(50, 10);
        
        for i in 0..50 {
            filter.add(&i);
        }
        
        let bytes = filter.as_bytes().to_vec();
        let num_hashes = filter.num_hashes();
        
        let restored = BloomFilter::from_bytes(bytes, num_hashes);
        
        for i in 0..50 {
            assert!(restored.may_contain(&i));
        }
    }
}
