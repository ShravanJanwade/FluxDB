//! Gorilla encoder for time-series compression

use super::bitstream::BitWriter;
use super::CompressedBlock;

/// Gorilla encoder for time-series data
pub struct GorillaEncoder {
    writer: BitWriter,
    count: usize,
    
    // Timestamp state
    first_timestamp: i64,
    prev_timestamp: i64,
    prev_timestamp_delta: i64,
    
    // Value state
    prev_value_bits: u64,
    prev_leading_zeros: u32,
    prev_trailing_zeros: u32,
}

impl GorillaEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            writer: BitWriter::with_capacity(4096),
            count: 0,
            first_timestamp: 0,
            prev_timestamp: 0,
            prev_timestamp_delta: 0,
            prev_value_bits: 0,
            prev_leading_zeros: 0,
            prev_trailing_zeros: 0,
        }
    }

    /// Encode a timestamp-value pair
    pub fn encode(&mut self, timestamp: i64, value: f64) {
        if self.count == 0 {
            self.encode_first(timestamp, value);
        } else {
            self.encode_timestamp(timestamp);
            self.encode_value(value);
        }
        self.count += 1;
    }

    /// Finish encoding and return compressed block
    pub fn finish(self) -> CompressedBlock {
        let last_timestamp = self.prev_timestamp;
        CompressedBlock {
            data: self.writer.finish(),
            count: self.count,
            first_timestamp: self.first_timestamp,
            last_timestamp,
        }
    }

    fn encode_first(&mut self, timestamp: i64, value: f64) {
        self.first_timestamp = timestamp;
        self.prev_timestamp = timestamp;
        
        // Write first timestamp as full 64 bits
        self.writer.write_bits(timestamp as u64, 64);
        
        // Write first value as full 64 bits
        let value_bits = value.to_bits();
        self.writer.write_bits(value_bits, 64);
        self.prev_value_bits = value_bits;
    }

    fn encode_timestamp(&mut self, timestamp: i64) {
        let delta = timestamp - self.prev_timestamp;
        let delta_of_delta = delta - self.prev_timestamp_delta;
        
        // Most consecutive timestamps have the same delta (e.g., every 10 seconds)
        // So delta-of-delta is usually 0, encoded as a single bit
        
        if delta_of_delta == 0 {
            // '0' bit: delta is the same
            self.writer.write_bit(false);
        } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
            // '10' + 7 bits: delta_of_delta fits in 7 bits
            self.writer.write_bits(0b10, 2);
            self.writer.write_bits((delta_of_delta + 63) as u64, 7);
        } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
            // '110' + 9 bits
            self.writer.write_bits(0b110, 3);
            self.writer.write_bits((delta_of_delta + 255) as u64, 9);
        } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
            // '1110' + 12 bits
            self.writer.write_bits(0b1110, 4);
            self.writer.write_bits((delta_of_delta + 2047) as u64, 12);
        } else {
            // '1111' + 64 bits: full delta_of_delta
            self.writer.write_bits(0b1111, 4);
            self.writer.write_bits(delta_of_delta as u64, 64);
        }
        
        self.prev_timestamp_delta = delta;
        self.prev_timestamp = timestamp;
    }

    fn encode_value(&mut self, value: f64) {
        let value_bits = value.to_bits();
        let xor = value_bits ^ self.prev_value_bits;
        
        if xor == 0 {
            // Values are identical, write single '0' bit
            self.writer.write_bit(false);
        } else {
            self.writer.write_bit(true);
            
            let leading_zeros = xor.leading_zeros();
            let trailing_zeros = xor.trailing_zeros();
            
            // Check if the meaningful bits fit within previous window
            if leading_zeros >= self.prev_leading_zeros 
                && trailing_zeros >= self.prev_trailing_zeros {
                // Use previous window
                self.writer.write_bit(false);
                let meaningful_bits = 64 - self.prev_leading_zeros - self.prev_trailing_zeros;
                let shifted = xor >> self.prev_trailing_zeros;
                self.writer.write_bits(shifted, meaningful_bits as usize);
            } else {
                // New window
                self.writer.write_bit(true);
                
                // Leading zeros (5 bits, max 31)
                let leading = leading_zeros.min(31);
                self.writer.write_bits(leading as u64, 5);
                
                // Meaningful bits length (6 bits, max 64)
                let meaningful_bits = 64 - leading_zeros - trailing_zeros;
                self.writer.write_bits(meaningful_bits as u64, 6);
                
                // Meaningful bits
                let shifted = xor >> trailing_zeros;
                self.writer.write_bits(shifted, meaningful_bits as usize);
                
                self.prev_leading_zeros = leading_zeros;
                self.prev_trailing_zeros = trailing_zeros;
            }
        }
        
        self.prev_value_bits = value_bits;
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_single_point() {
        let mut encoder = GorillaEncoder::new();
        encoder.encode(1000000000, 23.5);
        let block = encoder.finish();
        
        assert_eq!(block.count, 1);
        assert_eq!(block.first_timestamp, 1000000000);
        assert_eq!(block.last_timestamp, 1000000000);
    }

    #[test]
    fn test_encoder_constant_delta() {
        let mut encoder = GorillaEncoder::new();
        
        // Constant 10-second intervals
        for i in 0..100 {
            encoder.encode(1000000000 + i * 10_000_000_000, 23.5);
        }
        
        let block = encoder.finish();
        assert_eq!(block.count, 100);
        
        // With constant delta and constant value, compression should be excellent
        // Each additional point should take about 2 bits (1 for timestamp, 1 for value)
        let bytes_per_point = block.bytes_per_point();
        assert!(bytes_per_point < 2.0, "Expected < 2 bytes/point for constant data, got {}", bytes_per_point);
    }

    #[test]
    fn test_encoder_varying_values() {
        let mut encoder = GorillaEncoder::new();
        
        for i in 0..1000 {
            let timestamp = 1000000000 + i * 10_000_000_000;
            let value = 20.0 + (i as f64 * 0.1).sin() * 5.0;
            encoder.encode(timestamp, value);
        }
        
        let block = encoder.finish();
        assert_eq!(block.count, 1000);
        
        // Gorilla typically achieves 1.3-1.5 bytes per point on real data
        let bytes_per_point = block.bytes_per_point();
        assert!(bytes_per_point < 5.0, "Expected < 5 bytes/point, got {}", bytes_per_point);
    }
}
