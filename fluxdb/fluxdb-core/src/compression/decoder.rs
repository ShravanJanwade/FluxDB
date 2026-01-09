//! Gorilla decoder for time-series decompression

use super::bitstream::BitReader;
use crate::{FluxError, Result};

/// Gorilla decoder for time-series data
pub struct GorillaDecoder<'a> {
    reader: BitReader<'a>,
    count: usize,
    decoded: usize,
    
    // Timestamp state
    prev_timestamp: i64,
    prev_timestamp_delta: i64,
    
    // Value state
    prev_value_bits: u64,
    prev_leading_zeros: u32,
    prev_trailing_zeros: u32,
}

impl<'a> GorillaDecoder<'a> {
    /// Create a new decoder
    pub fn new(data: &'a [u8], count: usize) -> Self {
        Self {
            reader: BitReader::new(data),
            count,
            decoded: 0,
            prev_timestamp: 0,
            prev_timestamp_delta: 0,
            prev_value_bits: 0,
            prev_leading_zeros: 0,
            prev_trailing_zeros: 0,
        }
    }

    /// Decode all points
    pub fn decode_all(&mut self) -> Result<Vec<(i64, f64)>> {
        let mut points = Vec::with_capacity(self.count);
        
        while let Some((ts, val)) = self.decode_next()? {
            points.push((ts, val));
        }
        
        Ok(points)
    }

    /// Decode the next timestamp-value pair
    pub fn decode_next(&mut self) -> Result<Option<(i64, f64)>> {
        if self.decoded >= self.count {
            return Ok(None);
        }

        if self.decoded == 0 {
            return self.decode_first();
        }

        let timestamp = self.decode_timestamp()?;
        let value = self.decode_value()?;
        self.decoded += 1;

        Ok(Some((timestamp, value)))
    }

    fn decode_first(&mut self) -> Result<Option<(i64, f64)>> {
        let timestamp = self.reader.read_bits(64)
            .ok_or_else(|| FluxError::Compression("Unexpected end of data".into()))? as i64;
        
        let value_bits = self.reader.read_bits(64)
            .ok_or_else(|| FluxError::Compression("Unexpected end of data".into()))?;
        
        self.prev_timestamp = timestamp;
        self.prev_value_bits = value_bits;
        self.decoded = 1;
        
        Ok(Some((timestamp, f64::from_bits(value_bits))))
    }

    fn decode_timestamp(&mut self) -> Result<i64> {
        let first_bit = self.reader.read_bit()
            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
        
        let delta_of_delta = if !first_bit {
            // '0' - same delta
            0
        } else {
            let second_bit = self.reader.read_bit()
                .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
            
            if !second_bit {
                // '10' - 7 bit delta_of_delta
                let v = self.reader.read_bits(7)
                    .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
                v as i64 - 63
            } else {
                let third_bit = self.reader.read_bit()
                    .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
                
                if !third_bit {
                    // '110' - 9 bit delta_of_delta
                    let v = self.reader.read_bits(9)
                        .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
                    v as i64 - 255
                } else {
                    let fourth_bit = self.reader.read_bit()
                        .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
                    
                    if !fourth_bit {
                        // '1110' - 12 bit delta_of_delta
                        let v = self.reader.read_bits(12)
                            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
                        v as i64 - 2047
                    } else {
                        // '1111' - 64 bit delta_of_delta
                        self.reader.read_bits(64)
                            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))? as i64
                    }
                }
            }
        };
        
        let delta = self.prev_timestamp_delta + delta_of_delta;
        let timestamp = self.prev_timestamp + delta;
        
        self.prev_timestamp_delta = delta;
        self.prev_timestamp = timestamp;
        
        Ok(timestamp)
    }

    fn decode_value(&mut self) -> Result<f64> {
        let first_bit = self.reader.read_bit()
            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
        
        if !first_bit {
            // Same value
            return Ok(f64::from_bits(self.prev_value_bits));
        }
        
        let second_bit = self.reader.read_bit()
            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
        
        let (leading_zeros, meaningful_bits) = if !second_bit {
            // Use previous window
            let meaningful_bits = 64 - self.prev_leading_zeros - self.prev_trailing_zeros;
            (self.prev_leading_zeros, meaningful_bits)
        } else {
            // New window
            let leading = self.reader.read_bits(5)
                .ok_or_else(|| FluxError::Compression("Unexpected end".into()))? as u32;
            let meaningful = self.reader.read_bits(6)
                .ok_or_else(|| FluxError::Compression("Unexpected end".into()))? as u32;
            
            self.prev_leading_zeros = leading;
            self.prev_trailing_zeros = 64 - leading - meaningful;
            
            (leading, meaningful)
        };
        
        let meaningful_value = self.reader.read_bits(meaningful_bits as usize)
            .ok_or_else(|| FluxError::Compression("Unexpected end".into()))?;
        
        let trailing_zeros = 64 - leading_zeros - meaningful_bits;
        let xor = meaningful_value << trailing_zeros;
        let value_bits = self.prev_value_bits ^ xor;
        
        self.prev_value_bits = value_bits;
        
        Ok(f64::from_bits(value_bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::GorillaEncoder;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut encoder = GorillaEncoder::new();
        
        let points: Vec<(i64, f64)> = (0..100)
            .map(|i| (1000000000 + i * 10_000_000_000, 20.0 + i as f64 * 0.5))
            .collect();
        
        for (ts, val) in &points {
            encoder.encode(*ts, *val);
        }
        
        let block = encoder.finish();
        let mut decoder = GorillaDecoder::new(&block.data, block.count);
        let decoded = decoder.decode_all().unwrap();
        
        assert_eq!(decoded.len(), points.len());
        for (i, ((orig_ts, orig_val), (dec_ts, dec_val))) in points.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(orig_ts, dec_ts, "Timestamp mismatch at {}", i);
            assert!((orig_val - dec_val).abs() < 1e-10, "Value mismatch at {}: {} vs {}", i, orig_val, dec_val);
        }
    }

    #[test]
    fn test_decode_constant_values() {
        let mut encoder = GorillaEncoder::new();
        
        for i in 0..50 {
            encoder.encode(1000000000 + i * 10_000_000_000, 42.0);
        }
        
        let block = encoder.finish();
        let mut decoder = GorillaDecoder::new(&block.data, block.count);
        let decoded = decoder.decode_all().unwrap();
        
        assert_eq!(decoded.len(), 50);
        for (ts, val) in decoded {
            assert!((val - 42.0).abs() < 1e-10);
            assert!(ts >= 1000000000);
        }
    }
}
