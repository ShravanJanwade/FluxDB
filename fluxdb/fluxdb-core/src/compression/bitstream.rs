//! Bit-level I/O for Gorilla compression

/// Bit writer for encoding compressed data
#[derive(Debug)]
pub struct BitWriter {
    buffer: Vec<u8>,
    current_byte: u8,
    bit_position: u8,
}

impl BitWriter {
    /// Create a new BitWriter
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_byte: 0,
            bit_position: 0,
        }
    }

    /// Create with capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            current_byte: 0,
            bit_position: 0,
        }
    }

    /// Write a single bit
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_position);
        }
        self.bit_position += 1;

        if self.bit_position == 8 {
            self.buffer.push(self.current_byte);
            self.current_byte = 0;
            self.bit_position = 0;
        }
    }

    /// Write multiple bits from a u64 value
    #[inline]
    pub fn write_bits(&mut self, value: u64, num_bits: usize) {
        debug_assert!(num_bits <= 64);
        
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    /// Finish writing and return the buffer
    pub fn finish(mut self) -> Vec<u8> {
        if self.bit_position > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }

    /// Get current size in bytes
    pub fn len(&self) -> usize {
        self.buffer.len() + if self.bit_position > 0 { 1 } else { 0 }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.bit_position == 0
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Bit reader for decoding compressed data
#[derive(Debug)]
pub struct BitReader<'a> {
    data: &'a [u8],
    byte_position: usize,
    bit_position: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new BitReader
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_position: 0,
            bit_position: 0,
        }
    }

    /// Read a single bit
    #[inline]
    pub fn read_bit(&mut self) -> Option<bool> {
        if self.byte_position >= self.data.len() {
            return None;
        }

        let bit = (self.data[self.byte_position] >> (7 - self.bit_position)) & 1 == 1;
        self.bit_position += 1;

        if self.bit_position == 8 {
            self.byte_position += 1;
            self.bit_position = 0;
        }

        Some(bit)
    }

    /// Read multiple bits as a u64
    #[inline]
    pub fn read_bits(&mut self, num_bits: usize) -> Option<u64> {
        debug_assert!(num_bits <= 64);
        
        let mut value = 0u64;
        for _ in 0..num_bits {
            let bit = self.read_bit()?;
            value = (value << 1) | (bit as u64);
        }
        Some(value)
    }

    /// Check if there are more bits to read
    pub fn has_more(&self) -> bool {
        self.byte_position < self.data.len()
    }

    /// Get the current position in bits
    pub fn position(&self) -> usize {
        self.byte_position * 8 + self.bit_position as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_writer_reader() {
        let mut writer = BitWriter::new();
        
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bits(0b1010_1010, 8);
        writer.write_bits(0xFF, 8);

        let data = writer.finish();
        
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bit(), Some(false));
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bits(8), Some(0b1010_1010));
        assert_eq!(reader.read_bits(8), Some(0xFF));
    }

    #[test]
    fn test_write_read_various_sizes() {
        let mut writer = BitWriter::new();
        
        // Write values of various bit lengths
        writer.write_bits(0b111, 3);       // 3 bits
        writer.write_bits(0b10101, 5);     // 5 bits
        writer.write_bits(0xABCD, 16);     // 16 bits
        writer.write_bits(0xDEADBEEF, 32); // 32 bits

        let data = writer.finish();
        
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_bits(3), Some(0b111));
        assert_eq!(reader.read_bits(5), Some(0b10101));
        assert_eq!(reader.read_bits(16), Some(0xABCD));
        assert_eq!(reader.read_bits(32), Some(0xDEADBEEF));
    }
}
