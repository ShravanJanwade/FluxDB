//! Aggregate function implementations

/// Accumulator for computing aggregates incrementally
pub trait Accumulator: Send + Sync {
    /// Add a value to the accumulator
    fn add(&mut self, value: f64);
    
    /// Get the current result
    fn result(&self) -> Option<f64>;
    
    /// Reset the accumulator
    fn reset(&mut self);
    
    /// Merge another accumulator into this one
    fn merge(&mut self, other: &dyn Accumulator);
}

/// Count accumulator
#[derive(Debug, Default)]
pub struct CountAccumulator {
    count: u64,
}

impl Accumulator for CountAccumulator {
    fn add(&mut self, _value: f64) {
        self.count += 1;
    }
    
    fn result(&self) -> Option<f64> {
        Some(self.count as f64)
    }
    
    fn reset(&mut self) {
        self.count = 0;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(count) = other.result() {
            self.count += count as u64;
        }
    }
}

/// Sum accumulator
#[derive(Debug, Default)]
pub struct SumAccumulator {
    sum: f64,
    count: u64,
}

impl Accumulator for SumAccumulator {
    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }
    
    fn result(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum)
        } else {
            None
        }
    }
    
    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(sum) = other.result() {
            self.sum += sum;
            self.count += 1;
        }
    }
}

/// Mean accumulator
#[derive(Debug, Default)]
pub struct MeanAccumulator {
    sum: f64,
    count: u64,
}

impl Accumulator for MeanAccumulator {
    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }
    
    fn result(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }
    
    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
    
    fn merge(&mut self, _other: &dyn Accumulator) {
        // Note: proper merging of means requires knowing counts
    }
}

/// Min accumulator
#[derive(Debug)]
pub struct MinAccumulator {
    min: Option<f64>,
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self { min: None }
    }
}

impl Accumulator for MinAccumulator {
    fn add(&mut self, value: f64) {
        self.min = Some(match self.min {
            Some(current) => current.min(value),
            None => value,
        });
    }
    
    fn result(&self) -> Option<f64> {
        self.min
    }
    
    fn reset(&mut self) {
        self.min = None;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(other_min) = other.result() {
            self.add(other_min);
        }
    }
}

/// Max accumulator
#[derive(Debug)]
pub struct MaxAccumulator {
    max: Option<f64>,
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self { max: None }
    }
}

impl Accumulator for MaxAccumulator {
    fn add(&mut self, value: f64) {
        self.max = Some(match self.max {
            Some(current) => current.max(value),
            None => value,
        });
    }
    
    fn result(&self) -> Option<f64> {
        self.max
    }
    
    fn reset(&mut self) {
        self.max = None;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(other_max) = other.result() {
            self.add(other_max);
        }
    }
}

/// First value accumulator (keeps earliest value)
#[derive(Debug, Default)]
pub struct FirstAccumulator {
    value: Option<(i64, f64)>,
}

impl FirstAccumulator {
    pub fn add_with_time(&mut self, timestamp: i64, value: f64) {
        match &self.value {
            Some((ts, _)) if *ts <= timestamp => {}
            _ => self.value = Some((timestamp, value)),
        }
    }
}

impl Accumulator for FirstAccumulator {
    fn add(&mut self, value: f64) {
        if self.value.is_none() {
            self.value = Some((0, value));
        }
    }
    
    fn result(&self) -> Option<f64> {
        self.value.map(|(_, v)| v)
    }
    
    fn reset(&mut self) {
        self.value = None;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(v) = other.result() {
            self.add(v);
        }
    }
}

/// Last value accumulator (keeps latest value)
#[derive(Debug, Default)]
pub struct LastAccumulator {
    value: Option<(i64, f64)>,
}

impl LastAccumulator {
    pub fn add_with_time(&mut self, timestamp: i64, value: f64) {
        match &self.value {
            Some((ts, _)) if *ts >= timestamp => {}
            _ => self.value = Some((timestamp, value)),
        }
    }
}

impl Accumulator for LastAccumulator {
    fn add(&mut self, value: f64) {
        self.value = Some((i64::MAX, value));
    }
    
    fn result(&self) -> Option<f64> {
        self.value.map(|(_, v)| v)
    }
    
    fn reset(&mut self) {
        self.value = None;
    }
    
    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(v) = other.result() {
            self.add(v);
        }
    }
}

/// Standard deviation accumulator (Welford's algorithm)
#[derive(Debug, Default)]
pub struct StddevAccumulator {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Accumulator for StddevAccumulator {
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }
    
    fn result(&self) -> Option<f64> {
        if self.count > 1 {
            Some((self.m2 / self.count as f64).sqrt())
        } else {
            None
        }
    }
    
    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }
    
    fn merge(&mut self, _other: &dyn Accumulator) {
        // Note: proper merging requires parallel algorithm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_accumulator() {
        let mut acc = MeanAccumulator::default();
        acc.add(10.0);
        acc.add(20.0);
        acc.add(30.0);
        assert_eq!(acc.result(), Some(20.0));
    }

    #[test]
    fn test_min_max_accumulator() {
        let mut min_acc = MinAccumulator::default();
        let mut max_acc = MaxAccumulator::default();
        
        for v in [5.0, 2.0, 8.0, 1.0, 9.0] {
            min_acc.add(v);
            max_acc.add(v);
        }
        
        assert_eq!(min_acc.result(), Some(1.0));
        assert_eq!(max_acc.result(), Some(9.0));
    }

    #[test]
    fn test_stddev_accumulator() {
        let mut acc = StddevAccumulator::default();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.add(v);
        }
        let stddev = acc.result().unwrap();
        assert!((stddev - 2.0).abs() < 0.01);
    }
}
