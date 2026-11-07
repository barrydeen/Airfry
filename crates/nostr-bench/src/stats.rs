//! Latency statistics used by every benchmark subcommand.
//!
//! All samples are stored in microseconds so that sub-millisecond latencies (which is what a
//! fast relay should produce) are still meaningful. Percentiles use the nearest-rank method,
//! which is the standard way to report tail latency for load tests.

/// A summarised latency distribution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LatencySummary {
    pub count: usize,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub mean_us: u64,
}

impl LatencySummary {
    pub fn p50_ms(&self) -> f64 {
        self.p50_us as f64 / 1000.0
    }
    pub fn p99_ms(&self) -> f64 {
        self.p99_us as f64 / 1000.0
    }
    pub fn p999_ms(&self) -> f64 {
        self.p999_us as f64 / 1000.0
    }
    pub fn max_ms(&self) -> f64 {
        self.max_us as f64 / 1000.0
    }
    pub fn mean_ms(&self) -> f64 {
        self.mean_us as f64 / 1000.0
    }
}

/// Nearest-rank percentile over an ascending-sorted slice of microsecond samples.
///
/// Returns 0 for an empty slice. `p` is a percentage in `(0, 100]`.
pub fn percentile_us(sorted_us: &[u64], p: f64) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    let p = p.clamp(0.0, 100.0);
    let n = sorted_us.len() as f64;
    let rank = (p / 100.0 * n).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted_us.len() - 1);
    sorted_us[idx]
}

/// Summarise a set of latency samples (microseconds). The input is consumed and sorted.
pub fn summarize(mut samples_us: Vec<u64>) -> LatencySummary {
    if samples_us.is_empty() {
        return LatencySummary::default();
    }
    samples_us.sort_unstable();
    let count = samples_us.len();
    let sum: u128 = samples_us.iter().map(|v| *v as u128).sum();
    LatencySummary {
        count,
        p50_us: percentile_us(&samples_us, 50.0),
        p99_us: percentile_us(&samples_us, 99.0),
        p999_us: percentile_us(&samples_us, 99.9),
        max_us: *samples_us.last().unwrap(),
        mean_us: (sum / count as u128) as u64,
    }
}

/// Convert a `std::time::Duration` to whole microseconds, saturating.
pub fn dur_us(d: std::time::Duration) -> u64 {
    d.as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_nearest_rank() {
        let samples: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile_us(&samples, 50.0), 50);
        assert_eq!(percentile_us(&samples, 99.0), 99);
        assert_eq!(percentile_us(&samples, 99.9), 100);
        assert_eq!(percentile_us(&samples, 100.0), 100);
        assert_eq!(percentile_us(&samples, 1.0), 1);
    }

    #[test]
    fn percentile_single_and_empty() {
        assert_eq!(percentile_us(&[], 99.0), 0);
        assert_eq!(percentile_us(&[42], 50.0), 42);
        assert_eq!(percentile_us(&[42], 99.9), 42);
    }

    #[test]
    fn summarize_reports_tail() {
        let mut samples = vec![10u64; 990];
        samples.extend(std::iter::repeat(5_000).take(10));
        let s = summarize(samples);
        assert_eq!(s.count, 1000);
        assert_eq!(s.p50_us, 10);
        assert_eq!(s.p99_us, 10);
        assert_eq!(s.p999_us, 5_000);
        assert_eq!(s.max_us, 5_000);
        assert!(s.mean_us > 10 && s.mean_us < 5_000);
    }

    #[test]
    fn summarize_empty_is_zero() {
        let s = summarize(vec![]);
        assert_eq!(s, LatencySummary::default());
        assert_eq!(s.p99_ms(), 0.0);
    }

    #[test]
    fn duration_conversion() {
        assert_eq!(dur_us(std::time::Duration::from_micros(1234)), 1234);
        assert_eq!(dur_us(std::time::Duration::from_millis(2)), 2000);
    }
}
