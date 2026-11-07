//! Result reporting: machine-readable JSON and human-readable markdown tables.

use crate::stats::LatencySummary;
use serde::Serialize;

/// Latency distribution in milliseconds, suitable for JSON output and markdown tables.
#[derive(Serialize, Clone, Debug, Default, PartialEq)]
pub struct LatencyReport {
    pub count: usize,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

impl From<&LatencySummary> for LatencyReport {
    fn from(s: &LatencySummary) -> LatencyReport {
        LatencyReport {
            count: s.count,
            p50_ms: round3(s.p50_ms()),
            p99_ms: round3(s.p99_ms()),
            p999_ms: round3(s.p999_ms()),
            max_ms: round3(s.max_ms()),
            mean_ms: round3(s.mean_ms()),
        }
    }
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Print a serializable value as pretty JSON on stdout.
pub fn print_json<T: Serialize>(v: &T) {
    match serde_json::to_string_pretty(v) {
        Ok(s) => println!("{s}"),
        Err(e) => println!("{{\"error\":\"failed to serialize result: {e}\"}}"),
    }
}

/// Print a markdown table row set for a benchmark result.
pub fn print_table(title: &str, rows: &[(&str, String)]) {
    println!("## {title}");
    println!();
    println!("| metric | value |");
    println!("| --- | --- |");
    for (k, v) in rows {
        println!("| {k} | {v} |");
    }
    println!();
}

/// Format a float with 2 decimals.
pub fn f2(v: f64) -> String {
    format!("{v:.2}")
}

/// Format a float with 3 decimals.
pub fn f3(v: f64) -> String {
    format!("{v:.3}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_report_conversion() {
        let s = LatencySummary {
            count: 100,
            p50_us: 1_000,
            p99_us: 2_500,
            p999_us: 5_000,
            max_us: 9_000,
            mean_us: 1_200,
        };
        let r = LatencyReport::from(&s);
        assert_eq!(r.count, 100);
        assert_eq!(r.p50_ms, 1.0);
        assert_eq!(r.p99_ms, 2.5);
        assert_eq!(r.p999_ms, 5.0);
        assert_eq!(r.max_ms, 9.0);
        assert_eq!(r.mean_ms, 1.2);
    }

    #[test]
    fn latency_report_serializes() {
        let r = LatencyReport::from(&LatencySummary::default());
        let j = serde_json::to_string(&r).unwrap();
        assert!(j.contains("\"p99_ms\":0.0"));
    }

    #[test]
    fn formatting_helpers() {
        assert_eq!(f2(1.234), "1.23");
        assert_eq!(f3(1.2345), "1.234");
    }
}
