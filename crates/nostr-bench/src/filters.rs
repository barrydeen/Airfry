//! Filter construction helpers for the REQ benchmark.
//!
//! Filters are built as compact JSON strings (the relay parses them itself) and mirrored as
//! `nostr_core::Filter` values so the direct-store benchmark can use the same shapes.

use nostr_core::Filter;
use serde_json::{json, Value};

/// A named filter shape used by the REQ benchmark.
#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub name: &'static str,
    pub json: String,
    pub filter: Filter,
}

fn spec(name: &'static str, v: Value) -> QuerySpec {
    let filter = Filter::from_json_value(&v).expect("bench filter shapes are valid");
    QuerySpec {
        name,
        json: serde_json::to_string(&v).expect("filter serializes"),
        filter,
    }
}

/// Parse a user-supplied filter JSON string into a `QuerySpec`.
pub fn parse_filter(json: &str) -> Result<QuerySpec, String> {
    let v: Value = serde_json::from_str(json).map_err(|e| format!("invalid --filter JSON: {e}"))?;
    let filter = Filter::from_json_value(&v).map_err(|e| format!("invalid --filter: {e}"))?;
    Ok(QuerySpec {
        name: "custom",
        json: serde_json::to_string(&v).unwrap_or_else(|_| json.to_string()),
        filter,
    })
}

/// The default REQ benchmark filter: recent kind-1 notes with a bounded limit.
pub fn default_filter() -> QuerySpec {
    spec("kinds=[1] limit=100", json!({"kinds": [1], "limit": 100}))
}

/// A rotating set of realistic filter shapes, used by `req` and `mixed`.
///
/// `authors` and `topics` are derived from the generated corpus so every query hits real data.
pub fn rotating_filters(authors: &[String], topics: &[String]) -> Vec<QuerySpec> {
    let mut out = Vec::new();
    out.push(spec(
        "kinds=[1] limit=100",
        json!({"kinds": [1], "limit": 100}),
    ));
    out.push(spec(
        "kinds=[1,7] limit=50",
        json!({"kinds": [1, 7], "limit": 50}),
    ));
    if let Some(a) = authors.first() {
        out.push(spec(
            "authors=[a0] limit=100",
            json!({"authors": [a], "limit": 100}),
        ));
    }
    if authors.len() > 1 {
        out.push(spec(
            "authors=[a0,a1] limit=100",
            json!({"authors": [authors[0], authors[1]], "limit": 100}),
        ));
    }
    if let Some(t) = topics.first() {
        out.push(spec(
            "#t=[topic0] limit=100",
            json!({"#t": [t], "limit": 100}),
        ));
    }
    out.push(spec(
        "kinds=[1] limit=1 (newest)",
        json!({"kinds": [1], "limit": 1}),
    ));
    out.push(spec(
        "kinds=[1] limit=500",
        json!({"kinds": [1], "limit": 500}),
    ));
    out
}

/// Topic tag values matching `pool::TOPIC_COUNT`.
pub fn topic_values() -> Vec<String> {
    (0..crate::pool::TOPIC_COUNT)
        .map(|i| format!("topic{i}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filter_is_valid() {
        let f = default_filter();
        assert_eq!(f.filter.kinds, Some(vec![1]));
        assert_eq!(f.filter.limit, Some(100));
        assert!(f.json.contains("\"kinds\":[1]"));
    }

    #[test]
    fn parse_filter_roundtrip() {
        let f = parse_filter(r#"{"authors":["0000000000000000000000000000000000000000000000000000000000000001"],"limit":5}"#).unwrap();
        assert_eq!(f.filter.limit, Some(5));
        assert_eq!(f.filter.authors.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn parse_filter_rejects_garbage() {
        assert!(parse_filter("not json").is_err());
        assert!(parse_filter(r#"{"kinds":"nope"}"#).is_err());
    }

    #[test]
    fn rotating_filters_are_all_valid() {
        let authors = vec![
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
        ];
        let topics = topic_values();
        let specs = rotating_filters(&authors, &topics);
        assert!(specs.len() >= 6);
        for s in &specs {
            assert!(!s.json.is_empty());
            assert!(!s.filter.is_empty());
        }
        assert_eq!(topics.len(), crate::pool::TOPIC_COUNT as usize);
    }

    #[test]
    fn rotating_filters_without_authors() {
        let specs = rotating_filters(&[], &topic_values());
        assert!(specs.iter().all(|s| !s.json.is_empty()));
    }
}
