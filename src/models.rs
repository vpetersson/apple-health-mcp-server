#![allow(dead_code)]

use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
pub struct ImportStats {
    pub records: u64,
    pub workouts: u64,
    pub activity_summaries: u64,
    pub correlations: u64,
    pub ecg_readings: u64,
    pub route_points: u64,
    pub metadata_entries: u64,
    pub workout_events: u64,
    pub workout_statistics: u64,
}

pub fn compute_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"|");
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_hash_deterministic() {
        let h1 = compute_hash(&["a", "b", "c"]);
        let h2 = compute_hash(&["a", "b", "c"]);
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_hash_different_order() {
        let h1 = compute_hash(&["a", "b"]);
        let h2 = compute_hash(&["b", "a"]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn compute_hash_empty_input() {
        let h = compute_hash(&[]);
        assert!(!h.is_empty());
    }

    #[test]
    fn compute_hash_empty_strings() {
        let h1 = compute_hash(&[""]);
        let h2 = compute_hash(&["", ""]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn compute_hash_hex_output() {
        let h = compute_hash(&["test"]);
        assert_eq!(h.len(), 64); // SHA-256 = 32 bytes = 64 hex chars
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn import_stats_default() {
        let stats = ImportStats::default();
        assert_eq!(stats.records, 0);
        assert_eq!(stats.workouts, 0);
        assert_eq!(stats.activity_summaries, 0);
        assert_eq!(stats.correlations, 0);
        assert_eq!(stats.ecg_readings, 0);
        assert_eq!(stats.route_points, 0);
        assert_eq!(stats.metadata_entries, 0);
        assert_eq!(stats.workout_events, 0);
        assert_eq!(stats.workout_statistics, 0);
    }
}
