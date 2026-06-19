#![allow(dead_code)]

use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
    pub workout_metadata_entries: u64,
    pub workout_routes: u64,
    /// Maps `FileReference` path (verbatim, including the leading
    /// `/workout-routes/` prefix Apple emits) to the `workout_hash` of the
    /// owning `<Workout>`. Built while we already hold the workout in scope
    /// during the single XML scan, replacing the legacy second scan in
    /// `import::build_workout_route_map`. The map is consumed by the GPX
    /// importer so route points land on the same `workout_hash` value the
    /// XML importer produced for the workout.
    pub workout_route_map: HashMap<String, String>,
    /// Maps `workout_hash` to the workout's `startDate` UTC offset in
    /// minutes east of UTC (e.g. 540 for `+0900`, -420 for `-0700`). The
    /// GPX importer consults this to shift true-UTC route timestamps onto
    /// the same local-time basis the XML/ECG importers already use for
    /// every other date column. Workouts without a parseable offset on
    /// `startDate` are absent from the map; the GPX importer falls back to
    /// the legacy `Z`-strip behavior in that case.
    pub workout_offset_map: HashMap<String, i32>,
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
        assert_eq!(stats.workout_metadata_entries, 0);
        assert_eq!(stats.workout_routes, 0);
    }
}
