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
