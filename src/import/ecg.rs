use anyhow::{Context, Result};
use duckdb::Connection;
use std::fs;
use std::path::Path;
use tracing::info;

use crate::models::compute_hash;

pub fn import_ecg_files(conn: &Connection, ecg_dir: &Path, import_id: &str) -> Result<u64> {
    if !ecg_dir.exists() {
        info!("No electrocardiograms directory found, skipping ECG import");
        return Ok(0);
    }

    let mut count = 0u64;
    let mut entries: Vec<_> = fs::read_dir(ecg_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "csv"))
        .collect();
    entries.sort_by_key(|e| e.path());

    for entry in &entries {
        let path = entry.path();
        match import_single_ecg(conn, &path, import_id) {
            Ok(_) => count += 1,
            Err(e) => {
                tracing::warn!("Failed to import ECG file {:?}: {:?}", path, e);
            }
        }
    }

    info!("Imported {} ECG recordings", count);
    Ok(count)
}

pub(crate) fn import_single_ecg(conn: &Connection, path: &Path, import_id: &str) -> Result<()> {
    let content = fs::read_to_string(path).context("Failed to read ECG file")?;
    let mut lines = content.lines();

    // Parse header fields
    let mut recorded_date = String::new();
    let mut classification = None;
    let mut device = None;
    let mut sample_rate_hz: Option<f64> = None;
    let mut symptoms = None;
    let mut software_version = None;

    // Header lines are "Key,Value" pairs
    for line in lines.by_ref() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Check if this is a header line (contains a known key)
        if line.starts_with("Name,") {
            // Skip name for privacy
            continue;
        } else if line.starts_with("Date of Birth,") {
            // Skip DOB for privacy
            continue;
        } else if line.starts_with("Recorded Date,") {
            let raw = line.strip_prefix("Recorded Date,").unwrap_or("");
            // Strip timezone suffix " +0000"
            recorded_date = if let Some(pos) = raw.rfind(" +") {
                raw[..pos].to_string()
            } else if let Some(pos) = raw.rfind(" -") {
                raw[..pos].to_string()
            } else {
                raw.to_string()
            };
        } else if line.starts_with("Classification,") {
            classification = Some(
                line.strip_prefix("Classification,")
                    .unwrap_or("")
                    .to_string(),
            );
        } else if line.starts_with("Symptoms,") {
            let s = line.strip_prefix("Symptoms,").unwrap_or("").to_string();
            if !s.is_empty() {
                symptoms = Some(s);
            }
        } else if line.starts_with("Software Version,") {
            software_version = Some(
                line.strip_prefix("Software Version,")
                    .unwrap_or("")
                    .to_string(),
            );
        } else if line.starts_with("Device,") {
            let d = line.strip_prefix("Device,").unwrap_or("").to_string();
            // Remove surrounding quotes
            device = Some(d.trim_matches('"').to_string());
        } else if line.starts_with("Sample Rate,") {
            let sr_str = line.strip_prefix("Sample Rate,").unwrap_or("");
            // Extract numeric part: "513.992 hertz" -> 513.992
            sample_rate_hz = sr_str
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok());
        } else if line.starts_with("Lead,") || line.starts_with("Unit,") {
            // Skip these header lines
            continue;
        } else {
            // First non-header line - this should be voltage data
            // We need to parse from here
            break;
        }
    }

    if recorded_date.is_empty() {
        anyhow::bail!("No recorded date found in ECG file");
    }

    let ecg_hash = compute_hash(&[&recorded_date, device.as_deref().unwrap_or("")]);

    // Insert ECG reading using Appender
    {
        let mut appender = conn.appender("ecg_readings")?;
        appender.append_row(duckdb::params![
            ecg_hash,
            recorded_date,
            classification,
            device,
            sample_rate_hz,
            symptoms,
            software_version,
            import_id,
        ])?;
        appender.flush()?;
    }

    // Parse voltage samples using Appender
    let content = fs::read_to_string(path)?;
    let mut in_data = false;
    let mut sample_idx = 0i32;
    let mut appender = conn.appender("ecg_samples")?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(voltage) = line.parse::<f64>() {
            in_data = true;
            appender.append_row(duckdb::params![ecg_hash, sample_idx, voltage])?;
            sample_idx += 1;
        } else if in_data {
            break;
        }
    }
    appender.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ensure_schema, open_db_in_memory};

    const MINIMAL_ECG_CSV: &str = "Name,Test User
Date of Birth,1990-01-01
Recorded Date,2024-06-15 10:30:00 +0000
Classification,Sinus Rhythm
Symptoms,None
Software Version,2.0
Device,\"Apple Watch\"
Sample Rate,512.000 Hz
Lead,Lead I
Unit,µV

100
200
-50
150
75";

    #[test]
    fn import_single_ecg_minimal() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let ecg_path = dir.path().join("ecg_2024.csv");
        std::fs::write(&ecg_path, MINIMAL_ECG_CSV).unwrap();

        import_single_ecg(&conn, &ecg_path, "test_import").unwrap();

        let reading_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
            .unwrap();
        assert_eq!(reading_count, 1);

        let sample_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_samples", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sample_count, 5);

        let classification: String = conn
            .query_row(
                "SELECT classification FROM ecg_readings LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(classification, "Sinus Rhythm");
    }

    #[test]
    fn import_ecg_missing_date() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let csv = "Name,Test\nClassification,Normal\n\n100\n200\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_ecg.csv");
        std::fs::write(&path, csv).unwrap();

        let result = import_single_ecg(&conn, &path, "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No recorded date"));
    }

    #[test]
    fn import_ecg_files_missing_dir() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let missing = std::path::PathBuf::from("/nonexistent/path/ecgs");
        let count = import_ecg_files(&conn, &missing, "test").unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn import_ecg_files_with_csvs() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ecg1.csv"), MINIMAL_ECG_CSV).unwrap();

        let count = import_ecg_files(&conn, dir.path(), "test").unwrap();
        assert_eq!(count, 1);
    }
}
