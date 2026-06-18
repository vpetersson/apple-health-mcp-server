use anyhow::{Context, Result};
use duckdb::Connection;
use std::fs;
use std::path::Path;
use tracing::info;

use crate::models::compute_hash;

// Localized header labels emitted by Apple Health ECG CSV export.
//
// Apple Watch writes the ECG CSV in the watch's current display language.
// The English labels are authoritative (taken from a US English watch);
// the Japanese labels are verified against a real export. Labels for other
// locales below are best-effort and may need correction from contributors
// with watches set to those locales — file an issue if a label is wrong.
//
// Each constant lists every label that should map to the same field. The
// matcher tries them in order; the first prefix match wins.

const RECORDED_DATE_LABELS: &[&str] = &[
    "Recorded Date",
    "記録日時", // ja, verified
    // unverified below
    "记录日期", // zh-Hans
    "記錄日期", // zh-Hant
    "기록 일시", // ko
];

const CLASSIFICATION_LABELS: &[&str] = &[
    "Classification",
    "分類", // ja (verified) / zh-Hant (unverified) — same glyphs
    "分类", // zh-Hans (unverified)
    "분류", // ko (unverified)
];

const SYMPTOMS_LABELS: &[&str] = &[
    "Symptoms",
    "症状", // ja (verified) / zh-Hans (unverified) — same glyphs
    "症狀", // zh-Hant (unverified)
    "증상", // ko (unverified)
];

const SOFTWARE_VERSION_LABELS: &[&str] = &[
    "Software Version",
    // Apple Watch UI string per locale (unverified for ECG CSV specifically)
    "ソフトウェアバージョン", // ja
    "软件版本",                // zh-Hans
    "軟體版本",                // zh-Hant
    "소프트웨어 버전",         // ko
];

const DEVICE_LABELS: &[&str] = &[
    "Device",
    "デバイス", // ja
    "设备",     // zh-Hans
    "裝置",     // zh-Hant
    "기기",     // ko
];

const SAMPLE_RATE_LABELS: &[&str] = &[
    "Sample Rate",
    "サンプルレート", // ja
    "采样率",         // zh-Hans
    "取樣率",         // zh-Hant
    "샘플 레이트",    // ko
];

// Labels for fields that should be skipped (privacy, or irrelevant metadata).
const NAME_LABELS: &[&str] = &["Name", "名前", "姓名", "이름"];
const DOB_LABELS: &[&str] = &["Date of Birth", "生年月日", "出生日期", "출생일"];
const LEAD_LABELS: &[&str] = &["Lead", "リード", "导联", "導程", "리드"];
const UNIT_LABELS: &[&str] = &["Unit", "単位", "单位", "單位", "단위"];

/// If any label in `labels` matches the start of `line` followed by ',',
/// return the value after that comma. Uses `str::strip_prefix` so that label
/// lengths in bytes never need to align with UTF-8 char boundaries on `line`.
fn match_header<'a>(line: &'a str, labels: &[&str]) -> Option<&'a str> {
    labels
        .iter()
        .find_map(|label| line.strip_prefix(*label)?.strip_prefix(','))
}

/// Strip a UTF-8 BOM (0xEF 0xBB 0xBF) from the start of `s` if present.
/// Apple Health ECG CSVs from some locales are saved with a BOM.
fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

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
    let content = strip_bom(&content);
    let mut lines = content.lines();

    // Parse header fields
    let mut recorded_date = String::new();
    let mut classification = None;
    let mut device = None;
    let mut sample_rate_hz: Option<f64> = None;
    let mut symptoms = None;
    let mut software_version = None;

    // Header lines are "Key,Value" pairs. Labels are localized to the watch's
    // display language; see the *_LABELS constants at the top of this file.
    for line in lines.by_ref() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if match_header(line, NAME_LABELS).is_some()
            || match_header(line, DOB_LABELS).is_some()
        {
            // Skip name and date of birth for privacy.
            continue;
        } else if let Some(raw) = match_header(line, RECORDED_DATE_LABELS) {
            // Strip the timezone suffix (" +0000" / " -0500") so the value
            // fits a naive TIMESTAMP column. (#4 will preserve the offset.)
            recorded_date = if let Some(pos) = raw.rfind(" +") {
                raw[..pos].to_string()
            } else if let Some(pos) = raw.rfind(" -") {
                raw[..pos].to_string()
            } else {
                raw.to_string()
            };
        } else if let Some(raw) = match_header(line, CLASSIFICATION_LABELS) {
            classification = Some(raw.to_string());
        } else if let Some(raw) = match_header(line, SYMPTOMS_LABELS) {
            if !raw.is_empty() {
                symptoms = Some(raw.to_string());
            }
        } else if let Some(raw) = match_header(line, SOFTWARE_VERSION_LABELS) {
            software_version = Some(raw.to_string());
        } else if let Some(raw) = match_header(line, DEVICE_LABELS) {
            // Remove surrounding quotes that Apple writes around the device
            // string (e.g. "Apple Watch").
            device = Some(raw.trim_matches('"').to_string());
        } else if let Some(raw) = match_header(line, SAMPLE_RATE_LABELS) {
            // Extract the numeric part: "513.992 hertz" -> 513.992.
            sample_rate_hz = raw.split_whitespace().next().and_then(|s| s.parse().ok());
        } else if match_header(line, LEAD_LABELS).is_some()
            || match_header(line, UNIT_LABELS).is_some()
        {
            // Skip these informational header lines.
            continue;
        } else {
            // First unrecognized line; assume the voltage section starts here.
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

    // Japanese-locale ECG CSV — header labels are the verified strings emitted
    // by a Japanese-language Apple Watch. Tests that the i18n matcher accepts
    // them.
    const JAPANESE_ECG_CSV: &str = "名前,テスト ユーザー
生年月日,1990-01-01
記録日時,2024-06-15 10:30:00 +0900
分類,洞調律
症状,なし
ソフトウェアバージョン,10.0
デバイス,\"Apple Watch\"
サンプルレート,512.000 Hz
リード,Lead I
単位,µV

100
200
-50
150
75";

    #[test]
    fn import_single_ecg_japanese_locale() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let ecg_path = dir.path().join("ecg_ja.csv");
        std::fs::write(&ecg_path, JAPANESE_ECG_CSV).unwrap();

        import_single_ecg(&conn, &ecg_path, "test_import").unwrap();

        let reading_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
            .unwrap();
        assert_eq!(reading_count, 1);

        let (classification, symptoms, device, sample_rate): (String, String, String, f64) = conn
            .query_row(
                "SELECT classification, symptoms, device, sample_rate_hz FROM ecg_readings LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(classification, "洞調律");
        assert_eq!(symptoms, "なし");
        assert_eq!(device, "Apple Watch");
        assert!((sample_rate - 512.0).abs() < 0.01);

        let sample_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_samples", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sample_count, 5);
    }

    #[test]
    fn import_single_ecg_with_utf8_bom() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        // Prepend a UTF-8 BOM (0xEF 0xBB 0xBF).
        let csv_with_bom = format!("\u{feff}{}", MINIMAL_ECG_CSV);

        let dir = tempfile::tempdir().unwrap();
        let ecg_path = dir.path().join("ecg_bom.csv");
        std::fs::write(&ecg_path, csv_with_bom).unwrap();

        import_single_ecg(&conn, &ecg_path, "test_import").unwrap();

        let reading_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
            .unwrap();
        assert_eq!(reading_count, 1);
    }

    #[test]
    fn match_header_picks_first_label() {
        assert_eq!(match_header("Recorded Date,2024-01-01", RECORDED_DATE_LABELS), Some("2024-01-01"));
        assert_eq!(match_header("記録日時,2024-01-01", RECORDED_DATE_LABELS), Some("2024-01-01"));
        // Non-matching label returns None.
        assert_eq!(match_header("Name,foo", RECORDED_DATE_LABELS), None);
        // Prefix without comma must not match (avoids matching "Recorded Datetime,").
        assert_eq!(match_header("Recorded Datetime,2024", RECORDED_DATE_LABELS), None);
    }

    #[test]
    fn strip_bom_handles_both_cases() {
        assert_eq!(strip_bom("hello"), "hello");
        assert_eq!(strip_bom("\u{feff}hello"), "hello");
    }

    // Acceptance Criteria from issue #5: a symptoms field that itself contains
    // commas must not be truncated. The matcher returns the whole slice after
    // the first comma, so internal commas in the value are preserved.
    #[test]
    fn symptoms_field_preserves_internal_commas() {
        assert_eq!(
            match_header("Symptoms,Palpitations, Shortness of breath", SYMPTOMS_LABELS),
            Some("Palpitations, Shortness of breath"),
        );
        assert_eq!(
            match_header("症状,動悸, めまい", SYMPTOMS_LABELS),
            Some("動悸, めまい"),
        );
    }

    // Apple Health export from a Windows machine (or after a re-zip) can have
    // CRLF line endings. `str::lines()` handles both \n and \r\n.
    #[test]
    fn import_single_ecg_handles_crlf() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let csv_crlf = MINIMAL_ECG_CSV.replace('\n', "\r\n");

        let dir = tempfile::tempdir().unwrap();
        let ecg_path = dir.path().join("ecg_crlf.csv");
        std::fs::write(&ecg_path, csv_crlf).unwrap();

        import_single_ecg(&conn, &ecg_path, "test_import").unwrap();

        let reading_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
            .unwrap();
        assert_eq!(reading_count, 1);
        let sample_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ecg_samples", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sample_count, 5);
    }
}
