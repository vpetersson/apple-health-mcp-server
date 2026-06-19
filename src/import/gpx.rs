use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDateTime};
use duckdb::Connection;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::HashMap;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use tracing::info;

use crate::models::compute_hash;

pub fn import_gpx_files(
    conn: &Connection,
    routes_dir: &Path,
    import_id: &str,
    workout_route_map: &HashMap<String, String>,
    workout_offset_map: &HashMap<String, i32>,
) -> Result<u64> {
    if !routes_dir.exists() {
        info!("No workout-routes directory found, skipping GPX import");
        return Ok(0);
    }

    let mut total_points = 0u64;
    let mut entries: Vec<_> = fs::read_dir(routes_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "gpx"))
        .collect();
    entries.sort_by_key(|e| e.path());

    info!("Found {} GPX route files", entries.len());

    for entry in &entries {
        let path = entry.path();
        let filename = path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("")
            .to_string();

        // Look up workout hash from the route map
        // The map keys are like "/workout-routes/route_2020-05-21_1.14pm.gpx"
        let route_key = format!("/workout-routes/{}", filename);
        let workout_hash = workout_route_map.get(&route_key).cloned();
        let workout_offset = workout_hash
            .as_ref()
            .and_then(|h| workout_offset_map.get(h))
            .copied();

        match import_single_gpx(
            conn,
            &path,
            import_id,
            workout_hash.as_deref(),
            workout_offset,
        ) {
            Ok(n) => total_points += n,
            Err(e) => {
                tracing::warn!("Failed to import GPX file {:?}: {:?}", path, e);
            }
        }
    }

    info!("Imported {} route points from GPX files", total_points);
    Ok(total_points)
}

fn attr_value(e: &quick_xml::events::BytesStart, name: &[u8]) -> Option<String> {
    e.attributes().filter_map(|a| a.ok()).find_map(|a| {
        if a.key.as_ref() == name {
            String::from_utf8(a.value.to_vec()).ok()
        } else {
            None
        }
    })
}

pub(crate) fn import_single_gpx(
    conn: &Connection,
    path: &Path,
    import_id: &str,
    workout_hash: Option<&str>,
    workout_offset_minutes: Option<i32>,
) -> Result<u64> {
    let file = fs::File::open(path).context("Failed to open GPX file")?;
    let reader = BufReader::new(file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut count = 0u64;

    let mut in_trkpt = false;
    let mut lat: Option<f64> = None;
    let mut lon: Option<f64> = None;
    let mut ele: Option<f64> = None;
    let mut timestamp: Option<String> = None;
    let mut speed: Option<f64> = None;
    let mut course: Option<f64> = None;
    let mut h_accuracy: Option<f64> = None;
    let mut v_accuracy: Option<f64> = None;
    let mut current_tag: Option<String> = None;

    let mut appender = conn.appender("route_points")?;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) => {
                let local = e.local_name();
                match local.as_ref() {
                    b"trkpt" => {
                        in_trkpt = true;
                        lat = attr_value(e, b"lat").and_then(|v| v.parse().ok());
                        lon = attr_value(e, b"lon").and_then(|v| v.parse().ok());
                        ele = None;
                        timestamp = None;
                        speed = None;
                        course = None;
                        h_accuracy = None;
                        v_accuracy = None;
                    }
                    b"ele" | b"time" | b"speed" | b"course" | b"hAcc" | b"vAcc" if in_trkpt => {
                        current_tag = Some(String::from_utf8_lossy(local.as_ref()).to_string());
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref t)) if in_trkpt => {
                if let Some(ref tag) = current_tag {
                    let text = t.unescape().unwrap_or_default().to_string();
                    match tag.as_str() {
                        "ele" => ele = text.parse().ok(),
                        "time" => timestamp = Some(text),
                        "speed" => speed = text.parse().ok(),
                        "course" => course = text.parse().ok(),
                        "hAcc" => h_accuracy = text.parse().ok(),
                        "vAcc" => v_accuracy = text.parse().ok(),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let local = e.local_name();
                if local.as_ref() == b"trkpt" && in_trkpt {
                    if let (Some(lat_v), Some(lon_v), Some(ref ts)) = (lat, lon, &timestamp) {
                        let wh = workout_hash.unwrap_or("");
                        let point_hash =
                            compute_hash(&[wh, ts, &lat_v.to_string(), &lon_v.to_string()]);

                        // GPX timestamps are true UTC ("...Z"). The rest of
                        // the database stores local wall-clock time as a
                        // naive TIMESTAMP (the XML/ECG importers strip the
                        // offset suffix without converting). Shift the GPX
                        // value by the owning workout's offset so route
                        // points land on the same local-time basis as the
                        // workout itself. When the offset is unknown
                        // (workout-less GPX, malformed `startDate`), fall
                        // back to the legacy strip-only behavior.
                        let clean_ts = match workout_offset_minutes {
                            Some(offset) => shift_utc_to_local(ts, offset),
                            None => clean_timestamp(ts),
                        };

                        appender.append_row(duckdb::params![
                            point_hash,
                            workout_hash,
                            lat_v,
                            lon_v,
                            ele,
                            clean_ts,
                            speed,
                            course,
                            h_accuracy,
                            v_accuracy,
                            import_id,
                        ])?;
                        count += 1;
                    }
                    in_trkpt = false;
                }
                current_tag = None;
            }
            Ok(Event::Empty(ref _e)) if in_trkpt => {
                // Self-closing extension elements
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("GPX parse error: {:?}", e);
            }
        }
        buf.clear();
    }
    appender.flush()?;

    Ok(count)
}

/// Shift a true-UTC GPX timestamp by the owning workout's offset so the
/// result expresses local wall-clock time, then return it in DuckDB naive
/// TIMESTAMP form ("YYYY-MM-DD HH:MM:SS"). Used to align route points with
/// the XML/ECG convention of storing local time as a naive TIMESTAMP.
///
/// "2020-06-20T16:56:44Z" + 540 (i.e. +09:00) -> "2020-06-21 01:56:44"
///
/// Falls back to [`clean_timestamp`] when the input cannot be parsed as
/// an ISO 8601 datetime, so a malformed `<time>` element degrades to the
/// legacy behavior instead of dropping the point.
pub(crate) fn shift_utc_to_local(ts: &str, offset_minutes: i32) -> String {
    let trimmed = ts.trim();
    // GPX from Apple Health uses RFC 3339 with a trailing `Z`. Try that
    // first, then any RFC 3339 with explicit offset, then a naive form.
    let utc = if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        dt.naive_utc()
    } else if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        ndt
    } else if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        ndt
    } else {
        return clean_timestamp(ts);
    };
    let shifted = utc + Duration::minutes(offset_minutes as i64);
    shifted.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Strip timezone info from a GPX ISO 8601 timestamp and replace `T` with
/// a space so the value fits a DuckDB naive TIMESTAMP column. Note that
/// this preserves the original UTC instant in the emitted string
/// (`"2020-06-20T16:56:44Z"` -> `"2020-06-20 16:56:44"`), which only
/// matches the rest of the database if the rest is also interpreted as
/// UTC. The XML and ECG importers store local wall-clock time instead, so
/// the GPX importer normally calls [`shift_utc_to_local`] to align with
/// that convention; this helper is the fallback used when the owning
/// workout's offset is unknown.
pub(crate) fn clean_timestamp(ts: &str) -> String {
    let s = ts.trim();
    // Remove trailing 'Z'
    let s = s.strip_suffix('Z').unwrap_or(s);
    // Remove +HH:MM or -HH:MM timezone offset
    let s = if s.len() > 6 {
        let tail = &s[s.len() - 6..];
        if (tail.starts_with('+') || tail.starts_with('-')) && tail.contains(':') {
            &s[..s.len() - 6]
        } else {
            s
        }
    } else {
        s
    };
    // Replace 'T' with space for DuckDB
    s.replace('T', " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ensure_schema, open_db_in_memory};
    use std::collections::HashMap;

    #[test]
    fn clean_timestamp_z_suffix() {
        assert_eq!(
            clean_timestamp("2020-06-20T16:56:44Z"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_timestamp_positive_offset() {
        assert_eq!(
            clean_timestamp("2020-06-20T16:56:44+00:00"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_timestamp_negative_offset() {
        assert_eq!(
            clean_timestamp("2020-06-20T16:56:44-05:00"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_timestamp_no_tz() {
        assert_eq!(
            clean_timestamp("2020-06-20T16:56:44"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_timestamp_short_string() {
        assert_eq!(clean_timestamp("12:00"), "12:00");
    }

    #[test]
    fn import_single_gpx_minimal() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let gpx = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
  <trk>
    <trkseg>
      <trkpt lat="37.7749" lon="-122.4194">
        <ele>10.5</ele>
        <time>2024-01-01T10:00:00Z</time>
        <speed>3.5</speed>
        <course>180.0</course>
        <hAcc>5.0</hAcc>
        <vAcc>3.0</vAcc>
      </trkpt>
      <trkpt lat="37.7750" lon="-122.4195">
        <ele>11.0</ele>
        <time>2024-01-01T10:00:05Z</time>
        <speed>3.6</speed>
        <course>181.0</course>
        <hAcc>4.5</hAcc>
        <vAcc>2.8</vAcc>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"#;

        let dir = tempfile::tempdir().unwrap();
        let gpx_path = dir.path().join("route.gpx");
        std::fs::write(&gpx_path, gpx).unwrap();

        let count = import_single_gpx(
            &conn,
            &gpx_path,
            "test_import",
            Some("workout_hash_1"),
            None,
        )
        .unwrap();
        assert_eq!(count, 2);

        let db_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM route_points", [], |row| row.get(0))
            .unwrap();
        assert_eq!(db_count, 2);

        let wh: String = conn
            .query_row("SELECT workout_hash FROM route_points LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(wh, "workout_hash_1");
    }

    #[test]
    fn import_gpx_files_missing_dir() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let missing = std::path::PathBuf::from("/nonexistent/path/routes");
        let map = HashMap::new();
        let offsets = HashMap::new();
        let count = import_gpx_files(&conn, &missing, "test", &map, &offsets).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn import_gpx_no_workout_hash() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let gpx = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
  <trk><trkseg>
    <trkpt lat="37.0" lon="-122.0">
      <ele>5.0</ele>
      <time>2024-01-01T10:00:00Z</time>
    </trkpt>
  </trkseg></trk>
</gpx>"#;

        let dir = tempfile::tempdir().unwrap();
        let gpx_path = dir.path().join("route.gpx");
        std::fs::write(&gpx_path, gpx).unwrap();

        let count = import_single_gpx(&conn, &gpx_path, "test_import", None, None).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn shift_utc_to_local_jst() {
        // A `+0900` workout (JST) -> add 9 hours so the GPX wall-clock
        // matches the XML/ECG-side local-time representation.
        assert_eq!(
            shift_utc_to_local("2020-06-20T16:56:44Z", 540),
            "2020-06-21 01:56:44"
        );
    }

    #[test]
    fn shift_utc_to_local_pst_overseas_workout() {
        // A workout recorded in PST (UTC-08:00) on a watch whose owner is
        // traveling abroad. The GPX importer should land the route on
        // local-time-as-naive-TIMESTAMP so it joins cleanly against the
        // workout's `start_date`.
        assert_eq!(
            shift_utc_to_local("2024-03-03T15:00:00Z", -480),
            "2024-03-03 07:00:00"
        );
    }

    #[test]
    fn shift_utc_to_local_handles_explicit_offset_in_input() {
        // GPX in the wild sometimes carries an explicit ±HH:MM rather than
        // `Z`. Either form must parse as UTC and shift by the workout's
        // offset, not the GPX's own offset.
        assert_eq!(
            shift_utc_to_local("2024-03-03T15:00:00+00:00", 540),
            "2024-03-04 00:00:00"
        );
    }

    #[test]
    fn shift_utc_to_local_unparseable_falls_back() {
        // A malformed timestamp must not panic or drop the point. Fall
        // back to the legacy strip-only behavior so the import still lands
        // a row (the wall-clock will be wrong, but no worse than before).
        assert_eq!(shift_utc_to_local("garbage", 540), "garbage");
    }

    #[test]
    fn import_single_gpx_shifts_by_workout_offset() {
        // End-to-end check: a route point with a `Z` timestamp lands in
        // route_points with the local wall-clock equivalent of the
        // workout's offset, matching the convention used for every other
        // date column.
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let gpx = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
  <trk><trkseg>
    <trkpt lat="35.0" lon="139.0">
      <ele>10.0</ele>
      <time>2024-06-17T04:58:39Z</time>
    </trkpt>
  </trkseg></trk>
</gpx>"#;

        let dir = tempfile::tempdir().unwrap();
        let gpx_path = dir.path().join("route_jst.gpx");
        std::fs::write(&gpx_path, gpx).unwrap();

        // JST offset = +540 minutes
        import_single_gpx(&conn, &gpx_path, "test", Some("wh_jst"), Some(540)).unwrap();

        let ts: String = conn
            .query_row(
                "SELECT CAST(timestamp AS VARCHAR) FROM route_points LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ts, "2024-06-17 13:58:39");
    }

    #[test]
    fn import_single_gpx_without_offset_uses_legacy_strip() {
        // No workout offset known -> fall back to the legacy strip-only
        // behavior so an orphan GPX still lands (matching the pre-fix
        // contract). The stored value is true UTC in naive form, but
        // that's no worse than before the fix.
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let gpx = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
  <trk><trkseg>
    <trkpt lat="35.0" lon="139.0">
      <ele>10.0</ele>
      <time>2024-06-17T04:58:39Z</time>
    </trkpt>
  </trkseg></trk>
</gpx>"#;

        let dir = tempfile::tempdir().unwrap();
        let gpx_path = dir.path().join("route_orphan.gpx");
        std::fs::write(&gpx_path, gpx).unwrap();

        import_single_gpx(&conn, &gpx_path, "test", None, None).unwrap();

        let ts: String = conn
            .query_row(
                "SELECT CAST(timestamp AS VARCHAR) FROM route_points LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // Legacy behavior preserves true UTC instant in naive form.
        assert_eq!(ts, "2024-06-17 04:58:39");
    }
}
