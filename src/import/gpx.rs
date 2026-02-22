use anyhow::{Context, Result};
use duckdb::Connection;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use tracing::info;

use crate::models::compute_hash;

pub fn import_gpx_files(
    conn: &Connection,
    routes_dir: &Path,
    import_id: &str,
    workout_route_map: &std::collections::HashMap<String, String>,
) -> Result<u64> {
    if !routes_dir.exists() {
        info!("No workout-routes directory found, skipping GPX import");
        return Ok(0);
    }

    let mut total_points = 0u64;
    let mut entries: Vec<_> = fs::read_dir(routes_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "gpx"))
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

        match import_single_gpx(conn, &path, import_id, workout_hash.as_deref()) {
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

fn import_single_gpx(
    conn: &Connection,
    path: &Path,
    import_id: &str,
    workout_hash: Option<&str>,
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
                    b"ele" | b"time" | b"speed" | b"course" | b"hAcc" | b"vAcc"
                        if in_trkpt =>
                    {
                        current_tag =
                            Some(String::from_utf8_lossy(local.as_ref()).to_string());
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
                        let point_hash = compute_hash(&[
                            wh,
                            ts,
                            &lat_v.to_string(),
                            &lon_v.to_string(),
                        ]);

                        // Clean timestamp for DuckDB (strip timezone suffix)
                        let clean_ts = clean_timestamp(ts);

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

/// Strip timezone info from GPX timestamps for DuckDB TIMESTAMP compatibility.
/// Handles ISO 8601 formats like "2020-06-20T16:56:44Z" or "2020-06-20T16:56:44+00:00"
fn clean_timestamp(ts: &str) -> String {
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
