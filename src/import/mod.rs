pub mod ecg;
pub mod gpx;
pub mod xml;

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

use crate::db::{deduplicate_tables, ensure_schema, open_db, rebuild_daily_stats};

pub fn run_import(export_dir: &Path, db_path: &Path) -> Result<()> {
    let start = std::time::Instant::now();
    let import_id = format!("import_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S"));

    info!("Starting import {} from {:?}", import_id, export_dir);

    let conn = open_db(db_path)?;
    ensure_schema(&conn)?;

    // Phase 1: Parse export.xml
    info!("Phase 1: Parsing export.xml...");
    let xml_path = export_dir.join("export.xml");
    let stats = xml::import_xml(&conn, &xml_path, &import_id)?;

    // Build workout route map from the XML data
    // We need to query the workouts and their associated route files
    // The XML parser stores route file references — we'll build the map from the DB
    // For now, we'll build it by re-scanning the XML for WorkoutRoute → FileReference mappings
    let workout_route_map = build_workout_route_map(&conn, &xml_path)?;

    // Phase 2: Parse ECG files
    info!("Phase 2: Parsing ECG files...");
    let ecg_count =
        ecg::import_ecg_files(&conn, &export_dir.join("electrocardiograms"), &import_id)?;

    // Phase 3: Parse GPX routes
    info!("Phase 3: Parsing GPX route files...");
    let route_points = gpx::import_gpx_files(
        &conn,
        &export_dir.join("workout-routes"),
        &import_id,
        &workout_route_map,
    )?;

    // Phase 4: Deduplicate tables
    info!("Phase 4: Deduplicating tables...");
    deduplicate_tables(&conn)?;

    // Phase 5: Rebuild aggregation tables
    info!("Phase 5: Building daily statistics...");
    rebuild_daily_stats(&conn)?;

    // Phase 6: Log import metadata
    let duration = start.elapsed();
    conn.execute(
        "INSERT INTO imports (import_id, export_dir, record_count, workout_count, duration_secs) VALUES (?, ?, ?, ?, ?)",
        duckdb::params![
            import_id,
            export_dir.to_string_lossy().to_string(),
            stats.records as i64,
            stats.workouts as i64,
            duration.as_secs_f64(),
        ],
    )?;

    info!("Import complete in {:.1}s", duration.as_secs_f64());
    info!(
        "  Records: {}, Workouts: {}, Activity Summaries: {}",
        stats.records, stats.workouts, stats.activity_summaries
    );
    info!(
        "  ECG readings: {}, Route points: {}, Metadata entries: {}",
        ecg_count, route_points, stats.metadata_entries
    );

    Ok(())
}

/// Build a map from route file path to workout hash by re-scanning the XML
/// for Workout elements that contain WorkoutRoute > FileReference children.
fn build_workout_route_map(
    _conn: &duckdb::Connection,
    xml_path: &Path,
) -> Result<HashMap<String, String>> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;
    use std::io::BufReader;

    let mut map = HashMap::new();
    let file = std::fs::File::open(xml_path)?;
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut in_workout = false;
    let mut current_workout_hash: Option<String> = None;

    fn attr_val(e: &quick_xml::events::BytesStart, name: &[u8]) -> Option<String> {
        e.attributes().filter_map(|a| a.ok()).find_map(|a| {
            if a.key.as_ref() == name {
                String::from_utf8(a.value.to_vec()).ok()
            } else {
                None
            }
        })
    }

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let local = e.name().as_ref().to_vec();
                match local.as_slice() {
                    b"Workout" => {
                        in_workout = true;
                        let activity_type =
                            attr_val(e, b"workoutActivityType").unwrap_or_default();
                        let source_name = attr_val(e, b"sourceName").unwrap_or_default();
                        let start_date = attr_val(e, b"startDate").unwrap_or_default();
                        let end_date = attr_val(e, b"endDate").unwrap_or_default();
                        let duration_str = attr_val(e, b"duration");

                        let hash = crate::models::compute_hash(&[
                            &activity_type,
                            &source_name,
                            &start_date,
                            &end_date,
                            duration_str.as_deref().unwrap_or(""),
                        ]);
                        current_workout_hash = Some(hash);
                    }
                    b"FileReference" if in_workout => {
                        if let (Some(ref wh), Some(path)) =
                            (&current_workout_hash, attr_val(e, b"path"))
                        {
                            map.insert(path, wh.clone());
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"Workout" {
                    in_workout = false;
                    current_workout_hash = None;
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
        buf.clear();
    }

    info!("Built workout route map: {} entries", map.len());
    Ok(map)
}
