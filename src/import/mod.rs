pub mod ecg;
pub mod gpx;
pub mod xml;

use anyhow::Result;
use std::path::Path;
use tracing::info;

use crate::db::{
    deduplicate_tables, ensure_schema, open_db, populate_workout_vestigial_columns,
    rebuild_daily_stats,
};

pub fn run_import(export_dir: &Path, db_path: &Path) -> Result<()> {
    let start = std::time::Instant::now();
    let import_id = format!("import_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S"));

    info!("Starting import {} from {:?}", import_id, export_dir);

    let conn = open_db(db_path)?;
    ensure_schema(&conn)?;

    // Phase 1: Parse export.xml. The scanner records both the workout rows
    // and a FileReference path -> workout_hash map in a single pass, so we
    // never re-open the XML to rebuild that map (the old
    // build_workout_route_map second scan is gone) and the map's hashes are
    // guaranteed to match the workouts table because they are literally the
    // same values.
    info!("Phase 1: Parsing export.xml...");
    let xml_path = export_dir.join("export.xml");
    let stats = xml::import_xml(&conn, &xml_path, &import_id)?;

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
        &stats.workout_route_map,
    )?;

    // Phase 4: Deduplicate tables
    info!("Phase 4: Deduplicating tables...");
    deduplicate_tables(&conn)?;

    // Phase 5: Backfill workouts.total_distance / total_energy_burned from
    // workout_statistics. Must run after deduplication so the SUM over the
    // statistics table sees one row per (workout_hash, stat_type) instead
    // of the duplicate set that bulk-loading produced.
    info!("Phase 5: Backfilling vestigial workout columns from statistics...");
    populate_workout_vestigial_columns(&conn)?;

    // Phase 6: Rebuild aggregation tables
    info!("Phase 6: Building daily statistics...");
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
