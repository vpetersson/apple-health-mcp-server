mod common;

use apple_health_mcp::db::{deduplicate_tables, ensure_schema, open_db, rebuild_daily_stats};
use apple_health_mcp::import::ecg::import_ecg_files;
use apple_health_mcp::import::gpx::import_gpx_files;
use apple_health_mcp::import::run_import;
use apple_health_mcp::import::xml::import_xml;
use std::collections::HashMap;

#[test]
fn full_import_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let export_dir = dir.path().join("apple_health_export");
    std::fs::create_dir_all(&export_dir).unwrap();

    // Write export.xml
    std::fs::write(export_dir.join("export.xml"), common::MINIMAL_XML).unwrap();

    // Write ECG files
    let ecg_dir = export_dir.join("electrocardiograms");
    std::fs::create_dir_all(&ecg_dir).unwrap();
    std::fs::write(ecg_dir.join("ecg_2024.csv"), common::MINIMAL_ECG_CSV).unwrap();

    // Write GPX files
    let gpx_dir = export_dir.join("workout-routes");
    std::fs::create_dir_all(&gpx_dir).unwrap();
    std::fs::write(gpx_dir.join("route_2024-01-01.gpx"), common::MINIMAL_GPX).unwrap();

    // Use a file-based DB for full pipeline
    let db_path = dir.path().join("test.duckdb");
    let conn = open_db(&db_path).unwrap();
    ensure_schema(&conn).unwrap();

    // Phase 1: XML
    let stats = import_xml(&conn, &export_dir.join("export.xml"), "test_import").unwrap();
    assert_eq!(stats.records, 2);
    assert_eq!(stats.workouts, 1);
    assert_eq!(stats.activity_summaries, 1);
    assert_eq!(stats.correlations, 1);
    assert_eq!(stats.metadata_entries, 1);
    assert_eq!(stats.workout_events, 1);
    assert_eq!(stats.workout_statistics, 1);

    // Phase 2: ECG
    let ecg_count = import_ecg_files(&conn, &ecg_dir, "test_import").unwrap();
    assert_eq!(ecg_count, 1);

    // Phase 3: GPX (need route map)
    let mut route_map = HashMap::new();
    // The XML has a FileReference path="/workout-routes/route_2024-01-01.gpx"
    // We need the workout hash. Let's get it from the DB.
    let workout_hash: String = conn
        .query_row("SELECT workout_hash FROM workouts LIMIT 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    route_map.insert(
        "/workout-routes/route_2024-01-01.gpx".to_string(),
        workout_hash,
    );
    let route_count = import_gpx_files(&conn, &gpx_dir, "test_import", &route_map).unwrap();
    assert_eq!(route_count, 2);

    // Phase 4: Deduplicate
    deduplicate_tables(&conn).unwrap();

    // Phase 5: Rebuild daily stats
    rebuild_daily_stats(&conn).unwrap();

    // Verify all tables populated
    let record_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(record_count, 2);

    let workout_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workouts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(workout_count, 1);

    let activity_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM activity_summaries", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(activity_count, 1);

    let ecg_reading_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
        .unwrap();
    assert_eq!(ecg_reading_count, 1);

    let ecg_sample_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ecg_samples", [], |row| row.get(0))
        .unwrap();
    assert_eq!(ecg_sample_count, 5);

    let route_point_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM route_points", [], |row| row.get(0))
        .unwrap();
    assert_eq!(route_point_count, 2);

    let metadata_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM record_metadata", [], |row| row.get(0))
        .unwrap();
    assert_eq!(metadata_count, 1);

    let event_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workout_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(event_count, 1);

    let stat_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workout_statistics", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stat_count, 1);

    let daily_stat_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM daily_record_stats", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(daily_stat_count > 0);
}

#[test]
fn import_idempotent_with_dedup() {
    let conn = common::setup_test_db();

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("export.xml"), common::MINIMAL_XML).unwrap();
    let xml_path = dir.path().join("export.xml");

    // Import twice
    import_xml(&conn, &xml_path, "imp1").unwrap();
    import_xml(&conn, &xml_path, "imp2").unwrap();

    // Before dedup: should have 4 records (2 x 2)
    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(before, 4);

    deduplicate_tables(&conn).unwrap();

    // After dedup: should have 2 unique records
    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, 2);
}

/// Test the top-level `run_import()` which exercises the full pipeline including
/// `build_workout_route_map()` internally.
#[test]
fn run_import_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let export_dir = dir.path().join("apple_health_export");
    std::fs::create_dir_all(&export_dir).unwrap();

    // Write export.xml
    std::fs::write(export_dir.join("export.xml"), common::MINIMAL_XML).unwrap();

    // Write ECG files
    let ecg_dir = export_dir.join("electrocardiograms");
    std::fs::create_dir_all(&ecg_dir).unwrap();
    std::fs::write(ecg_dir.join("ecg_2024.csv"), common::MINIMAL_ECG_CSV).unwrap();

    // Write GPX files
    let gpx_dir = export_dir.join("workout-routes");
    std::fs::create_dir_all(&gpx_dir).unwrap();
    std::fs::write(gpx_dir.join("route_2024-01-01.gpx"), common::MINIMAL_GPX).unwrap();

    let db_path = dir.path().join("import_test.duckdb");

    // run_import covers: open_db, ensure_schema, import_xml, build_workout_route_map,
    // import_ecg_files, import_gpx_files, deduplicate_tables, rebuild_daily_stats,
    // and the imports table INSERT.
    run_import(&export_dir, &db_path).unwrap();

    // Verify DB was created and populated
    let conn = open_db(&db_path).unwrap();

    let record_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(record_count, 2);

    let workout_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workouts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(workout_count, 1);

    let route_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM route_points", [], |row| row.get(0))
        .unwrap();
    assert_eq!(route_count, 2);

    let ecg_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ecg_readings", [], |row| row.get(0))
        .unwrap();
    assert_eq!(ecg_count, 1);

    // Verify import metadata was logged
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))
        .unwrap();
    assert_eq!(import_count, 1);

    // Verify daily stats were built
    let daily_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM daily_record_stats", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(daily_count > 0);
}

/// Test run_import with no ECG or GPX directories (graceful handling)
#[test]
fn run_import_xml_only() {
    let dir = tempfile::tempdir().unwrap();
    let export_dir = dir.path().join("export");
    std::fs::create_dir_all(&export_dir).unwrap();
    std::fs::write(export_dir.join("export.xml"), common::MINIMAL_XML).unwrap();
    // No electrocardiograms/ or workout-routes/ directories

    let db_path = dir.path().join("xml_only.duckdb");
    run_import(&export_dir, &db_path).unwrap();

    let conn = open_db(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
}
