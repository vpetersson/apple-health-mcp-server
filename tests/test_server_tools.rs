mod common;

use apple_health_mcp::db::{ensure_schema, open_db, open_db_in_memory, rebuild_daily_stats};
use apple_health_mcp::import::xml::import_xml;
use apple_health_mcp::server::HealthServer;

#[test]
fn server_new_with_file_db() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.duckdb");

    // Create and populate DB
    {
        let conn = open_db(&db_path).unwrap();
        ensure_schema(&conn).unwrap();
        let xml_dir = tempfile::tempdir().unwrap();
        std::fs::write(xml_dir.path().join("export.xml"), common::MINIMAL_XML).unwrap();
        import_xml(&conn, &xml_dir.path().join("export.xml"), "test").unwrap();
        rebuild_daily_stats(&conn).unwrap();
    }

    // Open via HealthServer
    let server = HealthServer::new(&db_path).unwrap();
    let result = server
        .query_to_json("SELECT COUNT(*) as cnt FROM records", &[])
        .unwrap();
    let cnt = result.as_array().unwrap()[0]
        .get("cnt")
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(cnt, 2);
}

#[test]
fn server_in_memory_query() {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    conn.execute_batch(
        "INSERT INTO records VALUES ('rh1', 'HKQuantityTypeIdentifierHeartRate', 72.0, 'count/min', 'Watch', NULL, NULL, NULL, '2024-01-01 08:00:00', '2024-01-01 08:01:00', 'imp1');",
    ).unwrap();
    rebuild_daily_stats(&conn).unwrap();

    let server = HealthServer::new_in_memory(conn);

    // Test list record types query
    let result = server
        .query_to_json(
            "SELECT record_type as type, COUNT(*) as count FROM records GROUP BY record_type",
            &[],
        )
        .unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0].get("type").unwrap(),
        "HKQuantityTypeIdentifierHeartRate"
    );

    // Test parameterized query
    let result = server
        .query_to_json(
            "SELECT value FROM records WHERE record_type = ?",
            &[&"HKQuantityTypeIdentifierHeartRate" as &dyn duckdb::ToSql],
        )
        .unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);

    // Test empty result
    let result = server
        .query_to_json(
            "SELECT * FROM records WHERE record_type = ?",
            &[&"NonExistent" as &dyn duckdb::ToSql],
        )
        .unwrap();
    assert_eq!(result.as_array().unwrap().len(), 0);
}

#[test]
fn server_custom_query_safety() {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    let server = HealthServer::new_in_memory(conn);

    // Valid SELECT works
    let result = server.query_to_json("SELECT 1 AS one", &[]);
    assert!(result.is_ok());

    // Invalid SQL returns error
    let result = server.query_to_json("THIS IS NOT SQL", &[]);
    assert!(result.is_err());
}

#[test]
fn server_queries_all_tables() {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    conn.execute_batch(
        "
        INSERT INTO records VALUES ('rh1', 'HeartRate', 72.0, 'bpm', 'Watch', NULL, NULL, NULL, '2024-01-01 08:00:00', '2024-01-01 08:01:00', 'imp1');
        INSERT INTO workouts VALUES ('wh1', 'Running', 1800.0, 'sec', 5000.0, 'm', 300.0, 'kcal', 'Watch', NULL, NULL, NULL, '2024-01-01 10:00:00', '2024-01-01 10:30:00', 'imp1');
        INSERT INTO activity_summaries VALUES ('2024-01-01', 500.0, 600.0, 45.0, 30.0, 30.0, 30.0, 10.0, 12.0, 'imp1');
        INSERT INTO ecg_readings VALUES ('ecg1', '2024-01-01 12:00:00', 'Normal', 'Watch', 512.0, NULL, '2.0', 'imp1');
        INSERT INTO route_points VALUES ('rp1', 'wh1', 37.7749, -122.4194, 10.5, '2024-01-01 10:00:00', 3.5, 180.0, 5.0, 3.0, 'imp1');
        INSERT INTO imports VALUES ('imp1', '/tmp', '2024-01-01 00:00:00', 1, 1, 1.0);
        ",
    )
    .unwrap();
    rebuild_daily_stats(&conn).unwrap();

    let server = HealthServer::new_in_memory(conn);

    // All tables should be queryable
    for table in &[
        "records",
        "workouts",
        "activity_summaries",
        "ecg_readings",
        "route_points",
        "imports",
        "daily_record_stats",
    ] {
        let sql = format!("SELECT COUNT(*) as cnt FROM {}", table);
        let result = server.query_to_json(&sql, &[]).unwrap();
        let cnt = result.as_array().unwrap()[0]
            .get("cnt")
            .unwrap()
            .as_i64()
            .unwrap();
        assert!(cnt >= 1, "Table {} should have data", table);
    }
}

/// Regression guard: DuckDB refuses to convert TIMESTAMP columns to `String`,
/// so an unhandled temporal variant drops every date from every tool response.
#[test]
fn imported_dates_survive_into_tool_output() {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    let xml_dir = tempfile::tempdir().unwrap();
    std::fs::write(xml_dir.path().join("export.xml"), common::MINIMAL_XML).unwrap();
    import_xml(&conn, &xml_dir.path().join("export.xml"), "test").unwrap();
    rebuild_daily_stats(&conn).unwrap();

    let server = HealthServer::new_in_memory(conn);
    let result = server
        .query_to_json(
            "SELECT start_date, end_date FROM records ORDER BY start_date",
            &[],
        )
        .unwrap();
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let start = row
            .get("start_date")
            .unwrap_or_else(|| panic!("start_date missing from {row}"));
        assert!(start.as_str().unwrap().starts_with("2024-01-01"), "{start}");
        assert!(row.get("end_date").is_some(), "end_date missing from {row}");
    }

    // The daily stats table stores DATE, not TIMESTAMP — a separate ValueRef variant.
    let stats = server
        .query_to_json("SELECT date FROM daily_record_stats LIMIT 1", &[])
        .unwrap();
    assert_eq!(
        stats.as_array().unwrap()[0].get("date").unwrap(),
        "2024-01-01"
    );
}

/// An in-memory database has no file to lock, so its connection is kept.
#[test]
fn in_memory_server_still_serves_queries() {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    let server = HealthServer::new_in_memory(conn);
    for _ in 0..3 {
        let result = server.query_to_json("SELECT 1 AS one", &[]).unwrap();
        assert_eq!(result.as_array().unwrap().len(), 1);
    }
    assert!(format!("{server:?}").contains(":memory:"));
}

/// `HealthServer::new` opens the database once to validate it, so a bad path
/// fails at startup rather than on the first tool call.
#[test]
fn missing_database_fails_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    assert!(HealthServer::new(&dir.path().join("does-not-exist.duckdb")).is_err());
}

/// A file-backed server reopens per query, so data written between queries is
/// picked up without reconstructing the server.
#[test]
fn file_backed_server_sees_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("health.duckdb");
    {
        let conn = open_db(&db_path).unwrap();
        ensure_schema(&conn).unwrap();
        conn.execute_batch(
            "INSERT INTO records VALUES ('rh1', 'HKQuantityTypeIdentifierHeartRate', 72.0,
             'count/min', 'Watch', NULL, NULL, NULL, '2024-01-01 08:00:00',
             '2024-01-01 08:01:00', 'imp1');",
        )
        .unwrap();
    }

    let server = HealthServer::new(&db_path).unwrap();
    assert_eq!(count_records(&server), 1);

    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch(
            "INSERT INTO records VALUES ('rh2', 'HKQuantityTypeIdentifierBodyMass', 70.0, 'kg',
             'Scale', NULL, NULL, NULL, '2024-02-01 08:00:00', '2024-02-01 08:00:00', 'imp2');",
        )
        .unwrap();
    }

    assert_eq!(count_records(&server), 2);
}

fn count_records(server: &HealthServer) -> i64 {
    server
        .query_to_json("SELECT COUNT(*) AS cnt FROM records", &[])
        .unwrap()
        .as_array()
        .unwrap()[0]
        .get("cnt")
        .unwrap()
        .as_i64()
        .unwrap()
}
