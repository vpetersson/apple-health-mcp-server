use apple_health_mcp::db::{open_db, open_db_readonly};

#[test]
fn open_db_creates_file() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.duckdb");
    assert!(!db_path.exists());

    let _conn = open_db(&db_path).unwrap();
    assert!(db_path.exists());
}

#[test]
fn open_db_readonly_on_existing() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.duckdb");

    // Create the DB first
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER)")
            .unwrap();
    }

    // Open read-only
    let conn = open_db_readonly(&db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn open_db_readonly_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.duckdb");

    // Create the DB first
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER)")
            .unwrap();
    }

    let conn = open_db_readonly(&db_path).unwrap();
    let result = conn.execute_batch("INSERT INTO test VALUES (1)");
    assert!(result.is_err());
}
