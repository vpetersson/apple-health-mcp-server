use anyhow::Result;
use duckdb::{AccessMode, Config, Connection};
use std::path::Path;
use tracing::info;

pub fn open_db(db_path: &Path) -> Result<Connection> {
    let config = Config::default().access_mode(AccessMode::ReadWrite)?;
    let conn = Connection::open_with_flags(db_path, config)?;
    conn.execute_batch("PRAGMA threads=4;")?;
    Ok(conn)
}

pub fn open_db_readonly(db_path: &Path) -> Result<Connection> {
    let config = Config::default().access_mode(AccessMode::ReadOnly)?;
    let conn = Connection::open_with_flags(db_path, config)?;
    Ok(conn)
}

/// Create tables without PRIMARY KEY constraints so Appender can bulk-load.
/// Deduplication happens in `deduplicate_tables()` after loading.
pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS records (
            record_hash     VARCHAR,
            record_type     VARCHAR NOT NULL,
            value           DOUBLE,
            unit            VARCHAR,
            source_name     VARCHAR,
            source_version  VARCHAR,
            device          VARCHAR,
            creation_date   TIMESTAMP,
            start_date      TIMESTAMP NOT NULL,
            end_date        TIMESTAMP NOT NULL,
            import_id       VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS record_metadata (
            record_hash     VARCHAR NOT NULL,
            key             VARCHAR NOT NULL,
            value           VARCHAR
        );

        CREATE TABLE IF NOT EXISTS workouts (
            workout_hash         VARCHAR,
            activity_type        VARCHAR NOT NULL,
            duration             DOUBLE,
            duration_unit        VARCHAR,
            total_distance       DOUBLE,
            total_distance_unit  VARCHAR,
            total_energy_burned  DOUBLE,
            total_energy_unit    VARCHAR,
            source_name          VARCHAR,
            source_version       VARCHAR,
            device               VARCHAR,
            creation_date        TIMESTAMP,
            start_date           TIMESTAMP NOT NULL,
            end_date             TIMESTAMP NOT NULL,
            import_id            VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_events (
            workout_hash    VARCHAR NOT NULL,
            event_type      VARCHAR NOT NULL,
            date            TIMESTAMP,
            duration        DOUBLE,
            duration_unit   VARCHAR
        );

        CREATE TABLE IF NOT EXISTS workout_statistics (
            workout_hash    VARCHAR NOT NULL,
            stat_type       VARCHAR NOT NULL,
            start_date      TIMESTAMP,
            end_date        TIMESTAMP,
            average         DOUBLE,
            minimum         DOUBLE,
            maximum         DOUBLE,
            sum             DOUBLE,
            unit            VARCHAR
        );

        CREATE TABLE IF NOT EXISTS activity_summaries (
            date_components          VARCHAR,
            active_energy_burned     DOUBLE,
            active_energy_burned_goal DOUBLE,
            apple_move_time          DOUBLE,
            apple_move_time_goal     DOUBLE,
            apple_exercise_time      DOUBLE,
            apple_exercise_time_goal DOUBLE,
            apple_stand_hours        DOUBLE,
            apple_stand_hours_goal   DOUBLE,
            import_id                VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ecg_readings (
            ecg_hash         VARCHAR,
            recorded_date    TIMESTAMP NOT NULL,
            classification   VARCHAR,
            device           VARCHAR,
            sample_rate_hz   DOUBLE,
            symptoms         VARCHAR,
            software_version VARCHAR,
            import_id        VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ecg_samples (
            ecg_hash    VARCHAR NOT NULL,
            sample_idx  INTEGER NOT NULL,
            voltage_uv  DOUBLE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS route_points (
            point_hash    VARCHAR,
            workout_hash  VARCHAR,
            latitude      DOUBLE NOT NULL,
            longitude     DOUBLE NOT NULL,
            elevation     DOUBLE,
            timestamp     TIMESTAMP NOT NULL,
            speed         DOUBLE,
            course        DOUBLE,
            h_accuracy    DOUBLE,
            v_accuracy    DOUBLE,
            import_id     VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imports (
            import_id    VARCHAR,
            export_dir   VARCHAR NOT NULL,
            imported_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            record_count BIGINT,
            workout_count BIGINT,
            duration_secs DOUBLE
        );
        ",
    )?;
    Ok(())
}

/// Deduplicate all tables after bulk loading.
/// Replaces each table with a deduplicated version using DISTINCT ON or GROUP BY.
pub fn deduplicate_tables(conn: &Connection) -> Result<()> {
    info!("Deduplicating tables...");

    conn.execute_batch(
        "
        CREATE OR REPLACE TABLE records AS
        SELECT * FROM (
            SELECT DISTINCT ON (record_hash) *
            FROM records
        );

        CREATE OR REPLACE TABLE record_metadata AS
        SELECT * FROM (
            SELECT DISTINCT ON (record_hash, key) *
            FROM record_metadata
        );

        CREATE OR REPLACE TABLE workouts AS
        SELECT * FROM (
            SELECT DISTINCT ON (workout_hash) *
            FROM workouts
        );

        CREATE OR REPLACE TABLE activity_summaries AS
        SELECT * FROM (
            SELECT DISTINCT ON (date_components) *
            FROM activity_summaries
            ORDER BY date_components, import_id DESC
        );

        CREATE OR REPLACE TABLE ecg_readings AS
        SELECT * FROM (
            SELECT DISTINCT ON (ecg_hash) *
            FROM ecg_readings
        );

        CREATE OR REPLACE TABLE ecg_samples AS
        SELECT * FROM (
            SELECT DISTINCT ON (ecg_hash, sample_idx) *
            FROM ecg_samples
        );

        CREATE OR REPLACE TABLE route_points AS
        SELECT * FROM (
            SELECT DISTINCT ON (point_hash) *
            FROM route_points
        );

        CREATE OR REPLACE TABLE imports AS
        SELECT * FROM (
            SELECT DISTINCT ON (import_id) *
            FROM imports
        );

        -- Now add indexes
        CREATE INDEX IF NOT EXISTS idx_records_type_date ON records(record_type, start_date);
        CREATE INDEX IF NOT EXISTS idx_records_source ON records(source_name);
        CREATE INDEX IF NOT EXISTS idx_workouts_type_date ON workouts(activity_type, start_date);
        CREATE INDEX IF NOT EXISTS idx_route_points_workout ON route_points(workout_hash);
        ",
    )?;

    info!("Deduplication complete");
    Ok(())
}

pub fn rebuild_daily_stats(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE OR REPLACE TABLE daily_record_stats AS
        SELECT
            record_type,
            CAST(start_date AS DATE) AS date,
            unit,
            COUNT(*) AS count,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            SUM(value) AS sum_value
        FROM records
        WHERE value IS NOT NULL
        GROUP BY record_type, CAST(start_date AS DATE), unit;
        ",
    )?;
    Ok(())
}

pub fn open_db_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA threads=4;")?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Connection {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn schema_creation() {
        let conn = setup();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // records, record_metadata, workouts, workout_events, workout_statistics,
        // activity_summaries, ecg_readings, ecg_samples, route_points, imports = 10
        assert_eq!(count, 10);
    }

    #[test]
    fn schema_idempotent() {
        let conn = setup();
        ensure_schema(&conn).unwrap(); // second call should not fail
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn deduplication() {
        let conn = setup();
        // Insert duplicate records
        conn.execute_batch(
            "
            INSERT INTO records VALUES ('hash1', 'HeartRate', 72.0, 'count/min', 'Watch', '1.0', NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-01 00:01:00', 'imp1');
            INSERT INTO records VALUES ('hash1', 'HeartRate', 72.0, 'count/min', 'Watch', '1.0', NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-01 00:01:00', 'imp1');
            INSERT INTO records VALUES ('hash2', 'StepCount', 100.0, 'count', 'Phone', '1.0', NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-01 00:01:00', 'imp1');
            ",
        )
        .unwrap();

        deduplicate_tables(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn daily_stats_aggregation() {
        let conn = setup();
        conn.execute_batch(
            "
            INSERT INTO records VALUES ('h1', 'HeartRate', 72.0, 'count/min', 'Watch', NULL, NULL, NULL, '2024-01-01 08:00:00', '2024-01-01 08:01:00', 'imp1');
            INSERT INTO records VALUES ('h2', 'HeartRate', 80.0, 'count/min', 'Watch', NULL, NULL, NULL, '2024-01-01 09:00:00', '2024-01-01 09:01:00', 'imp1');
            INSERT INTO records VALUES ('h3', 'HeartRate', 65.0, 'count/min', 'Watch', NULL, NULL, NULL, '2024-01-02 08:00:00', '2024-01-02 08:01:00', 'imp1');
            ",
        )
        .unwrap();

        rebuild_daily_stats(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM daily_record_stats", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2); // 2 days

        let avg: f64 = conn
            .query_row(
                "SELECT avg_value FROM daily_record_stats WHERE date = '2024-01-01'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!((avg - 76.0).abs() < 0.01);
    }

    #[test]
    fn open_db_in_memory_works() {
        let conn = open_db_in_memory().unwrap();
        let result: i64 = conn.query_row("SELECT 1", [], |row| row.get(0)).unwrap();
        assert_eq!(result, 1);
    }
}
