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
            -- Minutes east of UTC parsed from the workout's `startDate`
            -- attribute (e.g. 540 for `+0900`, -420 for `-0700`). The XML
            -- importer strips the offset before storing `start_date`,
            -- leaving a naive TIMESTAMP that holds local wall-clock time.
            -- This column preserves the original offset so the GPX importer
            -- can shift true-UTC route timestamps onto the same local-time
            -- basis as the rest of the data.
            start_offset_minutes INTEGER,
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

        CREATE TABLE IF NOT EXISTS workout_metadata (
            workout_hash    VARCHAR NOT NULL,
            key             VARCHAR NOT NULL,
            value           VARCHAR,
            import_id       VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workout_routes (
            workout_hash    VARCHAR NOT NULL,
            file_path       VARCHAR NOT NULL,
            source_name     VARCHAR,
            source_version  VARCHAR,
            creation_date   TIMESTAMP,
            start_date      TIMESTAMP,
            end_date        TIMESTAMP,
            import_id       VARCHAR NOT NULL
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

        CREATE OR REPLACE TABLE workout_metadata AS
        SELECT * FROM (
            SELECT DISTINCT ON (workout_hash, key) *
            FROM workout_metadata
            ORDER BY workout_hash, key, import_id DESC
        );

        CREATE OR REPLACE TABLE workout_routes AS
        SELECT * FROM (
            SELECT DISTINCT ON (workout_hash, file_path) *
            FROM workout_routes
            ORDER BY workout_hash, file_path, import_id DESC
        );

        -- workout_events and workout_statistics carry no import_id column,
        -- so the dedupe key has to come from the row's own structure. Apple
        -- Health spec emits at most one event per (workout, type, date) and
        -- one statistic per (workout, stat_type) — re-importing the same
        -- export collapses cleanly under those keys. Without these dedupes
        -- a second import doubled workouts.total_distance /
        -- total_energy_burned, because populate_workout_vestigial_columns
        -- sums over a now-duplicated workout_statistics.
        CREATE OR REPLACE TABLE workout_events AS
        SELECT * FROM (
            SELECT DISTINCT ON (workout_hash, event_type, date) *
            FROM workout_events
            ORDER BY workout_hash, event_type, date
        );

        CREATE OR REPLACE TABLE workout_statistics AS
        SELECT * FROM (
            SELECT DISTINCT ON (workout_hash, stat_type) *
            FROM workout_statistics
            ORDER BY workout_hash, stat_type, start_date
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
        CREATE INDEX IF NOT EXISTS idx_workout_metadata_hash ON workout_metadata(workout_hash);
        CREATE INDEX IF NOT EXISTS idx_workout_routes_hash ON workout_routes(workout_hash);
        ",
    )?;

    info!("Deduplication complete");
    Ok(())
}

/// Populate `workouts.total_distance` / `total_energy_burned` and the matching
/// unit columns from `workout_statistics`. Apple Health stopped emitting these
/// values as `<Workout>` attributes in iOS 11 and moved them into
/// `<WorkoutStatistics>` children, leaving the legacy `workouts` columns as
/// vestigial NULL on every row. Aggregating the statistics back into those
/// columns lets existing tooling that queries `workouts` directly keep working
/// without having to learn the statistics table.
pub fn populate_workout_vestigial_columns(conn: &Connection) -> Result<()> {
    info!("Populating workouts.total_distance / total_energy_burned from workout_statistics...");

    // Aggregate active energy burned. Multiple statistics rows for the same
    // workout (rare, but allowed) are summed; the unit is taken from any one of
    // them (units should be consistent within a single workout).
    conn.execute_batch(
        "
        UPDATE workouts AS w
        SET total_energy_burned = agg.total_sum,
            total_energy_unit   = COALESCE(w.total_energy_unit, agg.unit)
        FROM (
            SELECT
                workout_hash,
                SUM(sum)  AS total_sum,
                MIN(unit) AS unit
            FROM workout_statistics
            WHERE stat_type = 'HKQuantityTypeIdentifierActiveEnergyBurned'
              AND sum IS NOT NULL
            GROUP BY workout_hash
        ) AS agg
        WHERE w.workout_hash = agg.workout_hash
          AND w.total_energy_burned IS NULL;
        ",
    )?;

    // Aggregate distance across every distance-flavored quantity type. The
    // statistics table only carries one distance type per workout in practice
    // (the type matching the activity), so SUM is effectively a passthrough
    // while still tolerating multi-row stats.
    conn.execute_batch(
        "
        UPDATE workouts AS w
        SET total_distance      = agg.total_sum,
            total_distance_unit = COALESCE(w.total_distance_unit, agg.unit)
        FROM (
            SELECT
                workout_hash,
                SUM(sum)  AS total_sum,
                MIN(unit) AS unit
            FROM workout_statistics
            WHERE stat_type LIKE 'HKQuantityTypeIdentifierDistance%'
              AND sum IS NOT NULL
            GROUP BY workout_hash
        ) AS agg
        WHERE w.workout_hash = agg.workout_hash
          AND w.total_distance IS NULL;
        ",
    )?;

    info!("Vestigial column population complete");
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
        // activity_summaries, ecg_readings, ecg_samples, route_points,
        // workout_metadata, workout_routes, imports = 12
        assert_eq!(count, 12);
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
        assert_eq!(count, 12);
    }

    #[test]
    fn populate_workout_vestigial_columns_aggregates_energy_and_distance() {
        let conn = setup();
        conn.execute_batch(
            "
            -- Two workouts: one running, one cycling
            INSERT INTO workouts VALUES
              ('wh_run', 'HKWorkoutActivityTypeRunning', 1800, 's',
               NULL, NULL, NULL, NULL,
               'Watch', '11', 'iPhone', '2024-01-01 06:00:00',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00', NULL, 'imp1'),
              ('wh_cyc', 'HKWorkoutActivityTypeCycling', 3600, 's',
               NULL, NULL, NULL, NULL,
               'Watch', '11', 'iPhone', '2024-01-01 07:00:00',
               '2024-01-01 07:00:00', '2024-01-01 08:00:00', NULL, 'imp1');

            INSERT INTO workout_statistics VALUES
              ('wh_run', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 240.5, 'kcal'),
              ('wh_run', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 3.2, 'km'),
              ('wh_cyc', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 07:00:00', '2024-01-01 08:00:00',
               NULL, NULL, NULL, 480.0, 'kcal'),
              ('wh_cyc', 'HKQuantityTypeIdentifierDistanceCycling',
               '2024-01-01 07:00:00', '2024-01-01 08:00:00',
               NULL, NULL, NULL, 18.0, 'km');
            ",
        )
        .unwrap();

        populate_workout_vestigial_columns(&conn).unwrap();

        let (energy, energy_unit, distance, distance_unit): (f64, String, f64, String) = conn
            .query_row(
                "SELECT total_energy_burned, total_energy_unit,
                        total_distance, total_distance_unit
                 FROM workouts WHERE workout_hash = 'wh_run'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert!((energy - 240.5).abs() < 1e-6);
        assert_eq!(energy_unit, "kcal");
        assert!((distance - 3.2).abs() < 1e-6);
        assert_eq!(distance_unit, "km");

        let (energy_cyc, distance_cyc): (f64, f64) = conn
            .query_row(
                "SELECT total_energy_burned, total_distance
                 FROM workouts WHERE workout_hash = 'wh_cyc'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!((energy_cyc - 480.0).abs() < 1e-6);
        assert!((distance_cyc - 18.0).abs() < 1e-6);
    }

    #[test]
    fn populate_workout_vestigial_columns_leaves_existing_values_alone() {
        let conn = setup();
        conn.execute_batch(
            "
            INSERT INTO workouts VALUES
              ('wh_legacy', 'HKWorkoutActivityTypeRunning', 1800, 's',
               5.0, 'mi', 300.0, 'kcal',
               'Watch', '10', 'iPhone', '2020-01-01 06:00:00',
               '2020-01-01 06:00:00', '2020-01-01 06:30:00', NULL, 'imp1');

            INSERT INTO workout_statistics VALUES
              ('wh_legacy', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2020-01-01 06:00:00', '2020-01-01 06:30:00',
               NULL, NULL, NULL, 999.0, 'kcal'),
              ('wh_legacy', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2020-01-01 06:00:00', '2020-01-01 06:30:00',
               NULL, NULL, NULL, 99.0, 'km');
            ",
        )
        .unwrap();

        populate_workout_vestigial_columns(&conn).unwrap();

        let (energy, distance, distance_unit): (f64, f64, String) = conn
            .query_row(
                "SELECT total_energy_burned, total_distance, total_distance_unit
                 FROM workouts WHERE workout_hash = 'wh_legacy'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        // Pre-iOS-11 exports already carry the values on the Workout element;
        // statistics-derived aggregates must not overwrite them.
        assert!((energy - 300.0).abs() < 1e-6);
        assert!((distance - 5.0).abs() < 1e-6);
        assert_eq!(distance_unit, "mi");
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
    fn deduplicate_collapses_workout_statistics_on_reimport() {
        // Re-importing the same export used to leave workout_statistics with
        // every row duplicated. populate_workout_vestigial_columns SUM()s
        // across the table, so the duplication leaked into workouts.
        // total_distance and total_energy_burned, multiplying them by the
        // number of imports. This regression test fixes the dedupe at the
        // statistics layer so re-imports stay idempotent.
        let conn = setup();
        conn.execute_batch(
            "
            INSERT INTO workout_statistics VALUES
              ('wh1', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 10:00:00', '2024-01-01 10:30:00',
               NULL, NULL, NULL, 300.0, 'kcal'),
              ('wh1', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 10:00:00', '2024-01-01 10:30:00',
               NULL, NULL, NULL, 300.0, 'kcal'),
              ('wh1', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2024-01-01 10:00:00', '2024-01-01 10:30:00',
               NULL, NULL, NULL, 5.0, 'km'),
              ('wh1', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2024-01-01 10:00:00', '2024-01-01 10:30:00',
               NULL, NULL, NULL, 5.0, 'km');
            ",
        )
        .unwrap();

        deduplicate_tables(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workout_statistics", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn deduplicate_collapses_workout_events_on_reimport() {
        let conn = setup();
        conn.execute_batch(
            "
            INSERT INTO workout_events VALUES
              ('wh1', 'HKWorkoutEventTypeLap',
               '2024-01-01 10:15:00', NULL, NULL),
              ('wh1', 'HKWorkoutEventTypeLap',
               '2024-01-01 10:15:00', NULL, NULL),
              ('wh1', 'HKWorkoutEventTypePause',
               '2024-01-01 10:20:00', 5.0, 'min');
            ",
        )
        .unwrap();

        deduplicate_tables(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workout_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn vestigial_column_backfill_stays_idempotent_under_duplicate_stats() {
        // End-to-end guard: even if workout_statistics contained the
        // duplicate set from a stale import (e.g. an import that happened
        // before this dedupe shipped), running deduplicate_tables before
        // populate_workout_vestigial_columns must produce the original
        // unit value, not a multiple of it.
        let conn = setup();
        conn.execute_batch(
            "
            INSERT INTO workouts VALUES
              ('wh_run', 'HKWorkoutActivityTypeRunning', 1800, 's',
               NULL, NULL, NULL, NULL,
               'Watch', '11', 'iPhone', '2024-01-01 06:00:00',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00', NULL, 'imp1');

            INSERT INTO workout_statistics VALUES
              ('wh_run', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 240.5, 'kcal'),
              ('wh_run', 'HKQuantityTypeIdentifierActiveEnergyBurned',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 240.5, 'kcal'),
              ('wh_run', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 3.2, 'km'),
              ('wh_run', 'HKQuantityTypeIdentifierDistanceWalkingRunning',
               '2024-01-01 06:00:00', '2024-01-01 06:30:00',
               NULL, NULL, NULL, 3.2, 'km');
            ",
        )
        .unwrap();

        deduplicate_tables(&conn).unwrap();
        populate_workout_vestigial_columns(&conn).unwrap();

        let (energy, distance): (f64, f64) = conn
            .query_row(
                "SELECT total_energy_burned, total_distance
                 FROM workouts WHERE workout_hash = 'wh_run'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        // Without dedupe of workout_statistics these would come back as
        // 481.0 and 6.4 (2x the truth). With dedupe the values are exact.
        assert!((energy - 240.5).abs() < 1e-6);
        assert!((distance - 3.2).abs() < 1e-6);
    }

    #[test]
    fn open_db_in_memory_works() {
        let conn = open_db_in_memory().unwrap();
        let result: i64 = conn.query_row("SELECT 1", [], |row| row.get(0)).unwrap();
        assert_eq!(result, 1);
    }
}
