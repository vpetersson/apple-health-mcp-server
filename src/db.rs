use anyhow::Result;
use duckdb::{AccessMode, Config, Connection};
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// How long [`open_db`] waits for another process to release the database.
///
/// The MCP server holds the file only for the length of a single query, so a
/// conflict means an import started while a query happened to be in flight.
/// Waiting a few seconds rides that out; waiting longer would just hang an
/// import behind a client that is genuinely busy.
const WRITE_LOCK_WAIT: Duration = Duration::from_secs(10);

/// DuckDB allows exactly one writer and no concurrent readers *across
/// processes*, and reports the clash in the error message — there is no
/// distinct error code to match on.
pub fn is_lock_conflict(err: &anyhow::Error) -> bool {
    err.to_string().contains("Conflicting lock")
}

/// Open the database read-write, waiting out a reader that is mid-query.
pub fn open_db(db_path: &Path) -> Result<Connection> {
    let deadline = Instant::now() + WRITE_LOCK_WAIT;
    let mut waited = false;
    loop {
        match open_db_once(db_path) {
            Ok(conn) => {
                if waited {
                    info!("Database lock released, continuing");
                }
                return Ok(conn);
            }
            Err(e) if is_lock_conflict(&e) && Instant::now() < deadline => {
                if !waited {
                    warn!(
                        "Database is locked by another process (an MCP client mid-query?); \
                         waiting up to {}s",
                        WRITE_LOCK_WAIT.as_secs()
                    );
                    waited = true;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            Err(e) if is_lock_conflict(&e) => {
                return Err(e.context(
                    "another process is holding the database open; stop it and retry the import",
                ));
            }
            Err(e) => return Err(e),
        }
    }
}

fn open_db_once(db_path: &Path) -> Result<Connection> {
    let config = Config::default().access_mode(AccessMode::ReadWrite)?;
    let conn = Connection::open_with_flags(db_path, config)?;
    conn.execute_batch("PRAGMA threads=4;")?;
    Ok(conn)
}

/// Open the database read-only.
///
/// This still takes a lock, so callers must not hold the connection open any
/// longer than a single query — see `server::Database`.
pub fn open_db_readonly(db_path: &Path) -> Result<Connection> {
    let config = Config::default().access_mode(AccessMode::ReadOnly)?;
    let conn = Connection::open_with_flags(db_path, config)?;
    Ok(conn)
}

/// Every table the import writes, as `(name, column definitions)`.
///
/// Tables carry no PRIMARY KEY constraints so the Appender can bulk-load them;
/// deduplication happens in [`deduplicate_tables`] after loading. The Appender
/// binds values by *position*, so this list is equally the contract the flush
/// functions in `import::xml` are written against — see [`ensure_schema`] for
/// what happens when a database on disk disagrees with it.
const TABLES: &[(&str, &str)] = &[
    (
        "records",
        "record_hash     VARCHAR,
         record_type     VARCHAR NOT NULL,
         value           DOUBLE,
         unit            VARCHAR,
         source_name     VARCHAR,
         source_version  VARCHAR,
         device          VARCHAR,
         creation_date   TIMESTAMP,
         start_date      TIMESTAMP NOT NULL,
         end_date        TIMESTAMP NOT NULL,
         import_id       VARCHAR NOT NULL",
    ),
    (
        "record_metadata",
        "record_hash     VARCHAR NOT NULL,
         key             VARCHAR NOT NULL,
         value           VARCHAR",
    ),
    (
        "workouts",
        "workout_hash         VARCHAR,
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
         import_id            VARCHAR NOT NULL",
    ),
    (
        "workout_events",
        "workout_hash    VARCHAR NOT NULL,
         event_type      VARCHAR NOT NULL,
         date            TIMESTAMP,
         duration        DOUBLE,
         duration_unit   VARCHAR",
    ),
    (
        "workout_statistics",
        "workout_hash    VARCHAR NOT NULL,
         stat_type       VARCHAR NOT NULL,
         start_date      TIMESTAMP,
         end_date        TIMESTAMP,
         average         DOUBLE,
         minimum         DOUBLE,
         maximum         DOUBLE,
         sum             DOUBLE,
         unit            VARCHAR",
    ),
    (
        "activity_summaries",
        "date_components          VARCHAR,
         active_energy_burned     DOUBLE,
         active_energy_burned_goal DOUBLE,
         apple_move_time          DOUBLE,
         apple_move_time_goal     DOUBLE,
         apple_exercise_time      DOUBLE,
         apple_exercise_time_goal DOUBLE,
         apple_stand_hours        DOUBLE,
         apple_stand_hours_goal   DOUBLE,
         import_id                VARCHAR NOT NULL",
    ),
    (
        "ecg_readings",
        "ecg_hash         VARCHAR,
         recorded_date    TIMESTAMP NOT NULL,
         classification   VARCHAR,
         device           VARCHAR,
         sample_rate_hz   DOUBLE,
         symptoms         VARCHAR,
         software_version VARCHAR,
         import_id        VARCHAR NOT NULL",
    ),
    (
        "ecg_samples",
        "ecg_hash    VARCHAR NOT NULL,
         sample_idx  INTEGER NOT NULL,
         voltage_uv  DOUBLE NOT NULL",
    ),
    (
        "route_points",
        "point_hash    VARCHAR,
         workout_hash  VARCHAR,
         latitude      DOUBLE NOT NULL,
         longitude     DOUBLE NOT NULL,
         elevation     DOUBLE,
         timestamp     TIMESTAMP NOT NULL,
         speed         DOUBLE,
         course        DOUBLE,
         h_accuracy    DOUBLE,
         v_accuracy    DOUBLE,
         import_id     VARCHAR NOT NULL",
    ),
    (
        "imports",
        "import_id    VARCHAR,
         export_dir   VARCHAR NOT NULL,
         imported_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
         record_count BIGINT,
         workout_count BIGINT,
         duration_secs DOUBLE",
    ),
];

fn create_table_sql(name: &str, columns: &str) -> String {
    format!("CREATE TABLE IF NOT EXISTS {name} ({columns});")
}

/// A column as [`reconcile_schema`] compares it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Create the schema, and bring a database written by an older version up to it.
///
/// `CREATE TABLE IF NOT EXISTS` leaves an existing table exactly as it is, so a
/// database whose tables predate the current [`TABLES`] keeps whatever columns
/// it was created with. The Appender binds by position, so a single drifted
/// column makes every value land in its neighbour's, and DuckDB reports it as a
/// cast failure — `invalid timestamp field format: "import_20260101_120000"` is
/// what `import_id` looks like when it lands in a `TIMESTAMP` column. Nothing in
/// that message points at the schema, so any table that no longer matches is
/// rebuilt here instead, carrying its rows across by column name where they fit.
pub fn ensure_schema(conn: &Connection) -> Result<()> {
    for (name, columns) in TABLES {
        conn.execute_batch(&create_table_sql(name, columns))?;
    }
    reconcile_schema(conn)
}

fn reconcile_schema(conn: &Connection) -> Result<()> {
    // Read the expectation back out of a scratch database built from `TABLES`,
    // so it can never drift from the DDL it is being compared against.
    let scratch = Connection::open_in_memory()?;
    for (name, columns) in TABLES {
        scratch.execute_batch(&create_table_sql(name, columns))?;
    }

    for (name, columns) in TABLES {
        let expected = read_columns(&scratch, name)?;
        let found = read_columns(conn, name)?;
        // Compare names and types only: `deduplicate_tables` rebuilds these
        // tables with `CREATE OR REPLACE TABLE ... AS SELECT`, which drops NOT
        // NULL and DEFAULT. Treating that as drift would rebuild every table on
        // every import.
        let drifted = found.len() != expected.len()
            || found
                .iter()
                .zip(&expected)
                .any(|(f, e)| f.name != e.name || f.data_type != e.data_type);
        if drifted {
            rebuild_table(conn, name, columns, &found, &expected)?;
        }
    }
    Ok(())
}

fn read_columns(conn: &Connection, table: &str) -> Result<Vec<Column>> {
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_schema = 'main' AND table_name = ?
         ORDER BY ordinal_position",
    )?;
    let rows = stmt.query_map([table], |row| {
        Ok(Column {
            name: row.get(0)?,
            data_type: row.get(1)?,
            nullable: row.get::<_, String>(2)? == "YES",
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

/// Replace a drifted table with one matching `columns`, keeping whatever rows
/// survive a by-name copy.
///
/// Rows are only carried across when every NOT NULL column of the new table has
/// a counterpart in the old one — otherwise there is nothing to put in it. Rows
/// that are dropped are not lost work: every table here is derived from the
/// export that the caller is about to re-read.
fn rebuild_table(
    conn: &Connection,
    name: &str,
    columns: &str,
    found: &[Column],
    expected: &[Column],
) -> Result<()> {
    warn!(
        "Table `{}` was written by an older version of this tool (found {}, expected {}); rebuilding it",
        name,
        describe(found),
        describe(expected)
    );

    // An index would block the rename, and `deduplicate_tables` recreates the
    // ones this tool owns at the end of the import anyway.
    drop_indexes(conn, name)?;
    let backup = format!("{name}__schema_drift");
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS \"{backup}\";
         ALTER TABLE \"{name}\" RENAME TO \"{backup}\";"
    ))?;
    conn.execute_batch(&create_table_sql(name, columns))?;

    let shared: Vec<&Column> = expected
        .iter()
        .filter(|e| found.iter().any(|f| f.name == e.name))
        .collect();
    let recoverable = expected
        .iter()
        .filter(|e| !e.nullable)
        .all(|e| shared.iter().any(|s| s.name == e.name));

    if recoverable && !shared.is_empty() {
        let names = shared
            .iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let values = shared
            .iter()
            .map(|c| format!("TRY_CAST(\"{}\" AS {})", c.name, c.data_type))
            .collect::<Vec<_>>()
            .join(", ");
        // A value the new column type cannot hold becomes NULL, which a NOT NULL
        // column would reject for the whole copy — skip those rows instead.
        let keep = shared
            .iter()
            .filter(|c| !c.nullable)
            .map(|c| format!("TRY_CAST(\"{}\" AS {}) IS NOT NULL", c.name, c.data_type))
            .collect::<Vec<_>>();
        let filter = if keep.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", keep.join(" AND "))
        };
        conn.execute_batch(&format!(
            "INSERT INTO \"{name}\" ({names}) SELECT {values} FROM \"{backup}\"{filter};"
        ))?;
        let kept: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{name}\""), [], |row| {
            row.get(0)
        })?;
        info!("Carried {} rows across the rebuild of `{}`", kept, name);
    } else {
        warn!(
            "Rows in `{}` do not fit the current schema; they will be repopulated from this export",
            name
        );
    }

    conn.execute_batch(&format!("DROP TABLE \"{backup}\";"))?;
    Ok(())
}

fn drop_indexes(conn: &Connection, table: &str) -> Result<()> {
    let mut stmt = conn.prepare("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?")?;
    let names: Vec<String> = stmt
        .query_map([table], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for index in names {
        conn.execute_batch(&format!("DROP INDEX IF EXISTS \"{index}\";"))?;
    }
    Ok(())
}

fn describe(columns: &[Column]) -> String {
    if columns.is_empty() {
        return "no columns".to_string();
    }
    columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.data_type))
        .collect::<Vec<_>>()
        .join(", ")
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

    /// The layout `ensure_schema` is supposed to leave behind.
    fn expected_records_columns() -> Vec<Column> {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        read_columns(&conn, "records").unwrap()
    }

    #[test]
    fn drifted_table_is_rebuilt() {
        let conn = open_db_in_memory().unwrap();
        // A `records` table from an older schema: a trailing `imported_at
        // TIMESTAMP` where the current one has `import_id VARCHAR`. Appending a
        // row positionally into this put the import id into a TIMESTAMP column.
        conn.execute_batch(
            "CREATE TABLE records (
                record_hash VARCHAR, record_type VARCHAR, value DOUBLE, unit VARCHAR,
                source_name VARCHAR, source_version VARCHAR, device VARCHAR,
                creation_date TIMESTAMP, start_date TIMESTAMP, end_date TIMESTAMP,
                imported_at TIMESTAMP);",
        )
        .unwrap();

        ensure_schema(&conn).unwrap();

        assert_eq!(
            read_columns(&conn, "records").unwrap(),
            expected_records_columns()
        );

        // The append that used to fail with "invalid timestamp field format".
        let mut appender = conn.appender("records").unwrap();
        appender
            .append_row(duckdb::params![
                "hash1",
                "HeartRate",
                72.0_f64,
                "count/min",
                "Watch",
                "1.0",
                None::<String>,
                None::<String>,
                "2024-01-01 00:00:00",
                "2024-01-01 00:01:00",
                "import_20260101_120000",
            ])
            .unwrap();
        appender.flush().unwrap();
    }

    #[test]
    fn rebuild_keeps_rows_the_new_schema_can_hold() {
        let conn = open_db_in_memory().unwrap();
        // Every current column is present; only a stray extra one has to go.
        conn.execute_batch(
            "CREATE TABLE records (
                record_hash VARCHAR, record_type VARCHAR, value DOUBLE, unit VARCHAR,
                source_name VARCHAR, source_version VARCHAR, device VARCHAR,
                creation_date TIMESTAMP, start_date TIMESTAMP, end_date TIMESTAMP,
                import_id VARCHAR, correlation_hash VARCHAR);
             INSERT INTO records VALUES ('h1', 'HeartRate', 72.0, 'count/min', 'Watch', '1.0',
                NULL, NULL, '2024-01-01 08:00:00', '2024-01-01 08:01:00', 'imp1', 'corr1');
             INSERT INTO records VALUES ('h2', 'StepCount', 100.0, 'count', 'Phone', '1.0',
                NULL, NULL, '2024-01-02 08:00:00', '2024-01-02 08:01:00', 'imp1', NULL);",
        )
        .unwrap();

        ensure_schema(&conn).unwrap();

        assert_eq!(
            read_columns(&conn, "records").unwrap(),
            expected_records_columns()
        );
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, 2);
        let import_id: String = conn
            .query_row(
                "SELECT import_id FROM records WHERE record_hash = 'h1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(import_id, "imp1");
    }

    #[test]
    fn rebuild_drops_rows_that_cannot_be_carried_across() {
        let conn = open_db_in_memory().unwrap();
        // No `import_id` to carry over, and it is NOT NULL in the new table.
        conn.execute_batch(
            "CREATE TABLE records (
                record_hash VARCHAR, record_type VARCHAR, value DOUBLE, unit VARCHAR,
                source_name VARCHAR, source_version VARCHAR, device VARCHAR,
                creation_date TIMESTAMP, start_date TIMESTAMP, end_date TIMESTAMP,
                imported_at TIMESTAMP);
             INSERT INTO records VALUES ('h1', 'HeartRate', 72.0, 'count/min', 'Watch', '1.0',
                NULL, NULL, '2024-01-01 08:00:00', '2024-01-01 08:01:00', '2024-01-01 09:00:00');",
        )
        .unwrap();

        ensure_schema(&conn).unwrap();

        assert_eq!(
            read_columns(&conn, "records").unwrap(),
            expected_records_columns()
        );
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, 0);
        // The scratch copy must not be left behind.
        let leftovers: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%__schema_drift'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn deduplicated_tables_are_not_treated_as_drift() {
        // `deduplicate_tables` rebuilds every table with CREATE OR REPLACE
        // TABLE ... AS SELECT, which drops NOT NULL and DEFAULT. The next
        // import must not read that as drift and throw the rows away.
        let conn = setup();
        conn.execute_batch(
            "INSERT INTO records VALUES ('hash1', 'HeartRate', 72.0, 'count/min', 'Watch', '1.0', NULL, NULL, '2024-01-01 00:00:00', '2024-01-01 00:01:00', 'imp1');",
        )
        .unwrap();
        deduplicate_tables(&conn).unwrap();

        ensure_schema(&conn).unwrap();

        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, 1);
    }

    #[test]
    fn indexed_table_can_be_rebuilt() {
        // `deduplicate_tables` leaves indexes on `records`; they would
        // otherwise block the rename the rebuild goes through.
        let conn = open_db_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE records (
                record_hash VARCHAR, record_type VARCHAR, value DOUBLE, unit VARCHAR,
                source_name VARCHAR, source_version VARCHAR, device VARCHAR,
                creation_date TIMESTAMP, start_date TIMESTAMP, end_date TIMESTAMP,
                imported_at TIMESTAMP);
             CREATE INDEX idx_records_type_date ON records(record_type, start_date);",
        )
        .unwrap();

        ensure_schema(&conn).unwrap();

        assert_eq!(
            read_columns(&conn, "records").unwrap(),
            expected_records_columns()
        );
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
