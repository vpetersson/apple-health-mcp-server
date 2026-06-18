pub mod tools;

use anyhow::Result;
use duckdb::types::ValueRef;
use duckdb::Connection;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::StreamableHttpService;
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use serde_json::{json, Value};
use sqlparser::ast::{Expr, ObjectName, Statement, TableFactor, Visit, Visitor};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tools::*;

/// Built-in DuckDB functions that can read host files or hit external networks.
/// `run_custom_query` is exposed to LLM-generated SQL, so even a read-only
/// connection must reject these because the data leaves through the query
/// result. See issues #1 and #2.
const DENIED_FUNCTIONS: &[&str] = &[
    "read_text",
    "read_text_auto",
    "read_blob",
    "read_blob_auto",
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_json",
    "read_json_auto",
    "read_ndjson",
    "read_ndjson_auto",
    "glob",
];

/// AST visitor that fails as soon as a denylisted function call is seen.
/// Covers scalar calls (`Expr::Function`), table-valued calls via the
/// `relation` hook (`TableFactor::Table::name` has `#[visit(with = ...)]`),
/// and `TableFactor::Function { name }` (the `LATERAL <fn>(...)` form) which
/// the relation hook does NOT visit.
struct DeniedFunctionVisitor {
    denied: Option<String>,
}

impl DeniedFunctionVisitor {
    fn check_name(&mut self, name: &ObjectName) -> ControlFlow<()> {
        if let Some(last) = name.0.last() {
            let Some(ident) = last.as_ident() else {
                return ControlFlow::Continue(());
            };
            if DENIED_FUNCTIONS
                .iter()
                .any(|f| ident.value.eq_ignore_ascii_case(f))
            {
                self.denied = Some(ident.value.clone());
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

impl Visitor for DeniedFunctionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            self.check_name(&func.name)?;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        // `FROM read_text('/etc/passwd')` reaches us here.
        self.check_name(relation)
    }

    fn pre_visit_table_factor(&mut self, tf: &TableFactor) -> ControlFlow<Self::Break> {
        // `FROM LATERAL read_text('/etc/passwd')` parses as TableFactor::Function
        // whose `name` field has no #[visit(with = visit_relation)] annotation,
        // so it is NOT reached by pre_visit_relation. Match it explicitly here.
        // The `Table` arm is intentionally redundant with pre_visit_relation —
        // belt-and-suspenders against AST refactors.
        match tf {
            TableFactor::Table { name, .. } => self.check_name(name)?,
            TableFactor::Function { name, .. } => self.check_name(name)?,
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Parse `sql` with the DuckDB dialect and reject anything that is not a
/// single read-only SELECT / WITH query free of dangerous built-ins. Returns
/// the human-readable error message that should be sent back to the MCP
/// client when validation fails.
pub(crate) fn validate_query(sql: &str) -> Result<(), String> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|e| format!("SQL parse error: {e}"))?;

    if statements.is_empty() {
        return Err("Query is empty".to_string());
    }
    if statements.len() > 1 {
        return Err(format!(
            "Only a single SQL statement is allowed (got {})",
            statements.len()
        ));
    }

    let stmt = &statements[0];
    if !matches!(stmt, Statement::Query(_)) {
        return Err(
            "Only SELECT / WITH queries are allowed (DDL, DML, ATTACH, COPY, \
             INSTALL, LOAD, PRAGMA, etc. are rejected)"
                .to_string(),
        );
    }

    let mut visitor = DeniedFunctionVisitor { denied: None };
    if let ControlFlow::Break(()) = stmt.visit(&mut visitor) {
        // Break() implies the visitor set `denied`.
        if let Some(name) = visitor.denied {
            return Err(format!(
                "Function '{name}' is not allowed (reads host files or external resources)"
            ));
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct HealthServer {
    db_path: PathBuf,
    conn: Arc<Mutex<Connection>>,
    tool_router: ToolRouter<Self>,
}

impl std::fmt::Debug for HealthServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthServer")
            .field("db_path", &self.db_path)
            .finish()
    }
}

impl HealthServer {
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = crate::db::open_db_readonly(db_path)?;
        Ok(Self {
            db_path: db_path.to_path_buf(),
            conn: Arc::new(Mutex::new(conn)),
            tool_router: Self::tool_router(),
        })
    }

    pub fn new_in_memory(conn: Connection) -> Self {
        Self {
            db_path: PathBuf::from(":memory:"),
            conn: Arc::new(Mutex::new(conn)),
            tool_router: Self::tool_router(),
        }
    }

    pub fn query_to_json(&self, sql: &str, params: &[&dyn duckdb::ToSql]) -> Result<Value, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;

        let rows = stmt
            .query_map(params, |row| {
                let column_count = row.as_ref().column_count();
                let mut map = serde_json::Map::new();
                for i in 0..column_count {
                    let name = row
                        .as_ref()
                        .column_name(i)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| format!("col{}", i));
                    let val = match row.get_ref(i) {
                        Ok(ValueRef::Null) => continue,
                        Ok(ValueRef::Boolean(b)) => Value::Bool(b),
                        Ok(ValueRef::TinyInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::SmallInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::Int(n)) => Value::Number(n.into()),
                        Ok(ValueRef::BigInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::HugeInt(n)) => {
                            // HugeInt may exceed JSON number range, use string
                            if let Ok(n64) = i64::try_from(n) {
                                Value::Number(n64.into())
                            } else {
                                Value::String(n.to_string())
                            }
                        }
                        Ok(ValueRef::UTinyInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::USmallInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::UInt(n)) => Value::Number(n.into()),
                        Ok(ValueRef::UBigInt(n)) => {
                            if let Ok(n64) = i64::try_from(n) {
                                Value::Number(n64.into())
                            } else {
                                Value::String(n.to_string())
                            }
                        }
                        Ok(ValueRef::Float(f)) => serde_json::Number::from_f64(f as f64)
                            .map(Value::Number)
                            .unwrap_or(Value::String(f.to_string())),
                        Ok(ValueRef::Double(f)) => serde_json::Number::from_f64(f)
                            .map(Value::Number)
                            .unwrap_or(Value::String(f.to_string())),
                        Ok(ValueRef::Text(bytes)) => {
                            Value::String(String::from_utf8_lossy(bytes).into_owned())
                        }
                        Ok(_) => {
                            // Timestamp, Date32, Time64, Decimal, Interval, etc.
                            // Fall back to string via DuckDB's own formatting
                            match row.get::<_, String>(i) {
                                Ok(s) => Value::String(s),
                                Err(_) => continue,
                            }
                        }
                        Err(_) => continue,
                    };
                    map.insert(name, val);
                }
                Ok(Value::Object(map))
            })
            .map_err(|e| e.to_string())?;

        let results: Vec<Value> = rows.filter_map(|r| r.ok()).collect();
        Ok(Value::Array(results))
    }
}

#[tool_router]
impl HealthServer {
    #[tool(
        description = "List all available health record types with counts and date ranges. Use this first to discover what data is available. Returns: type (e.g. HKQuantityTypeIdentifierHeartRate, HKQuantityTypeIdentifierStepCount), count, unit, earliest_date, latest_date."
    )]
    async fn list_record_types(&self) -> String {
        let sql = "SELECT record_type as type, COUNT(*) as count, unit, MIN(start_date) as earliest_date, MAX(start_date) as latest_date FROM records GROUP BY record_type, unit ORDER BY count DESC";
        match self.query_to_json(sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "Query individual health records. Returns: record_hash, record_type, value (numeric measurement), unit, source_name, start_date, end_date. Record types use Apple's HK identifiers (e.g. HKQuantityTypeIdentifierHeartRate). Use list_record_types first to discover available types."
    )]
    async fn query_records(&self, params: Parameters<QueryRecordsParams>) -> String {
        let Parameters(params) = params;
        let limit = params.limit.unwrap_or(100).min(1000);
        let mut sql = String::from(
            "SELECT record_hash, record_type, value, unit, source_name, start_date, end_date FROM records WHERE record_type = ?",
        );
        let record_type = params.record_type;

        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(" AND start_date >= '{}'", sd.replace('\'', "''")));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(" AND end_date <= '{}'", ed.replace('\'', "''")));
        }
        if let Some(ref sn) = params.source_name {
            sql.push_str(&format!(" AND source_name = '{}'", sn.replace('\'', "''")));
        }
        sql.push_str(&format!(" ORDER BY start_date DESC LIMIT {}", limit));

        match self.query_to_json(&sql, &[&record_type as &dyn duckdb::ToSql]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "Get aggregated statistics for a record type over time periods. Returns: period, count, avg_value, min_value, max_value, sum_value. Uses pre-computed daily_record_stats table for fast aggregation. Prefer this over query_records for trends and summaries."
    )]
    async fn get_record_statistics(&self, params: Parameters<GetRecordStatisticsParams>) -> String {
        let Parameters(params) = params;
        let period = params.period.as_deref().unwrap_or("day");
        let date_trunc = match period {
            "week" => "DATE_TRUNC('week', date)",
            "month" => "DATE_TRUNC('month', date)",
            "year" => "DATE_TRUNC('year', date)",
            _ => "date",
        };

        let mut sql = format!(
            "SELECT {} as period, SUM(count) as count, \
             SUM(sum_value)/SUM(count) as avg_value, \
             MIN(min_value) as min_value, MAX(max_value) as max_value, \
             SUM(sum_value) as sum_value \
             FROM daily_record_stats WHERE record_type = ?",
            date_trunc
        );

        let record_type = params.record_type;

        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(" AND date >= '{}'", sd.replace('\'', "''")));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(" AND date <= '{}'", ed.replace('\'', "''")));
        }
        sql.push_str(&format!(" GROUP BY {} ORDER BY period", date_trunc));

        match self.query_to_json(&sql, &[&record_type as &dyn duckdb::ToSql]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "List workouts with optional filtering. Returns: workout_hash, activity_type (e.g. HKWorkoutActivityTypeRunning), duration, duration_unit, total_distance, total_distance_unit, total_energy_burned, total_energy_unit, source_name, start_date, end_date. Use workout_hash with get_workout_details or get_workout_route."
    )]
    async fn list_workouts(&self, params: Parameters<ListWorkoutsParams>) -> String {
        let Parameters(params) = params;
        let limit = params.limit.unwrap_or(50).min(500);
        let mut sql = String::from(
            "SELECT workout_hash, activity_type, duration, duration_unit, \
             total_distance, total_distance_unit, total_energy_burned, total_energy_unit, \
             source_name, start_date, end_date FROM workouts WHERE 1=1",
        );

        if let Some(ref at) = params.activity_type {
            sql.push_str(&format!(
                " AND activity_type = '{}'",
                at.replace('\'', "''")
            ));
        }
        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(" AND start_date >= '{}'", sd.replace('\'', "''")));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(" AND end_date <= '{}'", ed.replace('\'', "''")));
        }
        sql.push_str(&format!(" ORDER BY start_date DESC LIMIT {}", limit));

        match self.query_to_json(&sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "Get full workout details by workout_hash. Returns: workout object (all fields), events (lap/pause markers), statistics (per-metric breakdowns like heart rate zones), and has_route boolean. Get the workout_hash from list_workouts."
    )]
    async fn get_workout_details(&self, params: Parameters<GetWorkoutDetailsParams>) -> String {
        let Parameters(params) = params;
        let hash = params.workout_hash;

        let workout = match self.query_to_json(
            "SELECT * FROM workouts WHERE workout_hash = ?",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let events = match self.query_to_json(
            "SELECT event_type, date, duration, duration_unit FROM workout_events WHERE workout_hash = ?",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let statistics = match self.query_to_json(
            "SELECT stat_type, start_date, end_date, average, minimum, maximum, sum, unit FROM workout_statistics WHERE workout_hash = ?",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let has_route = match self.query_to_json(
            "SELECT COUNT(*) as count FROM route_points WHERE workout_hash = ?",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let result = json!({
            "workout": workout.as_array().and_then(|a| a.first()).cloned().unwrap_or(Value::Null),
            "events": events,
            "statistics": statistics,
            "has_route": has_route.as_array().and_then(|a| a.first()).and_then(|r| r.get("count")).and_then(|c| c.as_i64()).unwrap_or(0) > 0,
        });

        serde_json::to_string_pretty(&result).unwrap_or_default()
    }

    #[tool(
        description = "Get Apple Watch activity ring data. Returns: date_components, active_energy_burned, active_energy_burned_goal, apple_exercise_time, apple_exercise_time_goal, apple_stand_hours, apple_stand_hours_goal. Values are in kcal, minutes, and hours respectively."
    )]
    async fn get_activity_summaries(
        &self,
        params: Parameters<GetActivitySummariesParams>,
    ) -> String {
        let Parameters(params) = params;
        let limit = params.limit.unwrap_or(30).min(365);
        let mut sql = String::from("SELECT * FROM activity_summaries WHERE 1=1");

        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(
                " AND date_components >= '{}'",
                sd.replace('\'', "''")
            ));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(
                " AND date_components <= '{}'",
                ed.replace('\'', "''")
            ));
        }
        sql.push_str(&format!(" ORDER BY date_components DESC LIMIT {}", limit));

        match self.query_to_json(&sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "Get GPS route data for a workout. Returns array of: latitude, longitude, elevation (meters), timestamp, speed (m/s), course (degrees). Use get_workout_details first to check has_route."
    )]
    async fn get_workout_route(&self, params: Parameters<GetWorkoutRouteParams>) -> String {
        let Parameters(params) = params;
        match self.query_to_json(
            "SELECT latitude, longitude, elevation, timestamp, speed, course FROM route_points WHERE workout_hash = ? ORDER BY timestamp",
            &[&params.workout_hash as &dyn duckdb::ToSql],
        ) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "List ECG recordings. Returns: ecg_hash, recorded_date, classification (e.g. SinusRhythm, AtrialFibrillation), device, sample_rate_hz. Use ecg_hash with get_ecg_data."
    )]
    async fn list_ecg_readings(&self, params: Parameters<ListEcgReadingsParams>) -> String {
        let Parameters(params) = params;
        let mut sql = String::from(
            "SELECT ecg_hash, recorded_date, classification, device, sample_rate_hz FROM ecg_readings WHERE 1=1",
        );
        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(
                " AND recorded_date >= '{}'",
                sd.replace('\'', "''")
            ));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(
                " AND recorded_date <= '{}'",
                ed.replace('\'', "''")
            ));
        }
        sql.push_str(" ORDER BY recorded_date DESC");

        match self.query_to_json(&sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "Get full ECG waveform by ecg_hash. Returns: reading (metadata), sample_count, voltages_uv (array of voltage values in microvolts). Get ecg_hash from list_ecg_readings."
    )]
    async fn get_ecg_data(&self, params: Parameters<GetEcgDataParams>) -> String {
        let Parameters(params) = params;
        let hash = params.ecg_hash;
        let metadata = match self.query_to_json(
            "SELECT * FROM ecg_readings WHERE ecg_hash = ?",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let samples = match self.query_to_json(
            "SELECT voltage_uv FROM ecg_samples WHERE ecg_hash = ? ORDER BY sample_idx",
            &[&hash as &dyn duckdb::ToSql],
        ) {
            Ok(r) => r,
            Err(e) => return format!("Error: {}", e),
        };

        let voltages: Vec<Value> = samples
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|r| r.get("voltage_uv").cloned())
                    .collect()
            })
            .unwrap_or_default();

        let result = json!({
            "reading": metadata.as_array().and_then(|a| a.first()).cloned().unwrap_or(Value::Null),
            "sample_count": voltages.len(),
            "voltages_uv": voltages,
        });

        serde_json::to_string_pretty(&result).unwrap_or_default()
    }

    #[tool(
        description = "Run a read-only SQL query (DuckDB dialect). Must start with SELECT or WITH. Tables: records (record_hash, record_type, value, unit, source_name, device, start_date, end_date), workouts (workout_hash, activity_type, duration, total_distance, total_energy_burned, start_date, end_date), workout_events, workout_statistics, activity_summaries, ecg_readings, ecg_samples, route_points (latitude, longitude, elevation, timestamp, speed), daily_record_stats (record_type, date, unit, count, avg_value, min_value, max_value, sum_value), record_metadata (record_hash, key, value), imports."
    )]
    async fn run_custom_query(&self, params: Parameters<RunCustomQueryParams>) -> String {
        let Parameters(params) = params;
        let trimmed = params.query.trim().to_string();

        if let Err(msg) = validate_query(&trimmed) {
            return format!("Error: {msg}");
        }

        match self.query_to_json(&trimmed, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "List all devices and apps that contributed health data. Returns: source_name, record_count, earliest_date, latest_date."
    )]
    async fn list_data_sources(&self) -> String {
        let sql = "SELECT source_name, COUNT(*) as record_count, MIN(start_date) as earliest_date, MAX(start_date) as latest_date FROM records GROUP BY source_name ORDER BY record_count DESC";
        match self.query_to_json(sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(
        description = "List all data imports. Returns: import_id, export_dir, imported_at, record_count, workout_count, duration_secs."
    )]
    async fn get_import_history(&self) -> String {
        let sql = "SELECT * FROM imports ORDER BY imported_at DESC";
        match self.query_to_json(sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }
}

#[tool_handler]
impl ServerHandler for HealthServer {}

pub async fn run_server(db_path: &Path, host: &str, port: u16, transport: &str) -> Result<()> {
    match transport {
        "stdio" => run_stdio_server(db_path).await,
        "http" => run_http_server(db_path, host, port).await,
        other => anyhow::bail!("Unknown transport: {other}. Expected \"http\" or \"stdio\"."),
    }
}

async fn run_stdio_server(db_path: &Path) -> Result<()> {
    let server = HealthServer::new(db_path)?;
    tracing::info!("MCP server running on stdio");
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn run_http_server(db_path: &Path, host: &str, port: u16) -> Result<()> {
    let db_path = db_path.to_path_buf();

    let service = StreamableHttpService::new(
        move || HealthServer::new(&db_path).map_err(|e| std::io::Error::other(e.to_string())),
        LocalSessionManager::default().into(),
        Default::default(),
    );

    let router = axum::Router::new().nest_service("/mcp", service);

    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("MCP server listening at http://{}/mcp", addr);

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.unwrap();
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ensure_schema, open_db_in_memory, rebuild_daily_stats};
    use rmcp::handler::server::wrapper::Parameters;

    fn setup_server() -> HealthServer {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        // Seed data
        conn.execute_batch(
            "
            INSERT INTO records VALUES ('rh1', 'HKQuantityTypeIdentifierHeartRate', 72.0, 'count/min', 'Apple Watch', '10.0', NULL, '2024-01-01 08:00:00', '2024-01-01 08:00:00', '2024-01-01 08:01:00', 'imp1');
            INSERT INTO records VALUES ('rh2', 'HKQuantityTypeIdentifierHeartRate', 80.0, 'count/min', 'Apple Watch', '10.0', NULL, '2024-01-01 09:00:00', '2024-01-01 09:00:00', '2024-01-01 09:01:00', 'imp1');
            INSERT INTO records VALUES ('rh3', 'HKQuantityTypeIdentifierStepCount', 1500.0, 'count', 'iPhone', '17.0', NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-01 23:59:59', 'imp1');
            INSERT INTO record_metadata VALUES ('rh1', 'HKMetadataKeyHeartRateMotionContext', '1');
            INSERT INTO workouts VALUES ('wh1', 'HKWorkoutActivityTypeRunning', 1800.0, 'sec', 5000.0, 'm', 300.0, 'kcal', 'Apple Watch', '10.0', NULL, '2024-01-01 10:00:00', '2024-01-01 10:00:00', '2024-01-01 10:30:00', 'imp1');
            INSERT INTO workout_events VALUES ('wh1', 'HKWorkoutEventTypeLap', '2024-01-01 10:15:00', NULL, NULL);
            INSERT INTO workout_statistics VALUES ('wh1', 'HKQuantityTypeIdentifierHeartRate', '2024-01-01 10:00:00', '2024-01-01 10:30:00', 150.0, 120.0, 180.0, NULL, 'count/min');
            INSERT INTO activity_summaries VALUES ('2024-01-01', 500.0, 600.0, 45.0, 30.0, 30.0, 30.0, 10.0, 12.0, 'imp1');
            INSERT INTO ecg_readings VALUES ('ecg1', '2024-01-01 12:00:00', 'Sinus Rhythm', 'Apple Watch', 512.0, NULL, '2.0', 'imp1');
            INSERT INTO ecg_samples VALUES ('ecg1', 0, 100.0);
            INSERT INTO ecg_samples VALUES ('ecg1', 1, 200.0);
            INSERT INTO ecg_samples VALUES ('ecg1', 2, -50.0);
            INSERT INTO route_points VALUES ('rp1', 'wh1', 37.7749, -122.4194, 10.5, '2024-01-01 10:00:00', 3.5, 180.0, 5.0, 3.0, 'imp1');
            INSERT INTO route_points VALUES ('rp2', 'wh1', 37.7750, -122.4195, 11.0, '2024-01-01 10:00:05', 3.6, 181.0, 4.5, 2.8, 'imp1');
            INSERT INTO imports VALUES ('imp1', '/tmp/export', '2024-01-01 00:00:00', 3, 1, 5.0);
            ",
        )
        .unwrap();
        rebuild_daily_stats(&conn).unwrap();
        HealthServer::new_in_memory(conn)
    }

    #[test]
    fn query_to_json_empty_result() {
        let server = setup_server();
        let result = server
            .query_to_json("SELECT * FROM records WHERE 1=0", &[])
            .unwrap();
        assert_eq!(result, Value::Array(vec![]));
    }

    #[test]
    fn query_to_json_with_data() {
        let server = setup_server();
        let result = server
            .query_to_json(
                "SELECT record_type, value FROM records ORDER BY record_hash",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn query_to_json_null_skipped() {
        let server = setup_server();
        let result = server
            .query_to_json("SELECT device FROM records WHERE record_hash = 'rh1'", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        let obj = arr[0].as_object().unwrap();
        // device is NULL, so the key should not be present
        assert!(!obj.contains_key("device"));
    }

    #[test]
    fn query_to_json_bool() {
        let server = setup_server();
        let result = server.query_to_json("SELECT true AS flag", &[]).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr[0].get("flag").unwrap(), &Value::Bool(true));
    }

    #[test]
    fn query_to_json_int_types() {
        let server = setup_server();
        let result = server
            .query_to_json(
                "SELECT 42::TINYINT AS t, 1000::SMALLINT AS s, 100000::INT AS i, 999999999::BIGINT AS b",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        let obj = arr[0].as_object().unwrap();
        assert_eq!(obj["t"], json!(42));
        assert_eq!(obj["s"], json!(1000));
        assert_eq!(obj["i"], json!(100000));
        assert_eq!(obj["b"], json!(999999999));
    }

    #[test]
    fn query_to_json_float_and_double() {
        let server = setup_server();
        let result = server
            .query_to_json("SELECT 3.14::FLOAT AS f, 2.718281828::DOUBLE AS d", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        let obj = arr[0].as_object().unwrap();
        assert!(obj["f"].as_f64().unwrap() > 3.0);
        assert!(obj["d"].as_f64().unwrap() > 2.7);
    }

    #[test]
    fn query_to_json_text() {
        let server = setup_server();
        let result = server.query_to_json("SELECT 'hello' AS msg", &[]).unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr[0].get("msg").unwrap(), "hello");
    }

    #[test]
    fn query_to_json_timestamp_cast() {
        let server = setup_server();
        // Cast timestamp to varchar in SQL so it comes through as Text
        let result = server
            .query_to_json(
                "SELECT CAST(start_date AS VARCHAR) AS sd FROM records WHERE record_hash = 'rh1'",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        let sd = arr[0].get("sd").unwrap().as_str().unwrap();
        assert!(sd.contains("2024-01-01"));
    }

    #[test]
    fn query_to_json_timestamp_raw() {
        // Raw timestamps may be skipped if the DuckDB driver can't convert them to String.
        // This tests that the query still succeeds even with timestamp columns.
        let server = setup_server();
        let result = server
            .query_to_json(
                "SELECT start_date FROM records WHERE record_hash = 'rh1'",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn query_to_json_invalid_sql() {
        let server = setup_server();
        let result = server.query_to_json("INVALID SQL STATEMENT", &[]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tool_list_record_types() {
        let server = setup_server();
        let result = server.list_record_types().await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2); // HeartRate and StepCount
    }

    #[tokio::test]
    async fn tool_query_records() {
        let server = setup_server();
        let params = Parameters(QueryRecordsParams {
            record_type: "HKQuantityTypeIdentifierHeartRate".to_string(),
            start_date: None,
            end_date: None,
            source_name: None,
            limit: Some(10),
        });
        let result = server.query_records(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn tool_query_records_with_filters() {
        let server = setup_server();
        let params = Parameters(QueryRecordsParams {
            record_type: "HKQuantityTypeIdentifierHeartRate".to_string(),
            start_date: Some("2024-01-01 08:30:00".to_string()),
            end_date: None,
            source_name: Some("Apple Watch".to_string()),
            limit: None,
        });
        let result = server.query_records(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_record_statistics() {
        let server = setup_server();
        let params = Parameters(GetRecordStatisticsParams {
            record_type: "HKQuantityTypeIdentifierHeartRate".to_string(),
            start_date: None,
            end_date: None,
            period: Some("day".to_string()),
        });
        let result = server.get_record_statistics(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert!(!parsed.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tool_list_workouts() {
        let server = setup_server();
        let params = Parameters(ListWorkoutsParams {
            activity_type: None,
            start_date: None,
            end_date: None,
            limit: None,
        });
        let result = server.list_workouts(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_workout_details() {
        let server = setup_server();
        let params = Parameters(GetWorkoutDetailsParams {
            workout_hash: "wh1".to_string(),
        });
        let result = server.get_workout_details(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("workout").unwrap().is_object());
        assert!(parsed.get("events").unwrap().is_array());
        assert!(parsed.get("statistics").unwrap().is_array());
        assert_eq!(parsed.get("has_route").unwrap(), &Value::Bool(true));
    }

    #[tokio::test]
    async fn tool_get_activity_summaries() {
        let server = setup_server();
        let params = Parameters(GetActivitySummariesParams {
            start_date: None,
            end_date: None,
            limit: None,
        });
        let result = server.get_activity_summaries(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_workout_route() {
        let server = setup_server();
        let params = Parameters(GetWorkoutRouteParams {
            workout_hash: "wh1".to_string(),
        });
        let result = server.get_workout_route(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn tool_list_ecg_readings() {
        let server = setup_server();
        let params = Parameters(ListEcgReadingsParams {
            start_date: None,
            end_date: None,
        });
        let result = server.list_ecg_readings(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_ecg_data() {
        let server = setup_server();
        let params = Parameters(GetEcgDataParams {
            ecg_hash: "ecg1".to_string(),
        });
        let result = server.get_ecg_data(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.get("sample_count").unwrap(), 3);
        assert_eq!(
            parsed.get("voltages_uv").unwrap().as_array().unwrap().len(),
            3
        );
    }

    #[tokio::test]
    async fn tool_run_custom_query() {
        let server = setup_server();
        let params = Parameters(RunCustomQueryParams {
            query: "SELECT COUNT(*) as cnt FROM records".to_string(),
        });
        let result = server.run_custom_query(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap()[0].get("cnt").unwrap(), 3);
    }

    #[tokio::test]
    async fn tool_run_custom_query_with_cte() {
        let server = setup_server();
        let params = Parameters(RunCustomQueryParams {
            query: "WITH t AS (SELECT 1 as n) SELECT n FROM t".to_string(),
        });
        let result = server.run_custom_query(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_run_custom_query_rejects_mutation() {
        let server = setup_server();
        let params = Parameters(RunCustomQueryParams {
            query: "DROP TABLE records".to_string(),
        });
        let result = server.run_custom_query(params).await;
        assert!(result.starts_with("Error:"), "got {result}");
    }

    #[tokio::test]
    async fn tool_run_custom_query_rejects_insert() {
        let server = setup_server();
        let params = Parameters(RunCustomQueryParams {
            query: "INSERT INTO records VALUES ('a','b',1,'c','d',NULL,NULL,NULL,'2024-01-01','2024-01-01','x')".to_string(),
        });
        let result = server.run_custom_query(params).await;
        assert!(result.starts_with("Error:"), "got {result}");
    }

    // Issue #2 bypass PoCs from the issue body — every one of these must be
    // rejected by `validate_query`. Also covers issue #1's filesystem-function
    // attack surface, which #2 absorbed.
    #[test]
    fn validate_rejects_multiple_statements() {
        let r = validate_query("SELECT 1; ATTACH 'http://example/x.duckdb' AS x");
        assert!(r.is_err(), "multi-statement must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_with_then_drop() {
        let r = validate_query("WITH _ AS (SELECT 1) DROP TABLE records");
        assert!(r.is_err(), "non-Query statement must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_comment_then_mutation() {
        let r = validate_query("SELECT/*comment*/ * FROM records; DELETE FROM records");
        assert!(r.is_err(), "multi-statement after comment must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_filesystem_functions() {
        for fname in [
            "read_text",
            "read_blob",
            "read_csv",
            "read_csv_auto",
            "read_parquet",
            "read_json",
            "read_json_auto",
            "glob",
        ] {
            let sql = format!("SELECT * FROM {fname}('/etc/passwd')");
            let r = validate_query(&sql);
            assert!(r.is_err(), "{fname} must be rejected, got {r:?} for {sql}");
            let msg = r.unwrap_err();
            assert!(
                msg.to_ascii_lowercase().contains(fname),
                "error must mention the denied function name: {msg}"
            );
        }
    }

    #[test]
    fn validate_rejects_filesystem_function_auto_variants() {
        // `read_text_auto` and `read_blob_auto` are denied as a precaution
        // even though they are not always present in every DuckDB version.
        for fname in ["read_text_auto", "read_blob_auto"] {
            let sql = format!("SELECT * FROM {fname}('/etc/passwd')");
            let r = validate_query(&sql);
            assert!(r.is_err(), "{fname} must be rejected: {r:?}");
        }
    }

    #[test]
    fn validate_rejects_filesystem_function_uppercase() {
        // Casing must not matter — `READ_TEXT` should also be denied.
        let r = validate_query("SELECT READ_TEXT('/etc/passwd')");
        assert!(r.is_err(), "uppercase variant must also be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_lateral_table_function() {
        // Regression test for the LATERAL bypass discovered during review.
        // sqlparser parses this as TableFactor::Function whose `name` is NOT
        // visited via the `relation` hook; only `pre_visit_table_factor`
        // catches it.
        let r = validate_query("SELECT * FROM LATERAL read_text('/etc/passwd')");
        assert!(r.is_err(), "LATERAL table function must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_schema_qualified_function() {
        // `name.0.last()` must take the last segment so a schema-qualified
        // call like `main.read_text(...)` is still recognized.
        let r = validate_query("SELECT * FROM main.read_text('/etc/passwd')");
        assert!(r.is_err(), "schema-qualified denied function must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_attach_statement() {
        let r = validate_query("ATTACH 'http://example/x.duckdb' AS x");
        assert!(r.is_err(), "ATTACH must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_copy_statement() {
        let r = validate_query("COPY records TO '/tmp/out.csv'");
        assert!(r.is_err(), "COPY must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_install_statement() {
        let r = validate_query("INSTALL httpfs");
        assert!(r.is_err(), "INSTALL must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_load_statement() {
        let r = validate_query("LOAD httpfs");
        assert!(r.is_err(), "LOAD must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_pragma_statement() {
        let r = validate_query("PRAGMA database_list");
        assert!(r.is_err(), "PRAGMA must be rejected: {r:?}");
    }

    #[test]
    fn validate_accepts_plain_select() {
        assert!(validate_query("SELECT 1").is_ok());
        assert!(validate_query("SELECT * FROM records WHERE value > 0 LIMIT 10").is_ok());
    }

    #[test]
    fn validate_accepts_with_query() {
        assert!(validate_query("WITH t AS (SELECT 1 AS n) SELECT n FROM t").is_ok());
    }

    #[test]
    fn validate_rejects_garbage_input() {
        let r = validate_query("not even sql");
        assert!(r.is_err(), "garbage must be rejected: {r:?}");
    }

    #[test]
    fn validate_rejects_empty_input() {
        // Empty input / bare `;` should produce a clear message, not
        // "got 0 statements".
        for sql in ["", "   ", ";", "  ;  "] {
            let r = validate_query(sql);
            assert!(r.is_err(), "empty input {sql:?} must be rejected: {r:?}");
        }
    }

    #[tokio::test]
    async fn tool_list_data_sources() {
        let server = setup_server();
        let result = server.list_data_sources().await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2); // Apple Watch, iPhone
    }

    #[tokio::test]
    async fn tool_get_import_history() {
        let server = setup_server();
        let result = server.get_import_history().await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[test]
    fn query_to_json_unsigned_int_types() {
        let server = setup_server();
        let result = server
            .query_to_json(
                "SELECT 42::UTINYINT AS ut, 1000::USMALLINT AS us, 100000::UINTEGER AS ui, 999999999::UBIGINT AS ub",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        let obj = arr[0].as_object().unwrap();
        assert_eq!(obj["ut"], json!(42));
        assert_eq!(obj["us"], json!(1000));
        assert_eq!(obj["ui"], json!(100000));
        assert_eq!(obj["ub"], json!(999999999));
    }

    #[test]
    fn query_to_json_hugeint() {
        let server = setup_server();
        // Small value that fits in i64
        let result = server
            .query_to_json("SELECT 123::HUGEINT AS h", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr[0].get("h").unwrap(), 123);

        // Large value that exceeds i64 range → returned as string
        let result = server
            .query_to_json(
                "SELECT 170141183460469231731687303715884105727::HUGEINT AS h",
                &[],
            )
            .unwrap();
        let arr = result.as_array().unwrap();
        assert!(arr[0].get("h").unwrap().is_string());
    }

    #[test]
    fn query_to_json_ubigint_overflow() {
        let server = setup_server();
        // Value exceeding i64 max → returned as string
        let result = server
            .query_to_json("SELECT 18446744073709551615::UBIGINT AS ub", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        let val = arr[0].get("ub").unwrap();
        assert!(val.is_string());
        assert_eq!(val.as_str().unwrap(), "18446744073709551615");
    }

    #[test]
    fn query_to_json_nan_float() {
        let server = setup_server();
        // NaN cannot be represented in JSON, falls back to string
        let result = server
            .query_to_json("SELECT 'NaN'::FLOAT AS f, 'NaN'::DOUBLE AS d", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        let obj = arr[0].as_object().unwrap();
        assert!(obj["f"].is_string());
        assert!(obj["d"].is_string());
    }

    #[test]
    fn query_to_json_date_type() {
        let server = setup_server();
        // DATE type goes through the catch-all branch
        let result = server
            .query_to_json("SELECT DATE '2024-01-15' AS d", &[])
            .unwrap();
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[tokio::test]
    async fn tool_run_server_invalid_transport() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        // Create a real DB file
        {
            let conn = crate::db::open_db(&db_path).unwrap();
            ensure_schema(&conn).unwrap();
        }

        let result = run_server(&db_path, "127.0.0.1", 0, "invalid").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown transport"));
    }

    #[test]
    fn debug_impl() {
        let server = setup_server();
        let debug = format!("{:?}", server);
        assert!(debug.contains("HealthServer"));
        assert!(debug.contains(":memory:"));
    }

    #[tokio::test]
    async fn tool_list_workouts_with_filters() {
        let server = setup_server();
        let params = Parameters(ListWorkoutsParams {
            activity_type: Some("HKWorkoutActivityTypeRunning".to_string()),
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-12-31".to_string()),
            limit: Some(10),
        });
        let result = server.list_workouts(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_activity_summaries_with_dates() {
        let server = setup_server();
        let params = Parameters(GetActivitySummariesParams {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-12-31".to_string()),
            limit: Some(10),
        });
        let result = server.get_activity_summaries(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_list_ecg_readings_with_dates() {
        let server = setup_server();
        let params = Parameters(ListEcgReadingsParams {
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-12-31".to_string()),
        });
        let result = server.list_ecg_readings(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tool_get_record_statistics_periods() {
        let server = setup_server();
        for period in &["day", "week", "month", "year"] {
            let params = Parameters(GetRecordStatisticsParams {
                record_type: "HKQuantityTypeIdentifierHeartRate".to_string(),
                start_date: None,
                end_date: None,
                period: Some(period.to_string()),
            });
            let result = server.get_record_statistics(params).await;
            let parsed: Value = serde_json::from_str(&result).unwrap();
            assert!(!parsed.as_array().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn tool_get_workout_details_nonexistent() {
        let server = setup_server();
        let params = Parameters(GetWorkoutDetailsParams {
            workout_hash: "nonexistent".to_string(),
        });
        let result = server.get_workout_details(params).await;
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("workout").unwrap().is_null());
        assert_eq!(parsed.get("has_route").unwrap(), &Value::Bool(false));
    }
}
