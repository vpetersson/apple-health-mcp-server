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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tools::*;

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

    fn query_to_json(&self, sql: &str, params: &[&dyn duckdb::ToSql]) -> Result<Value, String> {
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
    #[tool(description = "List all available health record types with counts and date ranges. Use this first to discover what data is available. Returns: type (e.g. HKQuantityTypeIdentifierHeartRate, HKQuantityTypeIdentifierStepCount), count, unit, earliest_date, latest_date.")]
    async fn list_record_types(&self) -> String {
        let sql = "SELECT record_type as type, COUNT(*) as count, unit, MIN(start_date) as earliest_date, MAX(start_date) as latest_date FROM records GROUP BY record_type, unit ORDER BY count DESC";
        match self.query_to_json(sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(description = "Query individual health records. Returns: record_hash, record_type, value (numeric measurement), unit, source_name, start_date, end_date. Record types use Apple's HK identifiers (e.g. HKQuantityTypeIdentifierHeartRate). Use list_record_types first to discover available types.")]
    async fn query_records(&self, params: Parameters<QueryRecordsParams>) -> String {
        let Parameters(params) = params;
        let limit = params.limit.unwrap_or(100).min(1000);
        let mut sql = String::from(
            "SELECT record_hash, record_type, value, unit, source_name, start_date, end_date FROM records WHERE record_type = ?",
        );
        let record_type = params.record_type;

        if let Some(ref sd) = params.start_date {
            sql.push_str(&format!(
                " AND start_date >= '{}'",
                sd.replace('\'', "''")
            ));
        }
        if let Some(ref ed) = params.end_date {
            sql.push_str(&format!(" AND end_date <= '{}'", ed.replace('\'', "''")));
        }
        if let Some(ref sn) = params.source_name {
            sql.push_str(&format!(
                " AND source_name = '{}'",
                sn.replace('\'', "''")
            ));
        }
        sql.push_str(&format!(" ORDER BY start_date DESC LIMIT {}", limit));

        match self.query_to_json(&sql, &[&record_type as &dyn duckdb::ToSql]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(description = "Get aggregated statistics for a record type over time periods. Returns: period, count, avg_value, min_value, max_value, sum_value. Uses pre-computed daily_record_stats table for fast aggregation. Prefer this over query_records for trends and summaries.")]
    async fn get_record_statistics(
        &self,
        params: Parameters<GetRecordStatisticsParams>,
    ) -> String {
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

    #[tool(description = "List workouts with optional filtering. Returns: workout_hash, activity_type (e.g. HKWorkoutActivityTypeRunning), duration, duration_unit, total_distance, total_distance_unit, total_energy_burned, total_energy_unit, source_name, start_date, end_date. Use workout_hash with get_workout_details or get_workout_route.")]
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
            sql.push_str(&format!(
                " AND start_date >= '{}'",
                sd.replace('\'', "''")
            ));
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

    #[tool(description = "Get full workout details by workout_hash. Returns: workout object (all fields), events (lap/pause markers), statistics (per-metric breakdowns like heart rate zones), and has_route boolean. Get the workout_hash from list_workouts.")]
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

    #[tool(description = "Get Apple Watch activity ring data. Returns: date_components, active_energy_burned, active_energy_burned_goal, apple_exercise_time, apple_exercise_time_goal, apple_stand_hours, apple_stand_hours_goal. Values are in kcal, minutes, and hours respectively.")]
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
        sql.push_str(&format!(
            " ORDER BY date_components DESC LIMIT {}",
            limit
        ));

        match self.query_to_json(&sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(description = "Get GPS route data for a workout. Returns array of: latitude, longitude, elevation (meters), timestamp, speed (m/s), course (degrees). Use get_workout_details first to check has_route.")]
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

    #[tool(description = "List ECG recordings. Returns: ecg_hash, recorded_date, classification (e.g. SinusRhythm, AtrialFibrillation), device, sample_rate_hz. Use ecg_hash with get_ecg_data.")]
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

    #[tool(description = "Get full ECG waveform by ecg_hash. Returns: reading (metadata), sample_count, voltages_uv (array of voltage values in microvolts). Get ecg_hash from list_ecg_readings.")]
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

    #[tool(description = "Run a read-only SQL query (DuckDB dialect). Must start with SELECT or WITH. Tables: records (record_hash, record_type, value, unit, source_name, device, start_date, end_date), workouts (workout_hash, activity_type, duration, total_distance, total_energy_burned, start_date, end_date), workout_events, workout_statistics, activity_summaries, ecg_readings, ecg_samples, route_points (latitude, longitude, elevation, timestamp, speed), daily_record_stats (record_type, date, unit, count, avg_value, min_value, max_value, sum_value), record_metadata (record_hash, key, value), imports.")]
    async fn run_custom_query(&self, params: Parameters<RunCustomQueryParams>) -> String {
        let Parameters(params) = params;
        let trimmed = params.query.trim().to_string();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") && !upper.starts_with("WITH") {
            return "Error: Query must start with SELECT or WITH".to_string();
        }

        match self.query_to_json(&trimmed, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(description = "List all devices and apps that contributed health data. Returns: source_name, record_count, earliest_date, latest_date.")]
    async fn list_data_sources(&self) -> String {
        let sql = "SELECT source_name, COUNT(*) as record_count, MIN(start_date) as earliest_date, MAX(start_date) as latest_date FROM records GROUP BY source_name ORDER BY record_count DESC";
        match self.query_to_json(sql, &[]) {
            Ok(result) => serde_json::to_string_pretty(&result).unwrap_or_default(),
            Err(e) => format!("Error: {}", e),
        }
    }

    #[tool(description = "List all data imports. Returns: import_id, export_dir, imported_at, record_count, workout_count, duration_secs.")]
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
    service.waiting().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn run_http_server(db_path: &Path, host: &str, port: u16) -> Result<()> {
    let db_path = db_path.to_path_buf();

    let service = StreamableHttpService::new(
        move || {
            HealthServer::new(&db_path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        },
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
