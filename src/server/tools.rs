use rmcp::schemars;
use serde::Deserialize;

// -- Parameter structs for tools that need them --

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryRecordsParams {
    #[schemars(description = "The health record type to query, e.g. HKQuantityTypeIdentifierHeartRate")]
    pub record_type: String,
    #[schemars(description = "Start date filter (ISO 8601 / YYYY-MM-DD)")]
    pub start_date: Option<String>,
    #[schemars(description = "End date filter (ISO 8601 / YYYY-MM-DD)")]
    pub end_date: Option<String>,
    #[schemars(description = "Filter by source name")]
    pub source_name: Option<String>,
    #[schemars(description = "Maximum number of results (default 100, max 1000)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetRecordStatisticsParams {
    #[schemars(description = "The health record type, e.g. HKQuantityTypeIdentifierHeartRate")]
    pub record_type: String,
    #[schemars(description = "Start date filter (ISO 8601 / YYYY-MM-DD)")]
    pub start_date: Option<String>,
    #[schemars(description = "End date filter (ISO 8601 / YYYY-MM-DD)")]
    pub end_date: Option<String>,
    #[schemars(description = "Aggregation period: day, week, month, or year (default: day)")]
    pub period: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListWorkoutsParams {
    #[schemars(description = "Filter by workout activity type, e.g. HKWorkoutActivityTypeRunning")]
    pub activity_type: Option<String>,
    #[schemars(description = "Start date filter (ISO 8601 / YYYY-MM-DD)")]
    pub start_date: Option<String>,
    #[schemars(description = "End date filter (ISO 8601 / YYYY-MM-DD)")]
    pub end_date: Option<String>,
    #[schemars(description = "Maximum number of results (default 50)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetWorkoutDetailsParams {
    #[schemars(description = "The workout hash identifier")]
    pub workout_hash: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetActivitySummariesParams {
    #[schemars(description = "Start date filter (ISO 8601 / YYYY-MM-DD)")]
    pub start_date: Option<String>,
    #[schemars(description = "End date filter (ISO 8601 / YYYY-MM-DD)")]
    pub end_date: Option<String>,
    #[schemars(description = "Maximum number of results (default 30)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetWorkoutRouteParams {
    #[schemars(description = "The workout hash identifier")]
    pub workout_hash: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListEcgReadingsParams {
    #[schemars(description = "Start date filter (ISO 8601 / YYYY-MM-DD)")]
    pub start_date: Option<String>,
    #[schemars(description = "End date filter (ISO 8601 / YYYY-MM-DD)")]
    pub end_date: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetEcgDataParams {
    #[schemars(description = "The ECG hash identifier")]
    pub ecg_hash: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCustomQueryParams {
    #[schemars(description = "A read-only SQL query (must start with SELECT or WITH)")]
    pub query: String,
}
