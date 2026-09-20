//! Structured output types for the MCP tools.
//!
//! Every tool returns one of these wrapped in [`rmcp::handler::server::wrapper::Json`],
//! so the tool advertises an `outputSchema` and the response carries
//! `structuredContent` alongside the JSON text block clients without structured
//! output support still read.

use rmcp::schemars;
use serde::Serialize;
use serde_json::Value;

/// A tabular query result.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct RowSet {
    /// Number of rows in `rows`.
    pub row_count: usize,
    /// One object per row. Columns that were NULL are omitted from the object.
    pub rows: Vec<Value>,
}

impl RowSet {
    pub fn new(rows: Vec<Value>) -> Self {
        Self {
            row_count: rows.len(),
            rows,
        }
    }
}

/// A workout together with its events, per-metric statistics, and route availability.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct WorkoutDetails {
    /// The workout row, or null when `workout_hash` matched nothing.
    pub workout: Option<Value>,
    /// Lap and pause markers recorded during the workout.
    pub events: Vec<Value>,
    /// Per-metric breakdowns (e.g. heart rate average/min/max) for the workout.
    pub statistics: Vec<Value>,
    /// Whether GPS route points exist for this workout.
    pub has_route: bool,
}

/// A full ECG recording: metadata plus the voltage waveform.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct EcgData {
    /// The `ecg_readings` row, or null when `ecg_hash` matched nothing.
    pub reading: Option<Value>,
    /// Number of voltage samples in `voltages_uv`.
    pub sample_count: usize,
    /// Voltage samples in microvolts, in recording order.
    pub voltages_uv: Vec<Value>,
}
