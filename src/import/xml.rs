use anyhow::{Context, Result};
use duckdb::Connection;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::io::BufReader;
use std::path::Path;
use tracing::info;

use crate::models::{compute_hash, ImportStats};

const BATCH_SIZE: usize = 100_000;

fn attr_value(e: &quick_xml::events::BytesStart, name: &[u8]) -> Option<String> {
    e.attributes().filter_map(|a| a.ok()).find_map(|a| {
        if a.key.as_ref() == name {
            String::from_utf8(a.value.to_vec()).ok()
        } else {
            None
        }
    })
}

fn parse_opt_f64(s: &Option<String>) -> Option<f64> {
    s.as_ref().and_then(|v| v.parse::<f64>().ok())
}

/// Strip the trailing UTC-offset suffix (e.g. ` +0900`) from an Apple
/// Health date string and return the naive local timestamp. Apple Health
/// emits dates as "YYYY-MM-DD HH:MM:SS +OOOO" where the time portion is
/// already the local wall-clock at the recording device; this helper
/// returns the wall-clock half so it fits a DuckDB naive TIMESTAMP column.
/// Pair with [`extract_offset_minutes`] to recover the dropped offset.
fn clean_date(s: &str) -> String {
    // "2020-06-20 16:56:44 +0000" -> "2020-06-20 16:56:44"
    if let Some(pos) = s.rfind(" +") {
        s[..pos].to_string()
    } else if let Some(pos) = s.rfind(" -") {
        s[..pos].to_string()
    } else {
        s.to_string()
    }
}

fn clean_date_opt(s: &Option<String>) -> Option<String> {
    s.as_ref().map(|v| clean_date(v))
}

/// Parse the UTC offset (minutes east of UTC) out of an Apple Health date
/// string such as "2020-06-20 16:56:44 +0900". Returns `Some(540)` for
/// `+0900`, `Some(-420)` for `-0700`, and `None` when the suffix is absent
/// or malformed. The companion to [`clean_date`]: that function discards
/// the offset to fit a naive TIMESTAMP, this one preserves it so the GPX
/// importer can later shift true-UTC route timestamps onto the same
/// local-time basis as everything else.
fn extract_offset_minutes(s: &str) -> Option<i32> {
    let (offset_str, sign) = if let Some(pos) = s.rfind(" +") {
        (&s[pos + 2..], 1)
    } else if let Some(pos) = s.rfind(" -") {
        (&s[pos + 2..], -1)
    } else {
        return None;
    };
    if offset_str.len() != 4 || !offset_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let hours: i32 = offset_str[..2].parse().ok()?;
    let mins: i32 = offset_str[2..].parse().ok()?;
    Some(sign * (hours * 60 + mins))
}

pub fn import_xml(conn: &Connection, xml_path: &Path, import_id: &str) -> Result<ImportStats> {
    let file = std::fs::File::open(xml_path).context("Failed to open export.xml")?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::with_capacity(4096);
    let mut stats = ImportStats::default();

    // Batch buffers
    let mut record_batch: Vec<RecordRow> = Vec::with_capacity(BATCH_SIZE);
    let mut metadata_batch: Vec<MetadataRow> = Vec::with_capacity(BATCH_SIZE);
    let mut workout_batch: Vec<WorkoutRow> = Vec::with_capacity(BATCH_SIZE);
    let mut workout_event_batch: Vec<WorkoutEventRow> = Vec::with_capacity(BATCH_SIZE);
    let mut workout_stat_batch: Vec<WorkoutStatRow> = Vec::with_capacity(BATCH_SIZE);
    let mut workout_metadata_batch: Vec<WorkoutMetadataRow> = Vec::with_capacity(BATCH_SIZE);
    let mut workout_route_batch: Vec<WorkoutRouteRow> = Vec::with_capacity(BATCH_SIZE);
    let mut activity_batch: Vec<ActivityRow> = Vec::with_capacity(BATCH_SIZE);

    // State for nested parsing
    let mut in_workout = false;
    let mut current_workout: Option<WorkoutRow> = None;
    let mut current_workout_events: Vec<WorkoutEventRow> = Vec::new();
    let mut current_workout_stats: Vec<WorkoutStatRow> = Vec::new();
    let mut current_workout_metadata: Vec<WorkoutMetadataRow> = Vec::new();
    let mut current_workout_route: Option<WorkoutRouteRow> = None;
    let mut in_workout_route = false;

    let mut in_record = false;
    let mut current_record_hash: Option<String> = None;

    // We skip Correlation children since the DTD says correlation member records
    // also appear as top-level records
    let mut in_correlation = false;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let name = e.name();
                let local = name.as_ref();

                match local {
                    b"Record" if !in_correlation => {
                        let record_type = attr_value(e, b"type").unwrap_or_default();
                        let source_name = attr_value(e, b"sourceName").unwrap_or_default();
                        let start_date =
                            clean_date(&attr_value(e, b"startDate").unwrap_or_default());
                        let end_date = clean_date(&attr_value(e, b"endDate").unwrap_or_default());
                        let value_str = attr_value(e, b"value");
                        let unit = attr_value(e, b"unit");
                        let value = parse_opt_f64(&value_str);

                        let hash = compute_hash(&[
                            &record_type,
                            &source_name,
                            &start_date,
                            &end_date,
                            value_str.as_deref().unwrap_or(""),
                            unit.as_deref().unwrap_or(""),
                        ]);

                        record_batch.push(RecordRow {
                            record_hash: hash.clone(),
                            record_type,
                            value,
                            unit,
                            source_name,
                            source_version: attr_value(e, b"sourceVersion"),
                            device: attr_value(e, b"device"),
                            creation_date: clean_date_opt(&attr_value(e, b"creationDate")),
                            start_date,
                            end_date,
                            import_id: import_id.to_string(),
                        });
                        stats.records += 1;

                        // Check if this is a self-closing element or has children
                        in_record = true;
                        current_record_hash = Some(hash);

                        if record_batch.len() >= BATCH_SIZE {
                            flush_records(conn, &mut record_batch)?;
                        }

                        if stats.records % 500_000 == 0 {
                            info!("Processed {} records...", stats.records);
                        }
                    }
                    b"MetadataEntry" => {
                        let key = attr_value(e, b"key").unwrap_or_default();
                        let value = attr_value(e, b"value").unwrap_or_default();

                        if in_workout {
                            // MetadataEntry inside <Workout> carries
                            // workout-level context (HKIndoorWorkout,
                            // HKAverageMETs, HKWeather*, HKElevationAscended,
                            // and third-party app keys). Buffer per workout
                            // and flush together with the workout so a
                            // workout that fails to close does not leak
                            // orphaned metadata rows.
                            if let Some(ref w) = current_workout {
                                current_workout_metadata.push(WorkoutMetadataRow {
                                    workout_hash: w.workout_hash.clone(),
                                    key,
                                    value: Some(value),
                                    import_id: import_id.to_string(),
                                });
                            }
                        } else if in_record {
                            if let Some(ref hash) = current_record_hash {
                                metadata_batch.push(MetadataRow {
                                    record_hash: hash.clone(),
                                    key,
                                    value,
                                });
                                stats.metadata_entries += 1;
                                if metadata_batch.len() >= BATCH_SIZE {
                                    flush_metadata(conn, &mut metadata_batch)?;
                                }
                            }
                        }
                    }
                    b"Workout" => {
                        in_workout = true;
                        let activity_type =
                            attr_value(e, b"workoutActivityType").unwrap_or_default();
                        let source_name = attr_value(e, b"sourceName").unwrap_or_default();
                        let start_date_raw = attr_value(e, b"startDate").unwrap_or_default();
                        let start_date = clean_date(&start_date_raw);
                        let start_offset_minutes = extract_offset_minutes(&start_date_raw);
                        let end_date = clean_date(&attr_value(e, b"endDate").unwrap_or_default());
                        let duration_str = attr_value(e, b"duration");
                        let duration = parse_opt_f64(&duration_str);

                        let hash = compute_hash(&[
                            &activity_type,
                            &source_name,
                            &start_date,
                            &end_date,
                            duration_str.as_deref().unwrap_or(""),
                        ]);

                        current_workout = Some(WorkoutRow {
                            workout_hash: hash,
                            activity_type,
                            duration,
                            duration_unit: attr_value(e, b"durationUnit"),
                            total_distance: parse_opt_f64(&attr_value(e, b"totalDistance")),
                            total_distance_unit: attr_value(e, b"totalDistanceUnit"),
                            total_energy_burned: parse_opt_f64(&attr_value(
                                e,
                                b"totalEnergyBurned",
                            )),
                            total_energy_unit: attr_value(e, b"totalEnergyBurnedUnit"),
                            source_name,
                            source_version: attr_value(e, b"sourceVersion"),
                            device: attr_value(e, b"device"),
                            creation_date: clean_date_opt(&attr_value(e, b"creationDate")),
                            start_date,
                            end_date,
                            start_offset_minutes,
                            import_id: import_id.to_string(),
                        });
                        current_workout_events.clear();
                        current_workout_stats.clear();
                        current_workout_metadata.clear();
                        current_workout_route = None;
                        in_workout_route = false;
                    }
                    b"WorkoutEvent" if in_workout => {
                        if let Some(ref w) = current_workout {
                            current_workout_events.push(WorkoutEventRow {
                                workout_hash: w.workout_hash.clone(),
                                event_type: attr_value(e, b"type").unwrap_or_default(),
                                date: clean_date_opt(&attr_value(e, b"date")),
                                duration: parse_opt_f64(&attr_value(e, b"duration")),
                                duration_unit: attr_value(e, b"durationUnit"),
                            });
                        }
                    }
                    b"WorkoutStatistics" if in_workout => {
                        if let Some(ref w) = current_workout {
                            current_workout_stats.push(WorkoutStatRow {
                                workout_hash: w.workout_hash.clone(),
                                stat_type: attr_value(e, b"type").unwrap_or_default(),
                                start_date: clean_date_opt(&attr_value(e, b"startDate")),
                                end_date: clean_date_opt(&attr_value(e, b"endDate")),
                                average: parse_opt_f64(&attr_value(e, b"average")),
                                minimum: parse_opt_f64(&attr_value(e, b"minimum")),
                                maximum: parse_opt_f64(&attr_value(e, b"maximum")),
                                sum: parse_opt_f64(&attr_value(e, b"sum")),
                                unit: attr_value(e, b"unit"),
                            });
                        }
                    }
                    b"WorkoutRoute" if in_workout => {
                        if let Some(ref w) = current_workout {
                            in_workout_route = true;
                            current_workout_route = Some(WorkoutRouteRow {
                                workout_hash: w.workout_hash.clone(),
                                file_path: String::new(),
                                source_name: attr_value(e, b"sourceName"),
                                source_version: attr_value(e, b"sourceVersion"),
                                creation_date: clean_date_opt(&attr_value(e, b"creationDate")),
                                start_date: clean_date_opt(&attr_value(e, b"startDate")),
                                end_date: clean_date_opt(&attr_value(e, b"endDate")),
                                import_id: import_id.to_string(),
                            });
                        }
                    }
                    b"FileReference" if in_workout_route => {
                        if let Some(ref mut wr) = current_workout_route {
                            if let Some(path) = attr_value(e, b"path") {
                                wr.file_path = path;
                            }
                        }
                    }
                    b"ActivitySummary" => {
                        let date_comp = attr_value(e, b"dateComponents").unwrap_or_default();
                        activity_batch.push(ActivityRow {
                            date_components: date_comp,
                            active_energy_burned: parse_opt_f64(&attr_value(
                                e,
                                b"activeEnergyBurned",
                            )),
                            active_energy_burned_goal: parse_opt_f64(&attr_value(
                                e,
                                b"activeEnergyBurnedGoal",
                            )),
                            apple_move_time: parse_opt_f64(&attr_value(e, b"appleMoveTime")),
                            apple_move_time_goal: parse_opt_f64(&attr_value(
                                e,
                                b"appleMoveTimeGoal",
                            )),
                            apple_exercise_time: parse_opt_f64(&attr_value(
                                e,
                                b"appleExerciseTime",
                            )),
                            apple_exercise_time_goal: parse_opt_f64(&attr_value(
                                e,
                                b"appleExerciseTimeGoal",
                            )),
                            apple_stand_hours: parse_opt_f64(&attr_value(e, b"appleStandHours")),
                            apple_stand_hours_goal: parse_opt_f64(&attr_value(
                                e,
                                b"appleStandHoursGoal",
                            )),
                            import_id: import_id.to_string(),
                        });
                        stats.activity_summaries += 1;
                        if activity_batch.len() >= BATCH_SIZE {
                            flush_activities(conn, &mut activity_batch)?;
                        }
                    }
                    b"Correlation" => {
                        in_correlation = true;
                        stats.correlations += 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name();
                let local = name.as_ref();
                match local {
                    b"Record" => {
                        in_record = false;
                        current_record_hash = None;
                    }
                    b"WorkoutRoute" => {
                        // Only commit the route row once a FileReference
                        // child has populated `file_path`. A WorkoutRoute
                        // without a file reference is malformed and would
                        // confuse the join against route_points later.
                        if let Some(wr) = current_workout_route.take() {
                            if !wr.file_path.is_empty() {
                                // Build the file-path -> workout_hash map
                                // inline while we already hold the matching
                                // workout in scope. The legacy
                                // build_workout_route_map() re-scanned the
                                // whole XML and recomputed the hash from raw
                                // (non-clean_date'd) dates, producing a
                                // value that never matched the workouts
                                // table -- which is why every route_points
                                // row was orphaned. Inserting here uses the
                                // exact same hash the workout itself was
                                // stored under.
                                stats
                                    .workout_route_map
                                    .insert(wr.file_path.clone(), wr.workout_hash.clone());
                                workout_route_batch.push(wr);
                                stats.workout_routes += 1;
                                if workout_route_batch.len() >= BATCH_SIZE {
                                    flush_workout_routes(conn, &mut workout_route_batch)?;
                                }
                            }
                        }
                        in_workout_route = false;
                    }
                    b"Workout" => {
                        if let Some(w) = current_workout.take() {
                            if let Some(offset) = w.start_offset_minutes {
                                stats
                                    .workout_offset_map
                                    .insert(w.workout_hash.clone(), offset);
                            }
                            workout_batch.push(w);
                            stats.workouts += 1;

                            for ev in current_workout_events.drain(..) {
                                workout_event_batch.push(ev);
                                stats.workout_events += 1;
                            }
                            for st in current_workout_stats.drain(..) {
                                workout_stat_batch.push(st);
                                stats.workout_statistics += 1;
                            }
                            for md in current_workout_metadata.drain(..) {
                                workout_metadata_batch.push(md);
                                stats.workout_metadata_entries += 1;
                            }

                            if workout_batch.len() >= BATCH_SIZE {
                                flush_workouts(conn, &mut workout_batch)?;
                            }
                            if workout_event_batch.len() >= BATCH_SIZE {
                                flush_workout_events(conn, &mut workout_event_batch)?;
                            }
                            if workout_stat_batch.len() >= BATCH_SIZE {
                                flush_workout_stats(conn, &mut workout_stat_batch)?;
                            }
                            if workout_metadata_batch.len() >= BATCH_SIZE {
                                flush_workout_metadata(conn, &mut workout_metadata_batch)?;
                            }
                        }
                        in_workout = false;
                    }
                    b"Correlation" => {
                        in_correlation = false;
                    }
                    _ => {}
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("XML parse error: {:?}, continuing...", e);
            }
        }
        buf.clear();
    }

    // Flush remaining batches
    flush_records(conn, &mut record_batch)?;
    flush_metadata(conn, &mut metadata_batch)?;
    flush_workouts(conn, &mut workout_batch)?;
    flush_workout_events(conn, &mut workout_event_batch)?;
    flush_workout_stats(conn, &mut workout_stat_batch)?;
    flush_workout_metadata(conn, &mut workout_metadata_batch)?;
    flush_workout_routes(conn, &mut workout_route_batch)?;
    flush_activities(conn, &mut activity_batch)?;

    info!(
        "XML import complete: {} records, {} workouts ({} metadata entries, {} routes), {} activity summaries, {} correlations",
        stats.records,
        stats.workouts,
        stats.workout_metadata_entries,
        stats.workout_routes,
        stats.activity_summaries,
        stats.correlations,
    );

    Ok(stats)
}

// -- Row types for batching --

struct RecordRow {
    record_hash: String,
    record_type: String,
    value: Option<f64>,
    unit: Option<String>,
    source_name: String,
    source_version: Option<String>,
    device: Option<String>,
    creation_date: Option<String>,
    start_date: String,
    end_date: String,
    import_id: String,
}

struct MetadataRow {
    record_hash: String,
    key: String,
    value: String,
}

struct WorkoutRow {
    workout_hash: String,
    activity_type: String,
    duration: Option<f64>,
    duration_unit: Option<String>,
    total_distance: Option<f64>,
    total_distance_unit: Option<String>,
    total_energy_burned: Option<f64>,
    total_energy_unit: Option<String>,
    source_name: String,
    source_version: Option<String>,
    device: Option<String>,
    creation_date: Option<String>,
    start_date: String,
    end_date: String,
    start_offset_minutes: Option<i32>,
    import_id: String,
}

struct WorkoutEventRow {
    workout_hash: String,
    event_type: String,
    date: Option<String>,
    duration: Option<f64>,
    duration_unit: Option<String>,
}

struct WorkoutStatRow {
    workout_hash: String,
    stat_type: String,
    start_date: Option<String>,
    end_date: Option<String>,
    average: Option<f64>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    sum: Option<f64>,
    unit: Option<String>,
}

struct WorkoutMetadataRow {
    workout_hash: String,
    key: String,
    value: Option<String>,
    import_id: String,
}

struct WorkoutRouteRow {
    workout_hash: String,
    file_path: String,
    source_name: Option<String>,
    source_version: Option<String>,
    creation_date: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    import_id: String,
}

struct ActivityRow {
    date_components: String,
    active_energy_burned: Option<f64>,
    active_energy_burned_goal: Option<f64>,
    apple_move_time: Option<f64>,
    apple_move_time_goal: Option<f64>,
    apple_exercise_time: Option<f64>,
    apple_exercise_time_goal: Option<f64>,
    apple_stand_hours: Option<f64>,
    apple_stand_hours_goal: Option<f64>,
    import_id: String,
}

// -- Flush functions using DuckDB Appender for bulk loading --

fn flush_records(conn: &Connection, batch: &mut Vec<RecordRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("records")?;
    for r in batch.iter() {
        appender.append_row(duckdb::params![
            r.record_hash,
            r.record_type,
            r.value,
            r.unit,
            r.source_name,
            r.source_version,
            r.device,
            r.creation_date,
            r.start_date,
            r.end_date,
            r.import_id,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_metadata(conn: &Connection, batch: &mut Vec<MetadataRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("record_metadata")?;
    for m in batch.iter() {
        appender.append_row(duckdb::params![m.record_hash, m.key, m.value])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_workouts(conn: &Connection, batch: &mut Vec<WorkoutRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("workouts")?;
    for w in batch.iter() {
        appender.append_row(duckdb::params![
            w.workout_hash,
            w.activity_type,
            w.duration,
            w.duration_unit,
            w.total_distance,
            w.total_distance_unit,
            w.total_energy_burned,
            w.total_energy_unit,
            w.source_name,
            w.source_version,
            w.device,
            w.creation_date,
            w.start_date,
            w.end_date,
            w.start_offset_minutes,
            w.import_id,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_workout_events(conn: &Connection, batch: &mut Vec<WorkoutEventRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("workout_events")?;
    for e in batch.iter() {
        appender.append_row(duckdb::params![
            e.workout_hash,
            e.event_type,
            e.date,
            e.duration,
            e.duration_unit,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_workout_stats(conn: &Connection, batch: &mut Vec<WorkoutStatRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("workout_statistics")?;
    for s in batch.iter() {
        appender.append_row(duckdb::params![
            s.workout_hash,
            s.stat_type,
            s.start_date,
            s.end_date,
            s.average,
            s.minimum,
            s.maximum,
            s.sum,
            s.unit,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_workout_metadata(conn: &Connection, batch: &mut Vec<WorkoutMetadataRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("workout_metadata")?;
    for m in batch.iter() {
        appender.append_row(duckdb::params![m.workout_hash, m.key, m.value, m.import_id,])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_workout_routes(conn: &Connection, batch: &mut Vec<WorkoutRouteRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("workout_routes")?;
    for r in batch.iter() {
        appender.append_row(duckdb::params![
            r.workout_hash,
            r.file_path,
            r.source_name,
            r.source_version,
            r.creation_date,
            r.start_date,
            r.end_date,
            r.import_id,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

fn flush_activities(conn: &Connection, batch: &mut Vec<ActivityRow>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut appender = conn.appender("activity_summaries")?;
    for a in batch.iter() {
        appender.append_row(duckdb::params![
            a.date_components,
            a.active_energy_burned,
            a.active_energy_burned_goal,
            a.apple_move_time,
            a.apple_move_time_goal,
            a.apple_exercise_time,
            a.apple_exercise_time_goal,
            a.apple_stand_hours,
            a.apple_stand_hours_goal,
            a.import_id,
        ])?;
    }
    appender.flush()?;
    batch.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ensure_schema, open_db_in_memory};

    #[test]
    fn clean_date_strips_positive_offset() {
        assert_eq!(
            clean_date("2020-06-20 16:56:44 +0000"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_date_strips_negative_offset() {
        assert_eq!(
            clean_date("2020-06-20 16:56:44 -0500"),
            "2020-06-20 16:56:44"
        );
    }

    #[test]
    fn clean_date_no_offset() {
        assert_eq!(clean_date("2020-06-20 16:56:44"), "2020-06-20 16:56:44");
    }

    #[test]
    fn clean_date_opt_some() {
        let s = Some("2020-06-20 16:56:44 +0000".to_string());
        assert_eq!(clean_date_opt(&s), Some("2020-06-20 16:56:44".to_string()));
    }

    #[test]
    fn clean_date_opt_none() {
        assert_eq!(clean_date_opt(&None), None);
    }

    #[test]
    fn parse_opt_f64_valid() {
        let s = Some("72.5".to_string());
        assert_eq!(parse_opt_f64(&s), Some(72.5));
    }

    #[test]
    fn parse_opt_f64_invalid() {
        let s = Some("not_a_number".to_string());
        assert_eq!(parse_opt_f64(&s), None);
    }

    #[test]
    fn parse_opt_f64_none() {
        assert_eq!(parse_opt_f64(&None), None);
    }

    #[test]
    fn import_xml_minimal() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData>
<HealthData locale="en_US">
 <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch" unit="count/min" value="72" startDate="2024-01-01 08:00:00 +0000" endDate="2024-01-01 08:01:00 +0000">
  <MetadataEntry key="HKMetadataKeyHeartRateMotionContext" value="1"/>
 </Record>
 <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Phone" unit="count" value="100" startDate="2024-01-01 09:00:00 +0000" endDate="2024-01-01 09:30:00 +0000"/>
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30.5" durationUnit="min" totalDistance="5.0" totalDistanceUnit="km" totalEnergyBurned="300" totalEnergyBurnedUnit="kcal" sourceName="Watch" startDate="2024-01-01 10:00:00 +0000" endDate="2024-01-01 10:30:00 +0000">
  <WorkoutEvent type="HKWorkoutEventTypeLap" date="2024-01-01 10:15:00 +0000"/>
  <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate" startDate="2024-01-01 10:00:00 +0000" endDate="2024-01-01 10:30:00 +0000" average="150" minimum="120" maximum="180" unit="count/min"/>
  <WorkoutRoute sourceName="Watch">
   <FileReference path="/workout-routes/route_2024-01-01.gpx"/>
  </WorkoutRoute>
 </Workout>
 <Correlation type="HKCorrelationTypeIdentifierBloodPressure" sourceName="BP" startDate="2024-01-01 12:00:00 +0000" endDate="2024-01-01 12:00:00 +0000">
  <Record type="HKQuantityTypeIdentifierBloodPressureSystolic" sourceName="BP" unit="mmHg" value="120" startDate="2024-01-01 12:00:00 +0000" endDate="2024-01-01 12:00:00 +0000"/>
 </Correlation>
 <ActivitySummary dateComponents="2024-01-01" activeEnergyBurned="500" activeEnergyBurnedGoal="600" appleExerciseTime="30" appleExerciseTimeGoal="30" appleStandHours="10" appleStandHoursGoal="12"/>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_import").unwrap();

        assert_eq!(stats.records, 2); // correlation child skipped
        assert_eq!(stats.workouts, 1);
        assert_eq!(stats.activity_summaries, 1);
        assert_eq!(stats.correlations, 1);
        assert_eq!(stats.metadata_entries, 1);
        assert_eq!(stats.workout_events, 1);
        assert_eq!(stats.workout_statistics, 1);
        assert_eq!(stats.workout_routes, 1);

        // Verify data in DB
        let rec_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rec_count, 2);

        let workout_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workouts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(workout_count, 1);

        let meta_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM record_metadata", [], |row| row.get(0))
            .unwrap();
        assert_eq!(meta_count, 1);

        let route_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workout_routes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(route_count, 1);
        let route_path: String = conn
            .query_row("SELECT file_path FROM workout_routes LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(route_path, "/workout-routes/route_2024-01-01.gpx");
    }

    #[test]
    fn import_xml_workout_metadata_persisted() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" sourceName="Watch" startDate="2024-02-02 06:00:00 +0000" endDate="2024-02-02 06:30:00 +0000">
  <MetadataEntry key="HKIndoorWorkout" value="0"/>
  <MetadataEntry key="HKAverageMETs" value="7.5 kcal/hr·kg"/>
  <MetadataEntry key="HKWeatherTemperature" value="18 degF"/>
 </Workout>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_meta").unwrap();
        assert_eq!(stats.workouts, 1);
        assert_eq!(stats.workout_metadata_entries, 3);
        // Record-level metadata counter must stay zero: the entries belong to
        // a Workout, not a Record.
        assert_eq!(stats.metadata_entries, 0);

        let row_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workout_metadata", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(row_count, 3);

        let indoor: String = conn
            .query_row(
                "SELECT value FROM workout_metadata WHERE key = 'HKIndoorWorkout'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indoor, "0");
    }

    #[test]
    fn import_xml_workout_route_links_to_workout_hash() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <Workout workoutActivityType="HKWorkoutActivityTypeCycling" duration="60" durationUnit="min" sourceName="Watch" startDate="2024-03-03 07:00:00 +0000" endDate="2024-03-03 08:00:00 +0000">
  <WorkoutRoute sourceName="Watch" sourceVersion="10.2" creationDate="2024-03-03 08:00:00 +0000" startDate="2024-03-03 07:00:00 +0000" endDate="2024-03-03 08:00:00 +0000">
   <FileReference path="/workout-routes/route_2024-03-03.gpx"/>
  </WorkoutRoute>
 </Workout>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_route").unwrap();
        assert_eq!(stats.workouts, 1);
        assert_eq!(stats.workout_routes, 1);

        // workout_routes.workout_hash must match the inserted workouts row
        // exactly; the previous build_workout_route_map() implementation
        // mis-hashed by skipping clean_date() and produced 100% orphans.
        let (wh_from_workouts, wh_from_routes): (String, String) = conn
            .query_row(
                "SELECT w.workout_hash, wr.workout_hash
                 FROM workouts w
                 JOIN workout_routes wr ON wr.workout_hash = w.workout_hash",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(wh_from_workouts, wh_from_routes);
    }

    #[test]
    fn import_xml_workout_route_without_file_reference_is_dropped() {
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" sourceName="Watch" startDate="2024-04-04 06:00:00 +0000" endDate="2024-04-04 06:30:00 +0000">
  <WorkoutRoute sourceName="Watch"/>
 </Workout>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_no_ref").unwrap();
        // A WorkoutRoute without a FileReference cannot be joined to a GPX
        // payload and is dropped on the floor rather than inserted with an
        // empty file_path that would silently break joins later.
        assert_eq!(stats.workout_routes, 0);
    }

    #[test]
    fn extract_offset_minutes_parses_common_offsets() {
        assert_eq!(
            extract_offset_minutes("2026-06-17 04:58:38 +0900"),
            Some(540)
        );
        assert_eq!(
            extract_offset_minutes("2026-06-17 04:58:38 -0700"),
            Some(-420)
        );
        assert_eq!(extract_offset_minutes("2026-06-17 04:58:38 +0000"), Some(0));
        // Half-hour zones (e.g. India Standard Time, Nepal Standard Time).
        assert_eq!(
            extract_offset_minutes("2026-06-17 04:58:38 +0530"),
            Some(330)
        );
        assert_eq!(
            extract_offset_minutes("2026-06-17 04:58:38 -0345"),
            Some(-225)
        );
    }

    #[test]
    fn extract_offset_minutes_rejects_missing_or_malformed() {
        // No offset suffix at all.
        assert_eq!(extract_offset_minutes("2026-06-17 04:58:38"), None);
        // Too few digits.
        assert_eq!(extract_offset_minutes("2026-06-17 04:58:38 +090"), None);
        // Colon-separated offset (RFC 3339 form) is GPX territory, not XML.
        // The XML helper deliberately rejects it so a stray ISO 8601 input
        // doesn't silently parse to a wrong value.
        assert_eq!(extract_offset_minutes("2026-06-17 04:58:38 +09:00"), None);
        // Non-digit body.
        assert_eq!(extract_offset_minutes("2026-06-17 04:58:38 +09ab"), None);
    }

    #[test]
    fn import_xml_captures_workout_start_offset() {
        // Two workouts recorded in different time zones land in the DB
        // with their offsets preserved on `start_offset_minutes`, and the
        // `workout_offset_map` exposes the same pair to the GPX importer.
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" sourceName="Watch" startDate="2024-06-17 04:58:38 +0900" endDate="2024-06-17 05:28:38 +0900">
 </Workout>
 <Workout workoutActivityType="HKWorkoutActivityTypeCycling" duration="60" durationUnit="min" sourceName="Watch" startDate="2024-03-03 07:00:00 -0700" endDate="2024-03-03 08:00:00 -0700">
 </Workout>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_offset").unwrap();
        assert_eq!(stats.workouts, 2);
        assert_eq!(stats.workout_offset_map.len(), 2);

        // Offsets sorted by start_date (ascending): the PST workout is
        // earlier in wall-clock order (after strip) than the JST workout.
        let offsets: Vec<i32> = {
            let mut stmt = conn
                .prepare("SELECT start_offset_minutes FROM workouts ORDER BY start_date")
                .unwrap();
            let rows: Vec<i32> = stmt
                .query_map([], |row| row.get::<_, i32>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            rows
        };
        assert_eq!(offsets, vec![-420, 540]);
    }

    #[test]
    fn import_xml_workout_without_offset_skips_offset_map_entry() {
        // A workout whose `startDate` lacks a TZ suffix lands in the
        // workouts table with start_offset_minutes = NULL, and the
        // `workout_offset_map` does NOT receive an entry for it (so the
        // GPX importer falls back to legacy strip behavior for that
        // workout's route).
        let conn = open_db_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="30" durationUnit="min" sourceName="Watch" startDate="2024-06-17 04:58:38" endDate="2024-06-17 05:28:38">
 </Workout>
</HealthData>"#;

        let dir = tempfile::tempdir().unwrap();
        let xml_path = dir.path().join("export.xml");
        std::fs::write(&xml_path, xml).unwrap();

        let stats = import_xml(&conn, &xml_path, "test_no_offset").unwrap();
        assert_eq!(stats.workouts, 1);
        assert_eq!(stats.workout_offset_map.len(), 0);

        let offset: Option<i32> = conn
            .query_row("SELECT start_offset_minutes FROM workouts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(offset, None);
    }
}
