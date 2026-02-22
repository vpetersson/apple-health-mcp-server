#![allow(dead_code)]

use apple_health_mcp::db::{ensure_schema, open_db_in_memory};
use duckdb::Connection;

pub const MINIMAL_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData>
<HealthData locale="en_US">
 <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Apple Watch" unit="count/min" value="72" startDate="2024-01-01 08:00:00 +0000" endDate="2024-01-01 08:01:00 +0000">
  <MetadataEntry key="HKMetadataKeyHeartRateMotionContext" value="1"/>
 </Record>
 <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count" value="1500" startDate="2024-01-01 09:00:00 +0000" endDate="2024-01-01 09:30:00 +0000"/>
 <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="1800" durationUnit="sec" totalDistance="5000" totalDistanceUnit="m" totalEnergyBurned="300" totalEnergyBurnedUnit="kcal" sourceName="Apple Watch" startDate="2024-01-01 10:00:00 +0000" endDate="2024-01-01 10:30:00 +0000">
  <WorkoutEvent type="HKWorkoutEventTypeLap" date="2024-01-01 10:15:00 +0000"/>
  <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate" startDate="2024-01-01 10:00:00 +0000" endDate="2024-01-01 10:30:00 +0000" average="150" minimum="120" maximum="180" unit="count/min"/>
  <WorkoutRoute sourceName="Apple Watch">
   <FileReference path="/workout-routes/route_2024-01-01.gpx"/>
  </WorkoutRoute>
 </Workout>
 <Correlation type="HKCorrelationTypeIdentifierBloodPressure" sourceName="BP Monitor" startDate="2024-01-01 12:00:00 +0000" endDate="2024-01-01 12:00:00 +0000">
  <Record type="HKQuantityTypeIdentifierBloodPressureSystolic" sourceName="BP Monitor" unit="mmHg" value="120" startDate="2024-01-01 12:00:00 +0000" endDate="2024-01-01 12:00:00 +0000"/>
 </Correlation>
 <ActivitySummary dateComponents="2024-01-01" activeEnergyBurned="500" activeEnergyBurnedGoal="600" appleExerciseTime="30" appleExerciseTimeGoal="30" appleStandHours="10" appleStandHoursGoal="12"/>
</HealthData>"#;

pub const MINIMAL_ECG_CSV: &str = "Name,Test User
Date of Birth,1990-01-01
Recorded Date,2024-06-15 10:30:00 +0000
Classification,Sinus Rhythm
Symptoms,None
Software Version,2.0
Device,\"Apple Watch\"
Sample Rate,512.000 Hz
Lead,Lead I
Unit,µV

100
200
-50
150
75";

pub const MINIMAL_GPX: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1">
  <trk>
    <trkseg>
      <trkpt lat="37.7749" lon="-122.4194">
        <ele>10.5</ele>
        <time>2024-01-01T10:00:00Z</time>
        <speed>3.5</speed>
        <course>180.0</course>
        <hAcc>5.0</hAcc>
        <vAcc>3.0</vAcc>
      </trkpt>
      <trkpt lat="37.7750" lon="-122.4195">
        <ele>11.0</ele>
        <time>2024-01-01T10:00:05Z</time>
        <speed>3.6</speed>
        <course>181.0</course>
        <hAcc>4.5</hAcc>
        <vAcc>2.8</vAcc>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"#;

pub fn setup_test_db() -> Connection {
    let conn = open_db_in_memory().unwrap();
    ensure_schema(&conn).unwrap();
    conn
}
