//! DuckDB's file lock is per-process, so the interaction that matters here —
//! a connected MCP client versus a read-write import — can only be exercised
//! by running the real binary twice.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_apple-health-mcp");

/// The scenario from the bundle: Claude keeps the server loaded for the whole
/// session, and an import has to be able to take the write lock anyway.
#[test]
fn import_succeeds_while_a_client_is_connected() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("health.duckdb");
    let export = write_export(dir.path(), "export", common::MINIMAL_XML);
    run_import(&export, &db).expect("initial import");

    let mut client = StdioClient::start(&db);
    assert_eq!(client.record_types().len(), 2);

    // A second export adding a record type the first one lacked.
    let extra = concat!(
        r#"<Record type="HKQuantityTypeIdentifierBodyMass" sourceName="Scale" unit="kg" "#,
        r#"value="70" startDate="2024-02-01 08:00:00 +0000" "#,
        r#"endDate="2024-02-01 08:00:00 +0000"/>"#,
        "\n</HealthData>",
    );
    let export2 = write_export(
        dir.path(),
        "export2",
        &common::MINIMAL_XML.replace("</HealthData>", extra),
    );

    // The client is still connected and has already run a query.
    run_import(&export2, &db).expect("an import must not be blocked by a connected client");

    // And the same session sees the new data without being restarted.
    let types = client.record_types();
    assert!(
        types.contains(&"HKQuantityTypeIdentifierBodyMass".to_string()),
        "expected the new record type in {types:?}"
    );

    client.shutdown();
}

fn write_export(root: &Path, name: &str, xml: &str) -> std::path::PathBuf {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("export.xml"), xml).unwrap();
    dir
}

fn run_import(export_dir: &Path, db: &Path) -> Result<(), String> {
    let out = Command::new(BIN)
        .args(["import", "--export-dir"])
        .arg(export_dir)
        .arg("--db")
        .arg(db)
        .output()
        .unwrap();
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

/// A minimal MCP client speaking JSON-RPC over the server's stdio transport.
struct StdioClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u32,
}

impl StdioClient {
    fn start(db: &Path) -> Self {
        let mut child = Command::new(BIN)
            .args(["serve", "--transport", "stdio", "--db"])
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };

        client.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            }),
        );
        client.notify("notifications/initialized");
        client
    }

    fn request(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }));

        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        let response: serde_json::Value =
            serde_json::from_str(&line).unwrap_or_else(|e| panic!("bad response {line:?}: {e}"));
        assert!(
            response.get("error").is_none(),
            "{method} failed: {response}"
        );
        response["result"].clone()
    }

    fn notify(&mut self, method: &str) {
        self.send(serde_json::json!({"jsonrpc": "2.0", "method": method}));
    }

    fn send(&mut self, message: serde_json::Value) {
        writeln!(self.stdin, "{message}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn record_types(&mut self) -> Vec<String> {
        let result = self.request(
            "tools/call",
            serde_json::json!({"name": "list_record_types", "arguments": {}}),
        );
        result["structuredContent"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["type"].as_str().unwrap().to_string())
            .collect()
    }

    fn shutdown(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
    }
}
