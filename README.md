# apple-health-mcp

MCP server for querying Apple Health export data. Imports your Apple Health export into a DuckDB database and exposes it through an [MCP](https://modelcontextprotocol.io/) server (Streamable HTTP or stdio), making your health data queryable by AI assistants.

## Why

[neiltron/apple-health-mcp](https://github.com/neiltron/apple-health-mcp) is a great project, but it depends on [Simple Health Export CSV](https://apps.apple.com/us/app/simple-health-export-csv/id1535380115?itsct=apps_box_badge&itscg=30200) to pre-process the data. I couldn't get that exporter to work for longer time ranges, so I wrote this to work directly with Apple's raw export data — and do it much faster in Rust.

## Setup

### Prerequisites

- Rust toolchain (1.70+)
- An Apple Health data export (exported from the Health app on your iPhone)

### Build & Install

```bash
cargo build --release
cp target/release/apple-health-mcp /usr/local/bin/
```

### Export your Apple Health data

1. Open the **Health** app on your iPhone or iPad
2. Tap your profile picture in the upper right corner
3. Tap **Export All Health Data** (note: this can be several gigabytes of data)
4. Copy the resulting zip file to the machine you'll run the import on
5. Extract the zip — the extracted folder is what you pass to `--export-dir` below

The export directory should contain `export.xml` and optionally `electrocardiograms/` and `workout-routes/` subdirectories.

### Import

```bash
apple-health-mcp import --export-dir /path/to/apple_health_export --db ./health.duckdb
```

This parses the XML export, ECG recordings, and GPX workout routes into a local DuckDB database. Re-running import on the same database is safe — records are deduplicated by content hash.

### Serve

The server supports two transport modes: **HTTP** (Streamable HTTP, the default) and **stdio** (stdin/stdout, for clients like Claude Desktop that spawn the server as a subprocess).

**HTTP** (default):

```bash
apple-health-mcp serve --db ./health.duckdb --port 8080
```

The MCP endpoint will be available at `http://127.0.0.1:8080/mcp`.

**stdio**:

```bash
apple-health-mcp serve --db ./health.duckdb --transport stdio
```

The server reads JSON-RPC messages from stdin and writes responses to stdout. This is typically invoked by the MCP client directly (see Claude Desktop config below).

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_record_types` | List all health record types with counts and date ranges |
| `query_records` | Query records by type, date range, and source |
| `get_record_statistics` | Aggregated stats (avg/min/max/sum) by day/week/month/year |
| `list_workouts` | List workouts with optional filtering |
| `get_workout_details` | Full workout details including events, statistics, and route availability |
| `get_workout_route` | GPS route data for a workout |
| `get_activity_summaries` | Daily activity ring data (energy, exercise, stand hours with goals) |
| `list_ecg_readings` | List ECG recordings with dates and classifications |
| `get_ecg_data` | Full ECG waveform with voltage samples |
| `list_data_sources` | Devices and apps that contributed data |
| `get_import_history` | History of data imports |
| `run_custom_query` | Run arbitrary read-only SQL (SELECT/WITH) against the database |

## Client Configuration

For HTTP-based clients, make sure the server is running before connecting. Stdio-based clients (Claude Desktop) launch the server automatically.

### Claude Code

```bash
claude mcp add apple-health --transport streamable-http http://127.0.0.1:8080/mcp
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "apple-health": {
      "command": "apple-health-mcp",
      "args": ["serve", "--db", "/path/to/health.duckdb", "--transport", "stdio"]
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json` in your project or `~/.cursor/mcp.json` globally:

```json
{
  "mcpServers": {
    "apple-health": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8080/mcp"
    }
  }
}
```

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "apple-health": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8080/mcp"
    }
  }
}
```
