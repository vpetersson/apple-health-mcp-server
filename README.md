# apple-health-mcp

MCP server for querying Apple Health export data. Imports your Apple Health export into a DuckDB database and exposes it through an [MCP](https://modelcontextprotocol.io/) server (Streamable HTTP or stdio), making your health data queryable by AI assistants.

## Why

[neiltron/apple-health-mcp](https://github.com/neiltron/apple-health-mcp) is a great project, but it depends on [Simple Health Export CSV](https://apps.apple.com/us/app/simple-health-export-csv/id1535380115?itsct=apps_box_badge&itscg=30200) to pre-process the data. I couldn't get that exporter to work for longer time ranges, so I wrote this to work directly with Apple's raw export data — and do it much faster in Rust.

## Setup

### Prerequisites

- An Apple Health data export (exported from the Health app on your iPhone)
- A Rust toolchain (1.88+, the MSRV of the `rmcp` SDK), if you build from source rather than installing with Homebrew

### Install

**Homebrew** (prebuilt binaries for macOS on Apple Silicon and Intel, and Linux on x86_64):

```bash
brew tap vpetersson/apple-health https://github.com/vpetersson/apple-health-mcp-server
brew install vpetersson/apple-health/apple-health-mcp
```

Since [Homebrew 6.0.0](https://brew.sh/2026/06/11/homebrew-6.0.0/), formulae from non-official
taps have to be [trusted](https://docs.brew.sh/Tap-Trust) explicitly before Homebrew will load
them. Installing by the fully qualified name above trusts this one formula and nothing else, so
no separate step is needed. If you would rather install by short name afterwards, trust it first:

```bash
brew trust --formula vpetersson/apple-health/apple-health-mcp
brew install apple-health-mcp
```

Use `brew trust --tap vpetersson/apple-health` only if you want to trust every current and future
formula and command in the tap. In a `Brewfile`:

```ruby
tap "vpetersson/apple-health", "https://github.com/vpetersson/apple-health-mcp-server"
brew "vpetersson/apple-health/apple-health-mcp", trusted: true
```

Upgrade with `brew upgrade apple-health-mcp`. The formula is published from the
[releases](https://github.com/vpetersson/apple-health-mcp-server/releases) of this repository,
so no separate tap repository is needed.

**From source**:

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
apple-health-mcp import --export-dir /path/to/apple_health_export
```

This parses the XML export, ECG recordings, and GPX workout routes into a local DuckDB database. Re-running import on the same database is safe — records are deduplicated by content hash.

You can re-import while an MCP client has the server loaded. The server opens the database read-only and only for the length of each query, so the file is unlocked in between and the import can take the write lock; if a query happens to be in flight, the import waits a few seconds for it. Once the import finishes, the next query sees the new data — there is no need to restart Claude. A query that lands while the import is still running gets told the database is busy rather than a raw error.

### Database location

Without `--db`, the database path is resolved in this order:

1. `$APPLE_HEALTH_MCP_DB`, if set
2. `./health.duckdb`, if it already exists in the working directory
3. `~/.config/apple-health-mcp/health.duckdb` (or `$XDG_CONFIG_HOME/apple-health-mcp/health.duckdb`)

New installs therefore keep their data under `~/.config`, while setups that already have a
`health.duckdb` next to them keep using it. Pass `--db /path/to/health.duckdb` to override.
To move an existing database to the new location:

```bash
mkdir -p ~/.config/apple-health-mcp
mv ./health.duckdb ~/.config/apple-health-mcp/
```

### Serve

The server supports two transport modes: **HTTP** (Streamable HTTP, the default) and **stdio** (stdin/stdout, for clients that spawn the server as a subprocess — this is what the Claude Desktop bundle uses).

**HTTP** (default):

```bash
apple-health-mcp serve --port 8080
```

The MCP endpoint will be available at `http://127.0.0.1:8080/mcp`.

**stdio**:

```bash
apple-health-mcp serve --transport stdio
```

The server reads JSON-RPC messages from stdin and writes responses to stdout. This is typically invoked by the MCP client directly (see Claude Desktop config below).

## MCP Tools

All tools are read-only. The tabular ones return `{ row_count, rows }`; `get_workout_details` and `get_ecg_data` return their own object shapes. Each tool's exact
output schema is published in `tools/list`.

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

The server speaks MCP revision **2026-07-28** (falling back to 2025-11-25 for clients that
still use the `initialize` handshake) and every tool publishes an `outputSchema`, so
responses arrive as `structuredContent` rather than a wall of text. Tools are annotated
read-only, which lets clients skip approval prompts for them.

### Claude Desktop (one-click, recommended)

Claude Desktop installs local MCP servers as **MCP bundles** (`.mcpb`, formerly `.dxt`)
rather than hand-edited JSON. Each release attaches a per-platform bundle:

1. Build the database once: `apple-health-mcp import --export-dir /path/to/apple_health_export`
2. Download `apple-health-mcp-<version>-<target>.mcpb` from the
   [releases page](https://github.com/vpetersson/apple-health-mcp-server/releases).
3. Open **Settings → Extensions** in Claude Desktop and drag the `.mcpb` file in.
4. **Health database** is prefilled with `~/.config/apple-health-mcp/health.duckdb`, where step 1
   puts it. Change it only if you imported somewhere else.

The bundle carries the server binary, so there is nothing else to install. To build one
yourself from a local checkout:

```bash
cargo build --release
./scripts/build-mcpb.sh target/release/apple-health-mcp dist/apple-health-mcp.mcpb
```

<details>
<summary>Manual JSON configuration (still supported)</summary>

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or
`%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "apple-health": {
      "command": "apple-health-mcp",
      "args": ["serve", "--transport", "stdio"]
    }
  }
}
```

</details>

### Claude Desktop / Claude.ai as a remote connector

To reach a server you are already running over HTTP, add it as a **custom connector**
(**Settings → Connectors → Add custom connector**) pointing at `http://127.0.0.1:8080/mcp`.
Use the bundle above for a local database — a connector is the right shape when the server
runs somewhere else.

### Claude Code

```bash
# stdio — Claude Code starts the server for you
claude mcp add apple-health -- apple-health-mcp serve --db /path/to/health.duckdb --transport stdio

# or against an already-running HTTP server
claude mcp add --transport http apple-health http://127.0.0.1:8080/mcp
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

## Versioning

apple-health-mcp uses calendar versioning in the form `YY.MM.MICRO` — the year, the month, and a counter for releases cut within that month. `26.9.0` is the first release of September 2026, superseding `0.1.0`. Months are not zero-padded, so the version is also a valid SemVer string that Cargo can parse and order.

The version says when a build was cut, not what it guarantees about compatibility. Breaking changes are called out in the release notes.

Releases are tagged `vYY.MM.MICRO`. Each tag builds binaries for Linux (x86_64) and macOS (Apple Silicon and Intel), packages each one as a Claude Desktop `.mcpb` bundle, and attaches both — with a `SHA256SUMS` file — to the [GitHub release](https://github.com/vpetersson/apple-health-mcp-server/releases).

To cut a release: bump `version` in `Cargo.toml`, refresh `Cargo.lock` with `cargo update -p apple-health-mcp`, merge to `master`, then push a `vYY.MM.MICRO` tag.
