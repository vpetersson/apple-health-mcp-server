# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MCP (Model Context Protocol) server for querying Apple Health export data. Imports Apple Health exports into DuckDB and exposes the data through an MCP Streamable HTTP server.

## Build & Run

```bash
cargo build                    # debug build
cargo build --release          # release build
cargo clippy                   # lint
cargo fmt                      # format
```

**Import** Apple Health data (expects an unzipped Apple Health export directory containing `export.xml`, `electrocardiograms/`, `workout-routes/`):
```bash
cargo run -- import --export-dir /path/to/apple_health_export --db ./health.duckdb
```

**Serve** the MCP server:
```bash
cargo run -- serve --db ./health.duckdb --port 8080 --host 127.0.0.1
```

The MCP endpoint is at `http://<host>:<port>/mcp`.

`--db` is optional on both subcommands; see `src/paths.rs` for how it is resolved.

## Versioning

CalVer, `YY.MM.MICRO` (`26.9.0` = first release of September 2026). Months are not zero-padded — Cargo requires a valid SemVer string and SemVer rejects leading zeros. To cut a release: bump `version` in `Cargo.toml`, refresh `Cargo.lock` with `cargo update -p apple-health-mcp`, merge to `master`, then push a `vYY.MM.MICRO` tag. The tag triggers `.github/workflows/release.yml`, which builds the Linux and macOS binaries, packages each as a `.mcpb` bundle, creates the GitHub release, and regenerates the Homebrew formula. `mcpb/manifest.json` does **not** need bumping — `scripts/build-mcpb.sh` injects the version from `Cargo.toml` at pack time.

## Desktop distribution

Claude Desktop installs local MCP servers as MCP bundles (`.mcpb`), not hand-written `claude_desktop_config.json` entries. `mcpb/manifest.json` describes the bundle (a `binary` server plus a `database_path` user config field); `scripts/build-mcpb.sh <binary> <output.mcpb>` stages the binary under `server/` and packs it with `@anthropic-ai/mcpb`. CI validates the manifest against the official schema on every PR.

## Architecture

**CLI** (`src/main.rs`): Two subcommands via clap — `import` and `serve`.

**Paths** (`src/paths.rs`): Resolves the default database location — `$APPLE_HEALTH_MCP_DB`, then an existing `./health.duckdb` in the working directory (the pre-`~/.config` location), then `~/.config/apple-health-mcp/health.duckdb`. `PathEnv` captures the environment explicitly so resolution is testable without mutating process state.

**Import pipeline** (`src/import/`): Multi-phase process orchestrated by `import::run_import`:
1. `xml.rs` — Streams `export.xml` with quick-xml, bulk-loads records, workouts, activity summaries, workout events/statistics, and record metadata using DuckDB's Appender API. Batches rows (100k) before flushing. Skips Correlation children (they appear as top-level records).
2. `ecg.rs` — Parses ECG CSV files from `electrocardiograms/` directory. Each CSV has header key-value pairs followed by voltage sample data.
3. `gpx.rs` — Parses GPX route files from `workout-routes/`, linking to workouts via a route map built from XML FileReference elements.
4. Post-load: deduplicates all tables using `DISTINCT ON` (since tables lack PRIMARY KEY to allow Appender usage), then rebuilds a `daily_record_stats` aggregation table.

**Database** (`src/db.rs`): DuckDB schema with tables: `records`, `record_metadata`, `workouts`, `workout_events`, `workout_statistics`, `activity_summaries`, `ecg_readings`, `ecg_samples`, `route_points`, `imports`, and the derived `daily_record_stats`. All entities are deduplicated by hash columns (SHA-256 of key fields, computed in `models.rs`).

**MCP Server** (`src/server/`): Uses `rmcp` 3.x (MCP revision 2026-07-28) with `#[tool_router]` / `#[tool]` macros on `HealthServer`. Opens the DB read-only, and only for the duration of each query (see `server::Database`). Served over Streamable HTTP via axum or over stdio. Tool parameter structs live in `server/tools.rs`; tool *output* types live in `server/results.rs`. Both use `schemars::JsonSchema` — the output types become each tool's `outputSchema`, and results are returned as `Json<T>` so responses carry `structuredContent`. The `run_custom_query` tool allows arbitrary read-only SQL (SELECT/WITH only).

## Key Patterns

- **Deduplication over constraints**: Tables are created without PRIMARY KEYs so DuckDB's Appender can bulk-load. Deduplication runs as a post-load step via `CREATE OR REPLACE TABLE ... SELECT DISTINCT ON`.
- **Schema reconciliation**: `db::TABLES` is the single source of truth for every table the import appends into, and the Appender binds by *position*, so a database whose tables drifted from that list writes each value into its neighbour's column. `CREATE TABLE IF NOT EXISTS` cannot repair that, and DuckDB only reports the resulting cast failure (`invalid timestamp field format: "import_..."` is `import_id` landing in a `TIMESTAMP` column), so `ensure_schema` compares each table against `TABLES` and rebuilds the ones that no longer match, carrying rows across by column name. The comparison deliberately ignores NOT NULL and DEFAULT, which `deduplicate_tables` strips on every import — the `imports.imported_at` default is one of them, which is why the import writes `CURRENT_TIMESTAMP` explicitly.
- **Hash-based identity**: All entities use SHA-256 hashes of their key fields as identifiers (`compute_hash` in `models.rs`).
- **Date handling**: Apple Health dates include timezone suffixes (`+0000`) that are stripped before inserting into DuckDB TIMESTAMP columns. See `clean_date` in `xml.rs` and `clean_timestamp` in `gpx.rs`.
- **Query results**: `HealthServer::query_to_json` maps each DuckDB `ValueRef` to JSON; NULL columns are omitted from the row object. Temporal types have no DuckDB-side `String` conversion, so `timestamp_to_string` / `date32_to_string` / `time64_to_string` render them explicitly — an unhandled temporal variant silently drops the column from every tool response.
- **Tool results**: tools return `Result<Json<T>, McpError>`. `Json<T>` makes rmcp emit both `structuredContent` and a JSON text block; `Err` makes the client see a real protocol error instead of a string starting with "Error:". All tools are annotated `read_only_hint` / `idempotent_hint` / `open_world_hint = false`.
- **Server identity**: `get_info` is implemented by hand because rmcp's `Implementation::from_build_env()` reports rmcp's own crate name and version, not this crate's.
- **Never hold the database open**: DuckDB locks the file for as long as *any* connection is open, read-only included, and refuses a read-write open from another process while that lock is held. A long-lived connection therefore blocks `apple-health-mcp import` for the whole session — and the Claude Desktop bundle keeps a session open permanently. `server::Database::File` reopens per query (~3 ms even on a 400 MB database) so the file is unlocked between calls; `Database::Memory` keeps its connection because there is no file and nothing to reopen from. The cross-process behaviour is only reproducible by running the binary twice — DuckDB's lock is per-process — so the regression guard is `tests/test_concurrent_access.rs`, which drives the real server over stdio while a real import runs.
- **Lock-aware errors**: `db::open_db` (read-write) waits out a reader that is mid-query before giving up, and `server::unavailable` turns DuckDB's lock message into "an import is probably running" rather than passing raw IO-error text to the model. `db::is_lock_conflict` matches on the message because DuckDB has no distinct error code for it.
- **Router wiring**: `#[tool_handler(router = self.tool_router)]` — the macro's default (`Self::tool_router()`) rebuilds the router on every request and leaves the struct field unread.
- **Release & Homebrew**: the `formula` job in `.github/workflows/release.yml` runs after the release is published, renders `Formula/apple-health-mcp.rb` from `scripts/formula.rb.tmpl` via `scripts/update-formula.sh` using the release's `SHA256SUMS`, and commits it through the contents API (so the commit is signed by GitHub). The formula is generated, never hand-edited; CI renders it from a synthetic release and checks it parses. The repository doubles as its own Homebrew tap, and the release assets are bare binaries named `apple-health-mcp-<tag>-<target>`, so the formula stages a single file rather than unpacking an archive. Non-official taps require explicit trust since Homebrew 6.0.0 — install instructions must use the fully qualified formula name or `brew trust`.
