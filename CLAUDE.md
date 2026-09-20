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

CalVer, `YY.MM.MICRO` (`26.9.0` = first release of September 2026). Months are not zero-padded — Cargo requires a valid SemVer string and SemVer rejects leading zeros. To cut a release: bump `version` in `Cargo.toml`, refresh `Cargo.lock` with `cargo update -p apple-health-mcp`, merge to `master`, then push a `vYY.MM.MICRO` tag. The tag triggers `.github/workflows/release.yml`, which builds the Linux and macOS binaries, creates the GitHub release, and regenerates the Homebrew formula.

## Architecture

**CLI** (`src/main.rs`): Two subcommands via clap — `import` and `serve`.

**Paths** (`src/paths.rs`): Resolves the default database location — `$APPLE_HEALTH_MCP_DB`, then an existing `./health.duckdb` in the working directory (the pre-`~/.config` location), then `~/.config/apple-health-mcp/health.duckdb`. `PathEnv` captures the environment explicitly so resolution is testable without mutating process state.

**Import pipeline** (`src/import/`): Multi-phase process orchestrated by `import::run_import`:
1. `xml.rs` — Streams `export.xml` with quick-xml, bulk-loads records, workouts, activity summaries, workout events/statistics, and record metadata using DuckDB's Appender API. Batches rows (100k) before flushing. Skips Correlation children (they appear as top-level records).
2. `ecg.rs` — Parses ECG CSV files from `electrocardiograms/` directory. Each CSV has header key-value pairs followed by voltage sample data.
3. `gpx.rs` — Parses GPX route files from `workout-routes/`, linking to workouts via a route map built from XML FileReference elements.
4. Post-load: deduplicates all tables using `DISTINCT ON` (since tables lack PRIMARY KEY to allow Appender usage), then rebuilds a `daily_record_stats` aggregation table.

**Database** (`src/db.rs`): DuckDB schema with tables: `records`, `record_metadata`, `workouts`, `workout_events`, `workout_statistics`, `activity_summaries`, `ecg_readings`, `ecg_samples`, `route_points`, `imports`, and the derived `daily_record_stats`. All entities are deduplicated by hash columns (SHA-256 of key fields, computed in `models.rs`).

**MCP Server** (`src/server/`): Uses `rmcp` crate with `#[tool_router]` / `#[tool]` macros on `HealthServer`. Opens DB read-only. Served over Streamable HTTP via axum. Tool parameter structs live in `server/tools.rs` and use `schemars::JsonSchema` for MCP schema generation. The `run_custom_query` tool allows arbitrary read-only SQL (SELECT/WITH only).

## Key Patterns

- **Deduplication over constraints**: Tables are created without PRIMARY KEYs so DuckDB's Appender can bulk-load. Deduplication runs as a post-load step via `CREATE OR REPLACE TABLE ... SELECT DISTINCT ON`.
- **Hash-based identity**: All entities use SHA-256 hashes of their key fields as identifiers (`compute_hash` in `models.rs`).
- **Date handling**: Apple Health dates include timezone suffixes (`+0000`) that are stripped before inserting into DuckDB TIMESTAMP columns. See `clean_date` in `xml.rs` and `clean_timestamp` in `gpx.rs`.
- **Release & Homebrew**: the `formula` job in `.github/workflows/release.yml` runs after the release is published, renders `Formula/apple-health-mcp.rb` from `scripts/formula.rb.tmpl` via `scripts/update-formula.sh` using the release's `SHA256SUMS`, and commits it through the contents API (so the commit is signed by GitHub). The formula is generated, never hand-edited; CI renders it from a synthetic release and checks it parses. The repository doubles as its own Homebrew tap, and the release assets are bare binaries named `apple-health-mcp-<tag>-<target>`, so the formula stages a single file rather than unpacking an archive. Non-official taps require explicit trust since Homebrew 6.0.0 — install instructions must use the fully qualified formula name or `brew trust`.
- **Query results**: `HealthServer::query_to_json` converts all DuckDB columns to strings first, then attempts numeric parsing — this means all tool responses are JSON arrays of objects with string or numeric values.
