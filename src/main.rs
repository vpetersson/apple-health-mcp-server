use apple_health_mcp::{import, paths, server};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "apple-health-mcp", about = "Apple Health MCP Server", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import Apple Health export data into DuckDB
    Import {
        /// Path to the Apple Health export directory
        #[arg(long, default_value = ".")]
        export_dir: PathBuf,

        /// Path to the DuckDB database file [default: ./health.duckdb if it
        /// exists, otherwise ~/.config/apple-health-mcp/health.duckdb]
        #[arg(long)]
        db: Option<PathBuf>,
    },
    /// Run the MCP server
    Serve {
        /// Path to the DuckDB database file [default: ./health.duckdb if it
        /// exists, otherwise ~/.config/apple-health-mcp/health.duckdb]
        #[arg(long)]
        db: Option<PathBuf>,

        /// Port to listen on (HTTP transport only)
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Host to bind to (HTTP transport only)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Transport type: "http" for Streamable HTTP, "stdio" for stdin/stdout
        #[arg(long, default_value = "http")]
        transport: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Import { export_dir, db } => {
            let db = db.unwrap_or_else(paths::default_db_path);
            tracing::info!("Using database {}", db.display());
            paths::ensure_parent_dir(&db)?;
            import::run_import(&export_dir, &db)?;
        }
        Commands::Serve {
            db,
            port,
            host,
            transport,
        } => {
            let db = db.unwrap_or_else(paths::default_db_path);
            if !db.exists() {
                anyhow::bail!(
                    "No database at {}. Run `apple-health-mcp import --export-dir <dir>` first, \
                     or point --db at an existing database.",
                    db.display()
                );
            }
            tracing::info!("Using database {}", db.display());
            server::run_server(&db, &host, port, &transport).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn db_is_optional_on_both_subcommands() {
        let cli = Cli::parse_from(["apple-health-mcp", "import", "--export-dir", "/tmp/export"]);
        match cli.command {
            Commands::Import { export_dir, db } => {
                assert_eq!(export_dir, PathBuf::from("/tmp/export"));
                assert_eq!(db, None);
            }
            _ => panic!("expected import"),
        }

        let cli = Cli::parse_from(["apple-health-mcp", "serve", "--db", "/tmp/health.duckdb"]);
        match cli.command {
            Commands::Serve { db, port, .. } => {
                assert_eq!(db, Some(PathBuf::from("/tmp/health.duckdb")));
                assert_eq!(port, 8080);
            }
            _ => panic!("expected serve"),
        }
    }
}
