use apple_health_mcp::{import, server};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "apple-health-mcp", about = "Apple Health MCP Server")]
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

        /// Path to the DuckDB database file
        #[arg(long, default_value = "./health.duckdb")]
        db: PathBuf,
    },
    /// Run the MCP server
    Serve {
        /// Path to the DuckDB database file
        #[arg(long, default_value = "./health.duckdb")]
        db: PathBuf,

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
            import::run_import(&export_dir, &db)?;
        }
        Commands::Serve {
            db,
            port,
            host,
            transport,
        } => {
            server::run_server(&db, &host, port, &transport).await?;
        }
    }

    Ok(())
}
