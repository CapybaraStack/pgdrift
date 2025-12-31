use clap::{Parser, Subcommand};

mod commands;
mod output;

#[derive(Parser)]
#[command(
    name = "pgdrift",
    about = "A tool to detect and manage schema drift in PostgreSQL databases."
)]
#[command(author, version, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List all jsonb columns in the database
    Discover {
        /// DB connection URL
        #[arg(short, long, env = "DATABASE_URL")]
        database_url: String,

        /// Output format
        #[arg(short, long, value_enum, default_value = "table")]
        format: output::OutputFormat,
    },

    /// Analyze a jsonb column for schema drift
    Analyze {
        /// DB connection URL
        #[arg(short, long, env = "DATABASE_URL")]
        database_url: String,

        /// Table name
        // #[arg(short, long)]
        table: String,

        /// Column name
        // #[arg(short, long)]
        column: String,

        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: output::OutputFormat,

        /// Number of samples to analyze
        #[arg(short, long, default_value = "5000")]
        sample_size: usize,

        /// Enable production mode
        #[arg(long)]
        production_mode: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Discover {
            database_url,
            format,
        } => {
            commands::discover::run(&database_url, format).await?;
        }
        Commands::Analyze {
            database_url,
            table,
            column,
            sample_size,
            format,
            production_mode,
        } => {
            commands::analyze::run(
                &database_url,
                &table,
                &column,
                sample_size,
                format,
                production_mode,
            )
            .await?;
        }
    }
    Ok(())
}
