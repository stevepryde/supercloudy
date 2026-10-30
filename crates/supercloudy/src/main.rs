use std::{net::Ipv4Addr, path::PathBuf};

use clap::{Parser, Subcommand};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the local emulator.
    Up {
        /// TCP port to listen on (0 selects an available port).
        #[arg(long, default_value_t = 8000)]
        port: u16,
        /// JSON file containing explicit deterministic fault rules.
        #[arg(long)]
        faults: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let Cli {
        command: Command::Up { port, faults },
    } = Cli::parse();
    let router = match faults {
        Some(path) => {
            let config = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
            supercloudy::server::router_with_faults(config)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
        }
        None => supercloudy::server::router(),
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("Unable to listen for Ctrl-C: {error}");
            }
        })
        .await
}
