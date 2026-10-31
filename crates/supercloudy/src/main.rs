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
        /// Create an in-memory S3 bucket (repeatable).
        #[arg(long = "bucket")]
        buckets: Vec<String>,
        /// JSON file containing synthetic immutable SSM parameters.
        #[arg(long)]
        ssm_parameters: Option<PathBuf>,
        /// JSON file enabling local Responses fixtures and optional faults.
        #[arg(long)]
        openai_fixtures: Option<PathBuf>,
        /// Enable an in-memory Firebase ID token issuer for synthetic users.
        #[arg(long)]
        firebase: bool,
        /// Default mint project (also enables the Firebase issuer).
        #[arg(long)]
        firebase_project_id: Option<String>,
        /// Certificate cache lifetime in seconds (default 3600; enables Firebase).
        #[arg(long)]
        firebase_cert_max_age: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let Cli {
        command:
            Command::Up {
                port,
                faults,
                buckets,
                ssm_parameters,
                openai_fixtures,
                firebase,
                firebase_project_id,
                firebase_cert_max_age,
            },
    } = Cli::parse();
    let faults = faults
        .map(|path| load_json(path, "faults"))
        .transpose()?
        .unwrap_or_default();
    let ssm = ssm_parameters
        .map(|path| load_json(path, "SSM parameters"))
        .transpose()?
        .unwrap_or_default();
    let openai = openai_fixtures
        .map(|path| load_json(path, "OpenAI fixtures"))
        .transpose()?;
    let firebase = (firebase || firebase_project_id.is_some() || firebase_cert_max_age.is_some())
        .then_some(supercloudy::server::FirebaseConfig {
            project_id: firebase_project_id,
            cert_max_age: firebase_cert_max_age.unwrap_or(3600),
        });
    let router = supercloudy::server::router_with_services(
        supercloudy::server::Config { faults, buckets },
        supercloudy::server::ServiceConfig {
            ssm,
            openai,
            firebase,
        },
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
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

fn load_json<T: serde::de::DeserializeOwned>(path: PathBuf, kind: &str) -> std::io::Result<T> {
    serde_json::from_slice(&std::fs::read(path)?).map_err(|error| {
        // Serde error messages can contain supplied values. Keep startup errors value-free.
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid {kind} JSON at line {}, column {} ({:?})",
                error.line(),
                error.column(),
                error.classify()
            ),
        )
    })
}
