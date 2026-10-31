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
        /// JSON function mappings, concurrency, drain timeout and Invoke faults.
        #[arg(long)]
        lambda_functions: Option<PathBuf>,
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
                lambda_functions,
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
    let lambda = lambda_functions
        .map(|path| load_json(path, "Lambda functions"))
        .transpose()?;
    let runtime = std::sync::Arc::new(
        supercloudy::server::runtime_with_services(
            supercloudy::server::Config { faults, buckets },
            supercloudy::server::ServiceConfig {
                ssm,
                openai,
                firebase,
                lambda,
            },
        )
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?,
    );
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    println!("Listening on http://{}", listener.local_addr()?);
    let draining = runtime.clone();
    let result = axum::serve(listener, runtime.router())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Accepted handlers may still need this listener for storage calls.
            draining.shutdown().await;
        })
        .await;
    runtime.shutdown().await;
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Unable to listen for termination");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
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
