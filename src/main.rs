#![forbid(clippy::unwrap_used)]
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use color_eyre::eyre::Context;
use mimalloc::MiMalloc;
use rustls::crypto::{CryptoProvider, ring};
use tokio::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod database;
mod server;
mod types;

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(format!(
            "{}=debug,tower_http=debug,axum::rejection=trace",
            env!("CARGO_CRATE_NAME")
        )))
        .with_line_number(true)
        .with_file(true)
        .init();
}

fn prometheus() -> color_eyre::Result<()> {
    let ip: SocketAddr = ([0, 0, 0, 0], 10101).into();
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new().with_http_listener(ip);

    builder.install().wrap_err("Failed to install Prometheus")?;
    tracing::info!(ip = ?ip, "Installing Prometheus");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = CryptoProvider::install_default(ring::default_provider()) {
        eprintln!("Failed to install rustls crypto provider: {error:?}");
        return;
    }

    setup_logging();
    prometheus().unwrap();

    #[allow(clippy::unwrap_used)]
    let _ = dotenvy::dotenv();

    tracing::info!(
        crate_name = env!("CARGO_CRATE_NAME"),
        "Starting application"
    );

    let server = match server::Server::init().await {
        Ok(server) => server,
        Err(error) => {
            tracing::error!(%error, "Server initialization failed");
            return;
        }
    };

    let server_state = Arc::new(tokio::sync::Mutex::new(server));

    let cloned_state = Arc::clone(&server_state);
    tracing::info!(interval_minutes = 1, "Initializing cache update loop");
    let mut interval = tokio::time::interval_at(
        Instant::now() + Duration::from_mins(1),
        Duration::from_mins(1),
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        tracing::debug!("Starting scheduled cache update");
        if let Err(error) = cloned_state.lock().await.update_cache().await {
            tracing::error!(%error, "Scheduled cache update failed");
        }
    }
}
