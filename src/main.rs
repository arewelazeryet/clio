#![forbid(clippy::unwrap_used)]
use std::{env, sync::Arc, time::Duration};

use axum::{Router, routing::get};
use axum_prometheus::PrometheusMetricLayer;
use mimalloc::MiMalloc;
use rustls::crypto::{CryptoProvider, ring};
use tokio::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod database;
mod routes;
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

#[tokio::main]
async fn main() {
    if let Err(error) = CryptoProvider::install_default(ring::default_provider()) {
        eprintln!("Failed to install rustls crypto provider: {error:?}");
        return;
    }

    setup_logging();

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

    let (layer, metric_handler) = PrometheusMetricLayer::pair();
    tracing::debug!("Initialized Prometheus metrics layer");

    let server_state = Arc::new(tokio::sync::Mutex::new(server));

    let cloned_state = Arc::clone(&server_state);
    tokio::task::spawn(async move {
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
    });

    tracing::debug!("Building HTTP router");
    let app: Router = Router::new()
        .nest("/api/bars", routes::bars::router())
        .nest("/api/graphs", routes::graphs::router())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .route("/metrics", get(|| async move { metric_handler.render() }))
        .layer(layer)
        .with_state(server_state);

    let addr = match env::var("APP_ADDR") {
        Ok(addr) => addr,
        Err(error) => {
            tracing::debug!(%error, "APP_ADDR not set, using default address");
            "0.0.0.0:6726".to_owned()
        }
    };

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%addr, %error, "Failed to bind HTTP listener");
            return;
        }
    };
    tracing::info!(%addr, "Listening for HTTP requests");

    if let Err(error) = axum::serve(listener, app).await {
        tracing::error!(%error, "HTTP server stopped with error");
    }
}
