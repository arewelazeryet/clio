//! Data for bars of stable vs lazer info

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};

use crate::{server::ServerState, types::SinglePointResponse};

async fn get_current(
    State(state): State<ServerState>,
) -> Result<Json<SinglePointResponse>, StatusCode> {
    let mut state = state.lock().await;
    let changelog = state
        .get_latest_changelog()
        .await
        .inspect_err(|e| tracing::warn!("Error on current data: {e}"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tracing::info!(
        stable = changelog.stable,
        lazer = changelog.lazer,
        "Served current bar data"
    );

    Ok(Json(changelog))
}

async fn get_highest_user_count(
    State(state): State<ServerState>,
) -> Result<Json<SinglePointResponse>, StatusCode> {
    let mut state = state.lock().await;
    let response = state
        .get_peak_user_count()
        .await
        .inspect_err(|error| tracing::warn!(%error, "Failed to fetch peak user count from cache"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    tracing::info!("Served peak user count bar data");

    Ok(Json(response))
}

async fn get_highest_user_percentage(
    State(state): State<ServerState>,
) -> Result<Json<SinglePointResponse>, StatusCode> {
    let mut state = state.lock().await;
    let response = state
        .get_peak_user_ratio()
        .await
        .inspect_err(|error| tracing::warn!(%error, "Failed to fetch peak ratio from cache"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    tracing::info!("Served peak ratio bar data");

    Ok(Json(response))
}

async fn get_highest_user_count_within_85th_percentile(
    State(state): State<ServerState>,
) -> Result<Json<SinglePointResponse>, StatusCode> {
    let mut state = state.lock().await;
    let response = state
        .get_peak_user_percentile()
        .await
        .inspect_err(|error| tracing::warn!(%error, "Failed to fetch peak percentile from cache"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    tracing::info!("Served peak percentile bar data");

    Ok(Json(response))
}

pub fn router() -> Router<ServerState> {
    tracing::debug!("Building bars router");
    Router::new()
        .route("/current", get(get_current))
        .route("/peak_users", get(get_highest_user_count))
        .route("/peak_ratio", get(get_highest_user_percentage))
        .route(
            "/peak_percentile",
            get(get_highest_user_count_within_85th_percentile),
        )
}
