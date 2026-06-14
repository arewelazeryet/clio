use serde::{Deserialize, Serialize};

/// Calculate ratio between stable and lazer user counts
pub fn ratio(stable: i64, lazer: i64) -> f64 {
    let (stable, lazer) = (stable as f64, lazer as f64);
    lazer / (stable + lazer)
}

#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub enum BucketSize {
    #[default]
    Day,
    Week,
    Month,
}

/// Changelog API entry, simplified
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SinglePointResponse {
    pub timestamp: i64,
    pub stable: i64,
    pub lazer: i64,
    pub sum: i64,
    pub ratio: f64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PointLineResponse {
    pub timestamp: Vec<i64>,
    pub stable: Vec<i64>,
    pub lazer: Vec<i64>,
    pub sum: Vec<i64>,
    pub ratio: Vec<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct RatioRegressionResponse {
    pub target_ratio: f64,
    pub was_reached: bool,
    pub estimated_timestamp: i64,
}
