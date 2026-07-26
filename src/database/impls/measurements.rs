use chrono::{DateTime, Days, Utc};
use color_eyre::eyre::{Context, Result, bail};
use sqlx::{Postgres, query, query_as, query_scalar};

use crate::{
    database::{Database, models::MeasurementEntry},
    types::ratio,
};

impl Database {
    #[tracing::instrument(skip(self))]
    pub async fn insert_measurement(&mut self, entry: MeasurementEntry) -> Result<()> {
        let result = query!(
            r#"
INSERT INTO measurements ( inserted_at, stable, lazer )
VALUES ( to_timestamp($1), $2, $3 )
            "#,
            entry.timestamp as f64,
            entry.stable,
            entry.lazer
        )
        .execute(&*self)
        .await;

        let _ = match result {
            Ok(result) if result.rows_affected() > 0 => tracing::info!("Inserted measurement"),
            Ok(_) => {
                tracing::debug!("Skipped duplicate measurement")
            }
            Err(e) => bail!("Database error: {e}"),
        };
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_count_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching peak lazer user count measurement");
        let result = query_as::<_, MeasurementEntry>(
            r#"
SELECT
    EXTRACT(EPOCH FROM inserted_at)::BIGINT AS timestamp,
    stable,
    lazer
FROM measurements
ORDER BY lazer DESC, inserted_at ASC
LIMIT 1
            "#,
        )
        .fetch_one(&*self)
        .await?;
        tracing::info!(
            stable = result.stable,
            lazer = result.lazer,
            "Found user peak at {}",
            result.timestamp
        );

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_ratio_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching peak lazer ratio measurement");
        let result = query_as::<_, MeasurementEntry>(
            r#"
SELECT
    EXTRACT(EPOCH FROM inserted_at)::BIGINT AS timestamp,
    stable,
    lazer
FROM measurements
WHERE (stable + lazer) > 5000
ORDER BY (lazer::DOUBLE PRECISION / NULLIF((stable + lazer)::DOUBLE PRECISION, 0)) DESC,
         inserted_at ASC
LIMIT 1
            "#,
        )
        .fetch_one(&*self)
        .await?;
        tracing::info!(
            stable = result.stable,
            lazer = result.lazer,
            ratio = ratio(result.stable, result.lazer),
            "Found lazer% peak at {}",
            result.timestamp
        );

        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_user_highest_percentile_peak(&self) -> Result<MeasurementEntry> {
        tracing::debug!("Fetching highest user count within peak ratio percentile");
        let max_percentage: f64 = query_scalar::<Postgres, f64>(
            r#"
            SELECT MAX(lazer::DOUBLE PRECISION / NULLIF((stable + lazer)::DOUBLE PRECISION, 0))
            FROM measurements
            WHERE ((stable + lazer) > 3000)
              AND lazer > 0 AND stable > 0
            "#,
        )
        .fetch_one(&*self)
        .await?;

        tracing::debug!(max_percentage, "Found maximum lazer ratio");

        let peak: MeasurementEntry = query_as(
            r#"
            SELECT
                EXTRACT(EPOCH FROM inserted_at)::BIGINT AS timestamp,
                stable,
                lazer
            FROM measurements
            WHERE ((stable + lazer) > 3000)
              AND lazer > 0 AND stable > 0
              AND (lazer::DOUBLE PRECISION / NULLIF((stable + lazer)::DOUBLE PRECISION, 0)) >= $1
            ORDER BY lazer DESC, inserted_at ASC
            LIMIT 1
            "#,
        )
        .bind(max_percentage - 0.015)
        .fetch_one(&*self)
        .await?;

        tracing::info!(
            timestamp = peak.timestamp,
            stable = peak.stable,
            lazer = peak.lazer,
            ratio = ratio(peak.stable, peak.lazer),
            "Found percentile-adjusted lazer ratio peak"
        );

        Ok(peak)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_history_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MeasurementEntry>> {
        tracing::debug!("Fetching measurement history range");

        let result = query_as::<_, MeasurementEntry>(
            r#"
SELECT
    EXTRACT(EPOCH FROM inserted_at)::BIGINT AS timestamp,
    stable,
    lazer
FROM measurements
WHERE inserted_at >= $1 AND inserted_at < $2
ORDER BY inserted_at ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(&*self)
        .await?;

        tracing::info!(len = result.len(), "Fetched history data");
        Ok(result)
    }

    pub async fn get_past_day(&self) -> Result<Vec<MeasurementEntry>> {
        let now = Utc::now();
        let Some(start) = now.checked_sub_days(Days::new(1)) else {
            tracing::error!(
                timestamp = now.timestamp(),
                "Failed to calculate past-day start time"
            );
            bail!("Failed to calculate past-day start time");
        };

        self.get_history_range(start, now)
            .await
            .wrap_err("Failed to fetch past day")
    }
}
