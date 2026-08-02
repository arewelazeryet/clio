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
}
