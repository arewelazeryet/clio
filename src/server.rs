use std::{env, sync::Arc};

use chrono::{Local, Utc};
use color_eyre::eyre::{Context, Result};
use pastey::paste;
use redis::{AsyncCommands, JsonAsyncCommands};
use rosu_v2::Osu;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::{
    database::Database,
    types::{BucketSize, PointLineResponse, SinglePointResponse, ratio},
};

const STREAM_NAMES: [&str; 4] = ["stable40", "cuttingedge", "lazer", "tachyon"];

enum Stream {
    Stable(i64),
    Lazer(i64),
}

pub struct Server {
    database: Database,
    osu_client: Osu,
    redis: redis::aio::ConnectionManager,
}

pub type ServerState = Arc<Mutex<Server>>;

fn parse_json_root<T: DeserializeOwned>(value: &str, key: &str) -> Result<T> {
    let mut parsed: Vec<T> = serde_json::from_str(value)?;
    parsed.pop().ok_or_else(|| {
        color_eyre::eyre::eyre!("cache entry {} was missing its JSON root value", key)
    })
}

macro_rules! cache_json_pair {
    (
        $suffix:ident,
        key = $key:expr,
        ty = $ty:ty,
        ttl = $ttl:expr,
        refresh => |$this:ident| $($refresh:tt)+
    ) => {
        paste! {
            pub async fn [<set_ $suffix>](&mut self, value: &$ty) -> Result<()> {
                let payload = serde_json::to_value(value)?;
                let _: () = self.cache().json_set($key, "$", &payload).await?;
                let _: bool = self.cache().expire($key, $ttl).await?;

                tracing::debug!(key = $key, ttl = $ttl, "Updated cache entry");
                Ok(())
            }

            pub async fn [<refresh_ $suffix>](&mut self) -> Result<$ty> {
                tracing::info!(key = $key, "Attempting to refresh cache entry");
                let $this = self;
                let value = { $($refresh)+ };
                $this.[<set_ $suffix>](&value).await?;
                tracing::info!(key = $key, "Refreshed cache entry");
                Ok(value)
            }

            pub async fn [<get_ $suffix>](&mut self) -> Result<$ty> {
                let ttl: i64 = self.cache().ttl($key).await?;

                if ttl <= 0 {
                    tracing::debug!(key = $key, ttl, "Cache entry expired or missing");
                    return self.[<refresh_ $suffix>]().await;
                }

                let serialized: String = self.cache().json_get($key, "$").await?;
                let value: $ty = parse_json_root(&serialized, $key)?;

                tracing::info!(key = $key, expires_in = ttl, "Fetched cache entry");
                Ok(value)
            }
        }
    };
    (
        $suffix:ident,
        key = $key:expr,
        ty = $ty:ty,
        ttl = $ttl:expr
    ) => {
        paste! {
            pub async fn [<set_ $suffix>](&mut self, value: &$ty) -> Result<()> {
                let payload = serde_json::to_value(value)?;
                let _: () = self.cache().json_set($key, "$", &payload).await?;
                let _: bool = self.cache().expire($key, $ttl).await?;

                tracing::debug!(key = $key, ttl = $ttl, "Updated cache entry");
                Ok(())
            }

            pub async fn [<get_ $suffix>](&mut self) -> Result<$ty> {
                let ttl: i64 = self.cache().ttl($key).await?;

                if ttl <= 0 {
                    tracing::debug!(key = $key, ttl, "Cache entry expired or missing");
                    return Err(color_eyre::eyre::eyre!(
                        "cache entry {} expired without a refresh function",
                        $key
                    ));
                }

                let serialized: String = self.cache().json_get($key, "$").await?;
                let value: $ty = parse_json_root(&serialized, $key)?;

                tracing::info!(key = $key, expires_in = ttl, "Fetched cache entry");
                Ok(value)
            }
        }
    };
    (
        $suffix:ident,
        key = $key:expr,
        ty = $ty:ty,
        refresh => |$this:ident| $($refresh:tt)+

    ) => {
        paste! {
            pub async fn [<set_ $suffix>](&mut self, value: &$ty) -> Result<()> {
                let payload = serde_json::to_value(value)?;
                let _: () = self.cache().json_set($key, "$", &payload).await?;

                tracing::debug!(key = $key, "Updated cache entry");
                Ok(())
            }

            pub async fn [<get_ $suffix>](&mut self) -> Result<$ty> {
                let serialized: String = self.cache().json_get($key, "$").await?;
                let value: $ty = parse_json_root(&serialized, $key)?;

                tracing::info!(key = $key, "Fetched cache entry");
                Ok(value)
            }

            pub async fn [<refresh_ $suffix>](&mut self) -> Result<$ty> {
                tracing::info!(key = $key, "Attempting to refresh cache entry");
                let $this = self;
                let value = { $($refresh)+ };
                $this.[<set_ $suffix>](&value).await?;
                tracing::info!(key = $key, "Refreshed cache entry");
                Ok(value)
            }
        }
    };

}

impl Server {
    pub async fn init() -> Result<Self> {
        tracing::info!("Initializing server dependencies");

        let database_url = env::var("DATABASE_URL")?;
        let client_id: u64 = env::var("OSU_API_CLIENT_ID")?.parse()?;
        let client_secret = env::var("OSU_API_CLIENT_SECRET")?;

        tracing::debug!("Connecting to database");
        let database = Database::new(&database_url).await?;
        // Run migrations owo
        cfg_select! {
            not(debug_assertions) => {
                database.migrate().await?;
            }
            _ => {}
        }

        tracing::debug!(client_id, "Building osu! API client");
        let client = rosu_v2::OsuBuilder::new()
            .client_id(client_id)
            .client_secret(client_secret)
            .ratelimit(1)
            .build()
            .await?;

        let redis_client =
            redis::Client::open(std::env::var("CACHE_URL").wrap_err("Failed to fetch CACHE_URL")?)?;
        let redis = redis::aio::ConnectionManager::new(redis_client).await?;

        let mut server = Self {
            database: database,
            osu_client: client,
            redis,
        };

        tracing::info!("Refreshing redis cache");
        server.update_cache().await?;

        tracing::info!("Server initialization complete");

        Ok(server)
    }

    pub async fn update_cache(&mut self) -> Result<()> {
        tracing::info!("Starting application cache update");
        let current_timestamp = Utc::now();

        let initial_changelog = self.refresh_latest_changelog().await?;
        let peak_users = self.refresh_peak_user_count().await?;
        let peak_ratio = self.refresh_peak_user_ratio().await?;
        let peak_percentile = self.refresh_peak_user_percentile().await?;
        let graph_day_users = self.refresh_day_user_graph().await?;
        let graph_history_users = self.refresh_history_user_graph().await?;

        let day_points = graph_day_users.timestamp.len();
        let history_points = graph_history_users.timestamp.len();

        tracing::info!(
            timestamp = current_timestamp.timestamp(),
            latest_stable = initial_changelog.stable,
            latest_lazer = initial_changelog.lazer,
            peak_users_timestamp = peak_users.timestamp,
            peak_ratio_timestamp = peak_ratio.timestamp,
            peak_percentile_timestamp = peak_percentile.timestamp,
            day_points,
            history_points,
            "Application cache update data loaded"
        );

        tracing::info!("Application cache update complete");

        Ok(())
    }

    pub fn database(&mut self) -> &mut Database {
        &mut self.database
    }

    pub fn osu(&self) -> &Osu {
        &self.osu_client
    }

    pub fn cache(&mut self) -> &mut redis::aio::ConnectionManager {
        &mut self.redis
    }

    cache_json_pair!(
        latest_changelog,
        key = "arewelazeryet:changelog:latest",
        ty = SinglePointResponse,
        refresh => |server| {
            let entry = fetch_changelog(server.osu()).await?;
            server.insert_new_entry(entry.clone()).await?;
            entry
        }
    );

    cache_json_pair!(
        peak_user_count,
        key = "arewelazeryet:peak:users",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_count_peak().await?.into()
    );

    cache_json_pair!(
        peak_user_ratio,
        key = "arewelazeryet:peak:ratio",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_ratio_peak().await?.into()
    );

    cache_json_pair!(
        peak_user_percentile,
        key = "arewelazeryet:peak:percentile",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_highest_percentile_peak().await?.into()
    );

    cache_json_pair!(
        day_user_graph,
        key = "arewelazeryet:graph:day",
        ty = PointLineResponse,
        ttl = 300,
        refresh => |server| server.database().get_past_day().await?.into()
    );

    cache_json_pair!(
        history_user_graph,
        key = "arewelazeryet:graph:history",
        ty = PointLineResponse,
        ttl = 300,
        refresh => |server| server.database().get_history(BucketSize::Day).await?.into()
    );

    pub fn insert_new_entry(
        &mut self,
        entry: SinglePointResponse,
    ) -> impl Future<Output = Result<()>> {
        self.database.insert_measurement(entry.into())
    }
}

pub async fn fetch_changelog(osu: &Osu) -> Result<SinglePointResponse> {
    tracing::debug!("Fetching changelog stream data from osu! API");
    let stream = osu.changelog_listing().await?;
    let stream_count = stream.streams.len();

    let (stable, lazer) = stream
        .streams
        .into_iter()
        .filter(|s| STREAM_NAMES.contains(&s.name.as_str()))
        .map(|s| match s.name.as_str() {
            "stable40" | "cuttingedge" => {
                let user_count = match s.user_count {
                    Some(user_count) => user_count,
                    None => {
                        tracing::warn!(stream = %s.name, "Changelog stream missing user count");
                        0
                    }
                };
                Stream::Stable(user_count)
            }
            "lazer" | "tachyon" => {
                let user_count = match s.user_count {
                    Some(user_count) => user_count,
                    None => {
                        tracing::warn!(stream = %s.name, "Changelog stream missing user count");
                        0
                    }
                };
                Stream::Lazer(user_count)
            }
            stream => unreachable!("All valid stream names are matched, wtf is {stream}"),
        })
        .fold((0, 0), |(stable, lazer), stream| match stream {
            Stream::Stable(v) => (stable + v, lazer),
            Stream::Lazer(v) => (stable, lazer + v),
        });

    let entries = SinglePointResponse {
        timestamp: Local::now().timestamp(),
        stable,
        lazer,
        ratio: ratio(stable, lazer),
        sum: stable + lazer,
    };

    tracing::info!(
        timestamp = entries.timestamp,
        stable = entries.stable,
        lazer = entries.lazer,
        sum = entries.sum,
        ratio = entries.ratio,
        stream_count,
        "Fetched changelog user counts"
    );

    Ok(entries)
}
