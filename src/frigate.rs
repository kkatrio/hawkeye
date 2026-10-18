use chrono::{Datelike, Local, TimeZone, Timelike};
use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;
use tracing::{info, instrument, warn};

mod error;
pub use error::*;

#[derive(Deserialize, Debug)]
pub(crate) struct After {
    pub id: String,
    pub camera: String,
    //label: String,
    start_time: f64,
    //position_changes: u16,
    //score: f64,
    has_snapshot: bool,
    //active: bool,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum Event {
    New { after: After },
    Update {},
    End { after: After },
}

impl After {
    // hour:min:sec-camera-day:month
    pub fn get_name(&self) -> Option<String> {
        let dt = Local
            .timestamp_opt(self.start_time.trunc() as i64, 0)
            .single()?;
        Some(format!(
            "{}:{}:{}-{}-{}-{}.jpg",
            dt.hour(),
            dt.minute(),
            dt.second(),
            self.camera,
            dt.day(),
            dt.month(),
        ))
    }
}

pub struct Client {
    client: reqwest::Client,
    // interior mutability: update token through a &self
    token: Arc<Mutex<String>>,
    frigate_user: String,
    frigate_pass: String,
    frigate_ip: String,
}

impl Client {
    pub async fn new() -> Result<Self, FrigateError> {
        let client = reqwest::Client::new();

        let frigate_user =
            dotenvy::var("FRIGATE_ADMIN").expect("FRIGATE_ADMIN must be set in .env");
        let frigate_pass = dotenvy::var("FRIGATE_PASS").expect("FRIGATE_PASS must be set in .env");
        let frigate_ip = dotenvy::var("FRIGATE_IP").expect("FRIGATE_IP must be set in .env");

        let token = Self::login(&client, &frigate_user, &frigate_pass, &frigate_ip).await?;

        Ok(Self {
            client,
            token: Arc::new(Mutex::new(token)),
            frigate_user,
            frigate_pass,
            frigate_ip,
        })
    }

    async fn login(
        client: &reqwest::Client,
        user: &str,
        pass: &str,
        fip: &str,
    ) -> Result<String, FrigateError> {
        let login_data = serde_json::json!({"user": user, "password": pass});
        let res = client
            .post(format!("http://{}:8971/api/login", fip))
            .json(&login_data)
            .send()
            .await
            .map_err(FrigateError::LoginFailed)?;

        let token = res
            .cookies()
            .find(|c| c.name() == "frigate_token")
            .map(|c| c.value().to_string())
            .ok_or(FrigateError::TokenMissing);
        token
    }

    async fn refresh_token(&self) -> Result<(), FrigateError> {
        let token = Self::login(
            &self.client,
            &self.frigate_user,
            &self.frigate_pass,
            &self.frigate_ip,
        )
        .await?;
        info!("login completed");
        *self.token.lock().await = token;
        Ok(())
    }

    async fn send_with_auth_retry(
        &self,
        build_request: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, FrigateError> {
        let token = { self.token.lock().await.clone() };
        let res = build_request(&token).send().await?;

        // if frigate has been restarted refresh the token
        // TODO: log the error, refresh for any kind of error
        if res.status().is_client_error() {
            warn!("unauthenticated. Refreshing token...");
            self.refresh_token().await?;
            let token = { self.token.lock().await.clone() };
            let res = build_request(&token).send().await?;
            return Ok(res);
        }

        let status = res.status();
        if status != reqwest::StatusCode::OK {
            let text = res.text().await?;
            return Err(FrigateError::BadResponse { status, body: text });
        }
        Ok(res)
    }

    #[allow(dead_code)]
    async fn get<T>(&self, url: &str) -> Result<Vec<T>, FrigateError>
    where
        T: serde::de::DeserializeOwned,
    {
        let res = self
            .send_with_auth_retry(|token| self.client.get(url).bearer_auth(token))
            .await?;
        Ok(serde_json::from_str(&res.text().await?)?)
    }

    async fn put(&self, url: &str, data: &serde_json::Value) -> Result<(), FrigateError> {
        self.send_with_auth_retry(|token| {
            self.client
                .put(url)
                .bearer_auth(token)
                .json(data)
                .header("Accept", "application/json")
        })
        .await?;
        Ok(())
    }

    pub async fn get_event_snapshot(&self, after_event: &After) -> Result<PathBuf, ClientError> {
        if !after_event.has_snapshot {
            return Err(ClientError::Frigate(FrigateError::SnapshotMissing));
        }
        let url = format!(
            "http://{}:8971/api/events/{}/snapshot.jpg",
            self.frigate_ip, after_event.id
        );

        let res = self
            .send_with_auth_retry(|token| self.client.get(&url).bearer_auth(token))
            .await?;

        let mut path = PathBuf::from(r"./tmp");
        let name = after_event.get_name().ok_or(ClientError::TimestampError)?;
        path.push(format!("{}.jpg", name));
        let mut file = File::create(&path)?;
        file.write_all(&res.bytes().await?)?;
        Ok(path)
    }

    pub async fn update_config(&self, config: &serde_json::Value) -> Result<(), FrigateError> {
        let url = format!("http://{}:8971/api/config/set", self.frigate_ip);
        self.put(&url, config).await
    }
}

struct CameraConfig;
impl CameraConfig {
    pub const EVENING_THESHOLDS: &[(&str, u16)] = &[
        ("pisw", 15),
        ("voreia-eisodos", 15),
        ("porta", 15),
        ("deuteri-porta", 15),
        ("mpros", 15),
        ("peuko-garage", 15),
        ("anatolika", 15),
        ("varka", 15),
    ];

    pub const MORNING_THESHOLDS: &[(&str, u16)] = &[
        ("pisw", 30),
        ("voreia-eisodos", 30),
        ("porta", 30),
        ("deuteri-porta", 30),
        ("mpros", 30),
        ("peuko-garage", 30),
        ("anatolika", 30),
        ("varka", 30),
    ];
}

#[derive(Debug)]
pub enum Time {
    Morning,
    Evening,
}

fn create_patches(time: &Time) -> Vec<serde_json::Value> {
    let thresholds = match time {
        Time::Morning => CameraConfig::MORNING_THESHOLDS,
        Time::Evening => CameraConfig::EVENING_THESHOLDS,
    };
    thresholds
        .iter()
        .map(|(name, threshold)| {
            serde_json::json!({
                "requires_restart": 0,
                "update_topic": "integer",
                "config_data": {
                    "cameras": {
                        *name: {
                            "motion": {
                                "threshold": threshold
                            }
                        }
                    }
                }
            })
        })
        .collect()
}

#[instrument(skip(client))]
pub async fn config_updater(schedule: cron::Schedule, time: Time, client: Arc<Client>) {
    for next_datetime in schedule.upcoming(chrono::Local) {
        info!("Upcoming fire time to change the threshold {next_datetime}");
        // difference should be positive
        let seconds_until_next = (next_datetime - chrono::Local::now())
            .to_std()
            .unwrap_or_default(); // default=0
        tokio::time::sleep(seconds_until_next).await;

        for patch in create_patches(&time) {
            match client.update_config(&patch).await {
                Ok(_) => {
                    info!("updated configuration");
                }
                Err(e) => {
                    error!("configuration update failed: {e}")
                }
            }
        }
    }
}
