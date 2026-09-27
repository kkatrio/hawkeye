use chrono::{Datelike, Local, TimeZone, Timelike};
use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;
use std::{fmt, path::PathBuf};
use tokio::sync::Mutex;
use tracing::{info, warn};

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
    // TODO: return a Result
    // hour:min:sec-camera-day:month
    pub fn get_name(&self) -> String {
        let dt = Local
            .timestamp_opt(self.start_time.trunc() as i64, 0)
            .unwrap();
        format!(
            "{}:{}:{}-{}-{}-{}.jpg",
            dt.hour(),
            dt.minute(),
            dt.second(),
            self.camera,
            dt.day(),
            dt.month(),
        )
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Motion {
    motion: f64,
    camera: String,
    start_time: i64,
}

impl fmt::Display for Motion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -- {} start: {}",
            self.motion,
            self.camera,
            Local.timestamp_opt(self.start_time, 0).unwrap()
        )
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct Review {
    id: String,
    camera: String,
    severity: String,
    start_time: f64,
    end_time: f64,
}

impl fmt::Display for Review {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -- {} start: {} end: {}",
            self.id,
            self.severity,
            timestamp_to_iso(self.start_time),
            timestamp_to_iso(self.end_time)
        )
    }
}

fn timestamp_to_iso(ts: f64) -> String {
    let secs = ts.trunc();
    let iso = Local.timestamp_opt(secs as i64, 0).unwrap();
    iso.to_string()
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

        let frigate_user = dotenv::var("FRIGATE_ADMIN").expect("FRIGATE_ADMIN must be set in .env");
        let frigate_pass = dotenv::var("FRIGATE_PASS").expect("FRIGATE_PASS must be set in .env");
        let frigate_ip = dotenv::var("FRIGATE_IP").expect("FRIGATE_IP must be set in .env");

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

    #[allow(dead_code)]
    pub async fn get_review(&self) -> Result<Vec<Review>, FrigateError> {
        let url = format!("http://{}:8971/api/review", self.frigate_ip);
        self.get(&url).await
    }

    #[allow(dead_code)]
    async fn get_motion_activity(&self) -> Result<Vec<Motion>, FrigateError> {
        let url = format!("http://{}:8971/api/review/activity/motion", self.frigate_ip);
        self.get(&url).await
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
        path.push(format!("{}.jpg", after_event.get_name()));
        let mut file = File::create(&path)?;
        file.write_all(&res.bytes().await?)?;
        Ok(path)
    }

    pub async fn update_config(&self, config: &serde_json::Value) -> Result<(), FrigateError> {
        let url = format!("http://{}:8971/api/config/set", self.frigate_ip);
        self.put(&url, config).await
    }
}
