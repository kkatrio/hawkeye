use chrono::{Datelike, Local, TimeZone, Timelike};
use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;
use std::{fmt, path::PathBuf};

#[derive(Deserialize, Debug)]
struct EventData {
    max_severity: String,
}

//#[derive(Deserialize, Debug)]
//struct ReviewData {
//    objects: Vec<String>,
//    zones: Vec<String>,
//}

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
struct Review {
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

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct Event {
    id: String,
    camera: String,
    label: String,
    zones: Vec<String>,
    start_time: f64,
    end_time: f64,
    data: EventData,
}

impl Event {
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

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -- {}, {}, start: {} end: {}",
            self.id,
            self.data.max_severity,
            self.label,
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
    token: String,
}

impl Client {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();

        dotenv::dotenv().ok();
        let frigate_user = dotenv::var("FRIGATE_ADMIN").expect("FRIGATE_ADMIN must be set in .env");
        let frigate_pass = dotenv::var("FRIGATE_PASS").expect("FRIGATE_PASS must be set in .env");

        // login
        let login_data = serde_json::json!({"user": frigate_user, "password": frigate_pass});
        let res = client
            .post("http://192.168.1.30:8971/api/login")
            .json(&login_data)
            .send()
            .await?;

        // get token
        let mut cookies = res.cookies();
        let token = cookies
            .find(|c| c.name() == "frigate_token")
            .expect("token cookie should be in the login response")
            .value()
            .to_string();

        Ok(Self { client, token })
    }

    #[allow(dead_code)]
    async fn get_review(&self) -> Result<Vec<Review>, Box<dyn std::error::Error>> {
        let res = self
            .client
            .get("http://192.168.1.30:8971/api/review")
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();
        let reviews = serde_json::from_str(&res.text().await?)?;
        Ok(reviews)
    }

    pub async fn get_events(&self) -> Result<Vec<Event>, Box<dyn std::error::Error>> {
        let res = self
            .client
            .get("http://192.168.1.30:8971/api/events?limit=10")
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();

        let events = serde_json::from_str(&res.text().await?)?;
        Ok(events)
    }

    pub async fn get_event_snapshot(
        &self,
        event: &Event,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let res = self
            .client
            .get(format!(
                "http://192.168.1.30:8971/api/events/{}/snapshot.jpg",
                event.id
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();

        let mut path = PathBuf::from(r"./media");
        path.push(format!("{}.jpg", event.get_name()));
        let mut file = File::create(&path)?;
        file.write_all(&res.bytes().await?)?;
        Ok(path)
    }

    #[allow(dead_code)]
    async fn get_motion_activity(&self) -> Result<Vec<Motion>, Box<dyn std::error::Error>> {
        let res = self
            .client
            .get("http://192.168.1.30:8971/api/review/activity/motion")
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();

        let motions = serde_json::from_str(&res.text().await?)?;
        Ok(motions)
    }
}
