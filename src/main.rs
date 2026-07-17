use std::error::Error;
use std::str::FromStr;
use tracing::{error, info, warn};
use tracing_subscriber::util::SubscriberInitExt;

mod frigate;
mod gdrive;
mod ipc;

use crate::gdrive::GClientError;
use frigate::FrigateError;
use std::sync::Arc;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;

// lower threshold at 20:00
// increase threshold at 07:00
// TODO: hardcode sunset / sunrise times
//sec  min   hour   day of month   month   day of week   year
const RETENTION: i64 = 10 * 24; // hours
const EVENING_EXPRESSION: &str = "0 0 21 * * * *";
const MORNING_EXPRESSION: &str = "0 0 07 * * * *";

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
enum Time {
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

async fn config_updater(expression: &str, time: Time, client: Arc<frigate::Client>) {
    let schedule = cron::Schedule::from_str(expression).expect("cron expression not valid");
    for next_datetime in schedule.upcoming(chrono::Local) {
        info!(
            "Upcoming fire time to set the threshold for: {:?} -> {}",
            &time, next_datetime
        );
        let seconds_until_next = (next_datetime - chrono::Local::now()).to_std().unwrap();
        tokio::time::sleep(seconds_until_next).await;

        for patch in create_patches(&time) {
            match client.update_config(&patch).await {
                Ok(_) => {
                    info!("successfully updated configuration");
                }
                Err(e) => {
                    error!("configuration not valid -- update failed: {e}")
                }
            }
        }
    }
}

async fn delete_old(gclient: Arc<gdrive::GClient>) {
    loop {
        info!("deleting old events");
        match gclient.get_uploaded_events_reverse().await {
            Ok(files) => {
                let deleted = gclient.delete_old(&files).await;
                info!("deleted: {deleted} files");
                // list gdrive api fetches 100 events
                if deleted == 100 {
                    warn!("must delete more frequently");
                }
            }
            Err(GClientError::ReqwestError) => {
                error!("request error");
                panic!();
            }
            Err(GClientError::SerdeError(e)) => {
                error!("serde error: {e}");
                panic!();
            }
        };
        tokio::time::sleep(std::time::Duration::from_hours(6)).await;
    }
}

async fn connect_frigate() -> frigate::Client {
    loop {
        match frigate::Client::new().await {
            Ok(client) => {
                info!("Connected to frigate");
                return client;
            }
            Err(FrigateError::LoginFailed(e)) => {
                error!("Could not reach frigate: {e}, {:?}", e.source());
                // keep restarting until able to login.
            }
            Err(FrigateError::TokenMissing) => {
                error!("Frigate did not issue token");
            }
            Err(_) => {
                unreachable!()
            }
        };
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn main_mqtt_subscriber(fclient: Arc<frigate::Client>, gclient: Arc<gdrive::GClient>) {
    let mut mqttoptions = MqttOptions::new("hawkeye", "mosquitto", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client
        .subscribe("frigate/events", QoS::AtMostOnce)
        .await
        .unwrap();

    loop {
        let event = eventloop.poll().await;

        match event {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                let fe: frigate::Event = match serde_json::from_slice(&p.payload) {
                    Ok(msg) => {
                        info!("msg received: {:?}", msg);
                        msg
                    }
                    Err(e) => {
                        error!("could not deserialize event received: {}", e);
                        continue;
                    }
                };
                match fe {
                    frigate::Event::New { after } => {
                        let res = tokio::spawn(ipc::send_alarm_notification(ipc::NewEvent {
                            camera: after.camera,
                        }));

                        if let Err(e) = res.await {
                            error!(
                                "Alarm notification not sent -- Did not connect to olympus: {}",
                                e
                            )
                        }
                    }
                    frigate::Event::Update { .. } => {
                        continue;
                    }
                    frigate::Event::End { after } => {
                        match fclient.get_event_snapshot(&after).await {
                            Ok(path) => {
                                let event_name = after.get_name();
                                match gclient.upload(&event_name, path).await {
                                    Ok(_) => {
                                        info!("uploaded: {}", event_name);
                                    }
                                    Err(e) => {
                                        error!("Error: Did not upload event to gdrive: {}", e);
                                        continue;
                                    }
                                };
                            }
                            Err(e) => {
                                error!("Did not get event snapshot: {}", e);
                            }
                        };
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!("mqtt error: {}", e)
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .finish()
        .init();

    dotenv::dotenv().ok();

    let gclient = match gdrive::GClient::new().await {
        Ok(gclient) => gclient,
        Err(GClientError::ReqwestError) => {
            error!("request error");
            panic!("no connection to gdrive");
        }
        Err(GClientError::SerdeError(e)) => {
            error!("serde error: {e}");
            panic!();
        }
    };
    let gclient = Arc::new(gclient);

    let dh = tokio::spawn(delete_old(gclient.clone()));

    let client = connect_frigate().await;
    let client = Arc::new(client);

    let lh = tokio::spawn(config_updater(
        EVENING_EXPRESSION,
        Time::Evening,
        client.clone(),
    ));

    let ih = tokio::spawn(config_updater(
        MORNING_EXPRESSION,
        Time::Morning,
        client.clone(),
    ));

    let mh = tokio::spawn(main_mqtt_subscriber(client.clone(), gclient.clone()));
    let _ = tokio::join!(mh, lh, ih, dh);
}
