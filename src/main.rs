use std::str::FromStr;
use std::{error::Error, net::TcpListener};
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::util::SubscriberInitExt;

mod frigate;
mod gdrive;
mod ipc;

use crate::gdrive::GClientError;
use frigate::FrigateError;
use std::sync::Arc;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::io::Read;
use std::net::TcpStream;
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

#[instrument(skip(client))]
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

#[instrument(skip(gclient))]
async fn delete_old(gclient: Arc<gdrive::GClient>) {
    let mut interval = 6;
    loop {
        info!("deleting old events");
        match gclient.get_uploaded_events_reverse().await {
            Ok(files) => {
                let deleted = gclient.delete_old(&files).await;
                info!("deleted: {deleted} files");
                // list gdrive api fetches 100 events
                if deleted == 100 {
                    warn!("more than 100 files to delete -- changing delete interval to 1 hour.");
                    interval = 1;
                } else if deleted < 100 && interval == 1 {
                    warn!("deleted less than 100 -- changing delete interval back to 6 hours.");
                    interval = 6;
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
        tokio::time::sleep(std::time::Duration::from_hours(interval)).await;
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

enum Cmd {
    Send(
        ipc::NewEvent,
        tokio::sync::oneshot::Sender<std::io::Result<()>>,
    ),
    TestSend(
        ipc::NewEvent,
        tokio::sync::oneshot::Sender<std::io::Result<()>>,
    ),
}

#[instrument(skip(fclient, gclient, tx))]
async fn main_mqtt_subscriber(
    fclient: Arc<frigate::Client>,
    gclient: Arc<gdrive::GClient>,
    tx: tokio::sync::mpsc::Sender<Cmd>,
) {
    let mut mqttoptions = MqttOptions::new("hawkeye", "mosquitto", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    if let Err(e) = client.subscribe("frigate/events", QoS::AtMostOnce).await {
        error!("Failed to subscribe: {}", e);
    }

    loop {
        let event = eventloop.poll().await;

        match event {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                let fe: frigate::Event = match serde_json::from_slice(&p.payload) {
                    Ok(msg) => {
                        info!(msg = ?msg, "msg received");
                        msg
                    }
                    Err(e) => {
                        error!(error = %e, "could not deserialize event received");
                        continue;
                    }
                };
                match fe {
                    frigate::Event::New { after } => {
                        // use a oneshot channel to get back the result here just for fun
                        // TODO: refactor
                        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                        if let Err(e) = tx
                            .send(Cmd::Send(
                                ipc::NewEvent {
                                    camera: after.camera,
                                },
                                reply_tx,
                            ))
                            .await
                        {
                            error!(error = %e, "error sending new event to connection thread")
                        }
                        if let Err(e) = reply_rx.await {
                            error!(error = %e, "no reply received");
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
            // poll will reconnect to the broker on connection drop
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(..))) => {
                info!("MQTT connection (re-)established");
                if let Err(e) = client.subscribe("frigate/events", QoS::AtMostOnce).await {
                    error!("Failed to subscribe: {}", e);
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!("mqtt error: {}", e);
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[instrument(skip(rx))]
async fn connection_handler(mut rx: tokio::sync::mpsc::Receiver<Cmd>) {
    // we panic for now -- connection to olympus necessary
    let mut connection = ipc::Connection::new().unwrap();
    info!("connected to olympus");

    while let Some(msg) = rx.recv().await {
        match msg {
            Cmd::Send(msg, reply) => {
                debug!("connection handler received Send cmd for msg: {:?}", msg);
                let res = connection.send(msg).await;
                // sending the result back to the calling thread
                reply.send(res).unwrap();
            }
            Cmd::TestSend(msg, reply) => {
                debug!(
                    "connection handler received TestSend cmd for msg: {:?}",
                    msg
                );
                let res = connection.send(msg).await;
                // sending the result back to the calling thread
                reply.send(res).unwrap();
            }
        }
    }
}

#[instrument(skip(tx, stream))]
async fn handle_backdoor_connection(
    mut stream: TcpStream,
    tx: tokio::sync::mpsc::Sender<Cmd>,
) -> std::io::Result<()> {
    loop {
        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf)?;
        if n == 0 {
            info!("backdoor connection closed");
            break;
        }
        debug!("backdoor read {n} bytes");
        match std::str::from_utf8(&buf[..n]) {
            Ok(s) => match s.trim() {
                "test" => {
                    info!("read test");
                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                    if let Err(e) = tx
                        .send(Cmd::TestSend(
                            ipc::NewEvent {
                                // TODO: use testing camera once implemented
                                camera: String::from("pisw"),
                            },
                            reply_tx,
                        ))
                        .await
                    {
                        error!(error = %e, "could not send to ipc connection handler");
                    }
                    if let Err(e) = reply_rx.await {
                        error!(error = %e, "no reply received");
                    }
                }
                other => {
                    info!("read {other}");
                }
            },
            Err(e) => {
                error!("invalid utf8: {e}");
            }
        }
    }
    // returning Ok when connection is closed?
    Ok(())
}

#[instrument(skip(tx))]
async fn backdoor_listener(tx: tokio::sync::mpsc::Sender<Cmd>) {
    let listener =
        TcpListener::bind("0.0.0.0:8044").expect("should bind port 8044 for backdoor listener");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tx = tx.clone();
                handle_backdoor_connection(stream, tx)
                    .await
                    .expect("should handle connection");
            }
            Err(e) => {
                error!("connection failed: {}", e);
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let timeformat = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();

    tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_timer(timeformat)
        .finish()
        .init();

    dotenv::dotenv().ok();

    // TODO: make gdrive connection optional -- handle new() errors
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

    let (tx, rx) = tokio::sync::mpsc::channel(10);

    let mh = tokio::spawn(main_mqtt_subscriber(
        client.clone(),
        gclient.clone(),
        tx.clone(),
    ));

    // TODO: reconnect if connection has dropped
    let ipch = tokio::spawn(connection_handler(rx));

    let bdh = tokio::spawn(backdoor_listener(tx));

    let _ = tokio::join!(mh, lh, ih, dh, ipch, bdh);
}
