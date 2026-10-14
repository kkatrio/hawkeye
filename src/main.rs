use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::util::SubscriberInitExt;

mod backdoor;
mod frigate;
mod gdrive;
mod ipc;

async fn connect_frigate() -> frigate::Client {
    loop {
        match frigate::Client::new().await {
            Ok(client) => {
                info!("Connected to frigate");
                return client;
            }
            Err(e) => {
                error!(error = ?e, "Could not connect to frigate");
            }
        };
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

enum Cmd {
    Send(ipc::NewEvent),
    TestSend(ipc::NewEvent),
}

struct Message {
    cmd: Cmd,
    reply: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

#[derive(Clone)]
struct TaskMessenger {
    tx: tokio::sync::mpsc::Sender<Message>,
}

impl TaskMessenger {
    async fn send(&self, cmd: Cmd) -> std::io::Result<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message {
                cmd,
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "error sending new event to connection thread",
                )
            })?;
        reply_rx.await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "no reply received from connection handler",
            )
        })?
        // returning the io::Result of the connection.send
    }
}

#[instrument(skip(messenger, uploader))]
async fn main_mqtt_subscriber(uploader: Option<DriveUploader>, messenger: TaskMessenger) {
    let mut mqttoptions = MqttOptions::new("hawkeye", "mosquitto", 1883);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    if let Err(e) = client.subscribe("frigate/events", QoS::AtMostOnce).await {
        error!(error = ?e, "Failed to subscribe");
    }

    loop {
        let event = eventloop.poll().await;

        match event {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                let fe: frigate::Event = match serde_json::from_slice(&p.payload) {
                    Ok(msg) => {
                        debug!(msg = ?msg, "msg received");
                        msg
                    }
                    Err(e) => {
                        error!(error = %e, "could not deserialize event received");
                        continue;
                    }
                };
                match fe {
                    frigate::Event::New { after } => {
                        let cmd = Cmd::Send(ipc::NewEvent {
                            camera: after.camera,
                        });
                        if let Err(e) = messenger.send(cmd).await {
                            error!(error = %e, "connection error");
                        }
                    }
                    frigate::Event::Update { .. } => {
                        continue;
                    }
                    frigate::Event::End { after } => {
                        if let Some(uploader) = &uploader {
                            if let Err(e) = uploader.upload(&after).await {
                                error!(error = %e, "error uploading event");
                            }
                        } else {
                            warn!("no gdrive connection -- event not uploaded");
                        }
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
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
}

#[instrument(skip(rx))]
async fn connection_handler(addr: &str, mut rx: tokio::sync::mpsc::Receiver<Message>) {
    // loops until connected.
    let mut connection = ipc::Connection::new(addr);
    info!("connected to olympus");

    while let Some(msg) = rx.recv().await {
        let reply = msg.reply;
        let res = match msg.cmd {
            Cmd::Send(event) => connection.send(event),
            Cmd::TestSend(event) => connection.send(event),
        };
        // sending the result back to the calling thread
        // if the caller has dropped we do not care about the error
        let _ = reply.send(res);
    }
}

struct DriveUploader {
    fclient: Arc<frigate::Client>,
    gclient: Arc<gdrive::GClient>,
}

impl DriveUploader {
    pub async fn upload(&self, event: &frigate::After) -> std::io::Result<()> {
        let path = self
            .fclient
            .get_event_snapshot(event)
            .await
            .map_err(std::io::Error::other)?;
        let event_name = self
            .gclient
            .upload(event, path)
            .await
            .map_err(std::io::Error::other)?;

        info!("uploaded: {}", event_name);
        Ok(())
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

    // connection to frigate is necessary. wait until connected.
    let client = connect_frigate().await;
    let client = Arc::new(client);

    let mut set = tokio::task::JoinSet::new();

    // connect to gdrive
    let uploader = match gdrive::GClient::new().await {
        Ok(gclient) => {
            let gclient = Arc::new(gclient);

            set.spawn(gdrive::delete_old(gclient.clone()));

            let uploader = DriveUploader {
                fclient: client.clone(),
                gclient: gclient.clone(),
            };
            Some(uploader)
        }
        Err(e) => {
            error!("error attempting to connect to gdrive: {}", e);
            None
        }
    };

    // send from mqtt main thread (and from backdoor) to ipc thread
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let messenger = TaskMessenger { tx };

    set.spawn(main_mqtt_subscriber(uploader, messenger.clone()));

    set.spawn(backdoor::listener(messenger.clone()));

    set.spawn(connection_handler("olympus:8080", rx));

    // modify threshold
    set.spawn(frigate::config_updater(
        frigate::EVENING_EXPRESSION,
        frigate::Time::Evening,
        client.clone(),
    ));
    set.spawn(frigate::config_updater(
        frigate::MORNING_EXPRESSION,
        frigate::Time::Morning,
        client.clone(),
    ));
}
