use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::io::Read;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::util::SubscriberInitExt;

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
    gclient: Option<Arc<gdrive::GClient>>,
    tx: tokio::sync::mpsc::Sender<Cmd>,
) {
    let mut mqttoptions = MqttOptions::new("hawkeye", "mosquitto", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

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
                        match reply_rx.await {
                            Ok(reply) => match reply {
                                Ok(_) => {
                                    info!("no error -- msg sent ok")
                                }
                                Err(e) => {
                                    error!(error = %e, "connection resulted in error");
                                }
                            },
                            Err(e) => {
                                error!(error = %e, "no reply received from connection handler");
                            }
                        }
                    }
                    frigate::Event::Update { .. } => {
                        continue;
                    }
                    frigate::Event::End { after } => {
                        match fclient.get_event_snapshot(&after).await {
                            Ok(path) => {
                                if let Some(gclient) = &gclient {
                                    match gclient.upload(&after, path).await {
                                        Ok(event_name) => {
                                            info!("uploaded: {}", event_name);
                                        }
                                        Err(e) => {
                                            error!("Error: Did not upload event to gdrive: {}", e);
                                            continue;
                                        }
                                    };
                                } else {
                                    warn!("no gdrive connection");
                                }
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
async fn connection_handler(addr: &str, mut rx: tokio::sync::mpsc::Receiver<Cmd>) {
    // loops until connected.
    // if not connected, the received won't be able to recv
    let mut connection = ipc::Connection::new(addr);
    info!("connected to olympus");

    while let Some(msg) = rx.recv().await {
        match msg {
            Cmd::Send(msg, reply) => {
                debug!("connection handler received Send cmd for msg: {:?}", msg);
                let res = connection.send(msg);
                // sending the result back to the calling thread
                // if the caller has dropped we do not care about the error
                let _ = reply.send(res);
            }
            Cmd::TestSend(msg, reply) => {
                debug!(
                    "connection handler received TestSend cmd for msg: {:?}",
                    msg
                );
                let res = connection.send(msg);
                let _ = reply.send(res);
            }
        }
    }
}

// returns when read 0 bytes (connection closes)
#[instrument(skip(tx, stream))]
async fn handle_backdoor_connection(
    mut stream: TcpStream,
    tx: tokio::sync::mpsc::Sender<Cmd>,
) -> std::io::Result<()> {
    loop {
        let mut buf = [0u8; 128];
        info!("backdoor connection reading");
        let n = stream.read(&mut buf)?;
        if n == 0 {
            info!("backdoor connection closed");
            break;
        }
        debug!("backdoor read {n} bytes");
        match std::str::from_utf8(&buf[..n]) {
            Ok(s) => match s.trim() {
                "test" => {
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
                    // get the result from the connection send
                    match reply_rx.await {
                        Ok(reply) => match reply {
                            Ok(_) => {
                                info!("no error -- msg was sent ok")
                            }
                            Err(e) => {
                                error!(error = ?e, "connection resulted in error");
                            }
                        },
                        Err(e) => {
                            error!(error = %e, "no reply received from connection handler");
                        }
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
    // returning Ok when connection is closed
    Ok(())
}

// nc 127.0.0.1 8044
#[instrument(skip(tx))]
async fn backdoor_listener(tx: tokio::sync::mpsc::Sender<Cmd>) {
    let listener =
        TcpListener::bind("0.0.0.0:8044").expect("should bind port 8044 for backdoor listener");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tx = tx.clone();
                if let Err(e) = handle_backdoor_connection(stream, tx).await {
                    error!(error = ?e, "read error");
                }
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

    // connection to frigate is necessary. Loop until connected.
    let client = connect_frigate().await;
    let client = Arc::new(client);
    // send from mqtt main thread (and from backdoor) to ipc thread
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    let mut set = tokio::task::JoinSet::new();

    // connect to gdrive
    let gclient = match gdrive::GClient::new().await {
        Ok(gclient) => {
            let gclient = Arc::new(gclient);
            set.spawn(gdrive::delete_old(gclient.clone()));
            Some(gclient)
        }
        Err(e) => {
            error!("error attempting to connect to gdrive: {}", e);
            None
        }
    };

    // subscribe to frigate events. gclient is optional
    set.spawn(main_mqtt_subscriber(
        client.clone(),
        gclient.clone(),
        tx.clone(),
    ));

    // backdoor
    set.spawn(backdoor_listener(tx));

    // ipc
    set.spawn(connection_handler("olympus:8080", rx));

    // config update
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
