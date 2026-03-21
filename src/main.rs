use std::error::Error;
use tracing::{error, info};

mod frigate;
mod gdrive;
mod ipc;

use frigate::FrigateError;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .finish()
        .init();

    let client = match frigate::Client::new().await {
        Ok(client) => client,
        Err(FrigateError::LoginFailed(e)) => {
            error!("Could not reach frigate: {e}, {:?}", e.source());
            std::process::exit(1);
        }
        Err(FrigateError::TokenMissing) => {
            error!("Frigate did not issue token");
            std::process::exit(1);
        }
        Err(_) => {
            unreachable!()
        }
    };

    let gclient = gdrive::GClient::new();

    // (folder_id, event_names)
    // uploaded events are identified with a unique event_name (camera-hour:min:sec-day:month)
    let mut uploaded: (String, Vec<String>) = gclient.get_uploaded_events().await?;
    // there is no upload sync, so if they are deleted form gdrive they won't be uploaded again
    let folder_id = uploaded.0;

    loop {
        match client.get_events().await {
            Ok(events) => {
                for event in events {
                    info!("{event}");

                    let event_name = event.get_name();
                    let camera = event.get_camera();

                    if !uploaded.1.contains(&event_name) {
                        // send alarm notification
                        if let Some(e) =
                            ipc::send_alarm_notification(ipc::NewEvent { camera }).err()
                        {
                            error!(
                                "Alarm notification not sent -- Did not connect to olympus: {}",
                                e
                            );
                        };
                        // upload screenshot
                        match client.get_event_snapshot(&event).await {
                            Ok(path) => {
                                match gclient.upload(&event_name, path, &folder_id).await {
                                    Ok(_) => {
                                        info!("uploaded: {}", event_name);
                                        uploaded.1.push(event_name);
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
            Err(e) => {
                error!("frigate error: {}", e);
            }
        };
        info!("uploaded events: {}", uploaded.1.len());
        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    // TODO: delete old events
}
