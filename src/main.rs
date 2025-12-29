mod frigate;
mod gdrive;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let events: Vec<Event> = Vec::new();

    let client = frigate::Client::new().await.unwrap();
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
                    println!("{event}");

                    let event_name = event.get_name();

                    if !uploaded.1.contains(&event_name) {
                        match client.get_event_snapshot(&event).await {
                            Ok(path) => {
                                match gclient.upload(&event_name, path, &folder_id).await {
                                    Ok(_) => {
                                        println!("uploaded: {}", event_name);
                                        uploaded.1.push(event_name);
                                    }
                                    Err(e) => {
                                        eprintln!("Error: Did not upload event to gdrive: {}", e);
                                        continue;
                                    }
                                };
                            }
                            Err(e) => {
                                eprintln!("Did not get event snapshot: {}", e);
                            }
                        };
                    }
                }
            }
            Err(e) => {
                eprintln!("Did not get events: {}", e);
                continue;
            }
        };

        println!("uploaded events: {}", uploaded.1.len());
        std::thread::sleep(std::time::Duration::from_secs(10));
    }

    // TODO: delete uploaded events
}
