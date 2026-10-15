use crate::{ipc, Cmd, TaskMessenger};
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument};

// returns when read 0 bytes (connection closes)
#[instrument(skip(messenger, stream))]
async fn handle_backdoor_connection(
    mut stream: TcpStream,
    messenger: TaskMessenger,
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
                    // TODO: use testing camera once implemented
                    let cmd = Cmd::TestSend(ipc::NewEvent {
                        camera: String::from("pisw"),
                    });
                    if let Err(e) = messenger.send(cmd).await {
                        error!(error = %e, "connection error");
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
    // connection closed
    Ok(())
}

// nc 127.0.0.1 8044
#[instrument(skip(msgr))]
pub async fn listener(msgr: TaskMessenger) {
    let listener =
        TcpListener::bind("0.0.0.0:8044").expect("should bind port 8044 for backdoor listener");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let msgr = msgr.clone();
                if let Err(e) = handle_backdoor_connection(stream, msgr).await {
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
