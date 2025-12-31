use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/*
 * new event # 1010
 */
pub enum Message {
    NewEvent,
}

impl Message {
    fn bytes(self) -> [u8; 4] {
        match self {
            Self::NewEvent => [1, 0, 1, 0],
        }
    }
}

// printf "\x01\x00\x00\x05" | nc 127.0.0.1 8080
pub fn send_alarm_notification(message: Message) -> std::io::Result<()> {
    let mut addrs_iter = "olympus:8080".to_socket_addrs().unwrap();
    let addr = addrs_iter
        .next()
        .expect("addr must be created out of the domain");
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    println!("connected to olympus");

    stream.write_all(&message.bytes())?;
    let mut buf = [0u8; 2];
    let _ = stream.read(&mut buf)?;
    if buf != "ok".as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response is not ok",
        ));
    }
    Ok(())
}
