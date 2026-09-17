use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use tracing::{error, info, warn};

/*
 * new generic event           # 1010
 * new event cam:mpros#          1011
 * new event cam:pisw#           1012
 * new event cam:porta#          1013
 * new event cam:voreia-eisodos# 1014
 * new event cam:deuteri-porta#  1015
 * new event cam:peuko-garage#   1016
 * new event cam:anatolika#      1017
 * new event cam:varka#          1018
 */
pub struct NewEvent {
    pub camera: String,
}

impl NewEvent {
    fn bytes(self) -> [u8; 4] {
        match &*self.camera {
            "mpros" => [1, 0, 1, 1],
            "pisw" => [1, 0, 1, 2],
            "porta" => [1, 0, 1, 3],
            "voreia-eisodos" => [1, 0, 1, 4],
            "deuteri-porta" => [1, 0, 1, 5],
            "peuko-garage" => [1, 0, 1, 6],
            "anatolika" => [1, 0, 1, 7],
            "varka" => [1, 0, 1, 8],
            _ => {
                panic!("wrong camera");
            }
        }
    }
}

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new() -> std::io::Result<Connection> {
        // note: use 0.0.0.0 from the host
        let mut addrs_iter = "olympus:8080".to_socket_addrs()?;
        let addr = addrs_iter
            .next()
            .expect("addr must be created out of the domain");
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        Ok(Connection { stream })
    }

    // printf "\x01\x00\x00\x05" | nc 127.0.0.1 8080
    pub async fn send(mut self, msg: NewEvent) -> std::io::Result<()> {
        self.stream.write_all(&msg.bytes());
        let mut buf = [0u8; 2];
        let _ = self.stream.read_exact(&mut buf)?;
        if buf != "ok".as_bytes() {
            // not sure this is ever possible
            error!("received response from olympus: {:?}", buf);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response is not ok",
            ));
        }
        Ok(())
    }
}
