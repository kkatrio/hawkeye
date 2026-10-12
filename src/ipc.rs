use std::io;
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use tracing::{debug, error, info};

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

#[derive(Debug)]
pub struct NewEvent {
    pub camera: String,
}

impl NewEvent {
    fn bytes(self) -> [u8; 4] {
        let (_is_replay, cam_name) = match self.camera.strip_prefix("_replay_") {
            Some(rest) => (true, rest),
            None => (false, self.camera.as_str()),
        };
        match cam_name {
            "mpros" => [1, 0, 1, 1],
            "pisw" => [1, 0, 1, 2],
            "porta" => [1, 0, 1, 3],
            "voreia-eisodos" => [1, 0, 1, 4],
            "deuteri-porta" => [1, 0, 1, 5],
            "peuko-garage" => [1, 0, 1, 6],
            "anatolika" => [1, 0, 1, 7],
            "varka" => [1, 0, 1, 8],
            _ => {
                // TODO: remove this, create a testing variant
                panic!("wrong camera");
            }
        }
        // TODO
        //if is_replay {
        //
        //}
    }
}

pub struct Connection {
    stream: Option<TcpStream>,
    addr: SocketAddr,
}

impl Connection {
    pub fn new(addr: &str) -> Connection {
        let mut attempt = 0;
        let addr = loop {
            match addr.to_socket_addrs() {
                Ok(mut addr_it) => match addr_it.next() {
                    Some(a) => break a,
                    None => {
                        error!("empty socket addr iterator");
                    }
                },
                Err(e) => {
                    error!(error = ?e, "could not resolve olympus");
                    std::thread::sleep(Duration::from_millis(100 * 2u64.pow(attempt.min(6))));
                }
            };
            attempt += 1;
        };

        let mut attempt = 0;
        loop {
            if attempt != 0 {
                std::thread::sleep(Duration::from_millis(100 * 2u64.pow(attempt.min(6))));
            }
            info!("ipc connection attempt");

            match Self::connect(addr) {
                Ok(stream) => {
                    return Connection {
                        stream: Some(stream),
                        addr,
                    }
                }
                Err(e) => {
                    error!(error = ?e, "could not connect to olympus");
                }
            }
            attempt += 1;
        }
    }

    fn connect(addr: SocketAddr) -> std::io::Result<TcpStream> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    fn sendrcv(mut stream: &TcpStream, payload: &[u8]) -> std::io::Result<()> {
        stream.write_all(payload)?;
        debug!("connection written payload");
        let mut buf = [0u8; 2];
        stream.read_exact(&mut buf)?;
        debug!("connection read payload");
        // needed if we will use read
        //if n == 0 {
        //    info!("read 0 bytes, connection down");
        //}
        if buf != "ok".as_bytes() {
            // this is possible if using the backdoor
            error!("received response from olympus: {:?}", buf);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response is not ok",
            ));
        }
        Ok(())
    }

    // printf "\x01\x00\x00\x05" | nc 127.0.0.1 8080
    pub fn send(&mut self, msg: NewEvent) -> std::io::Result<()> {
        let mut attempt = 0;
        let payload = msg.bytes();
        // TODO: add max attempts
        loop {
            if attempt != 0 {
                std::thread::sleep(Duration::from_millis(100 * 2u64.pow(attempt.min(6))));
            }

            if let Some(stream) = &self.stream {
                match Self::sendrcv(stream, &payload) {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::InvalidData => return Err(e),
                    Err(e) => {
                        error!(error = ?e, "disconnected");
                        self.stream = None
                    }
                }
            } else {
                match Self::connect(self.addr) {
                    Ok(stream) => {
                        info!("new connection to olympus established");
                        self.stream = Some(stream)
                    }
                    Err(e) => {
                        error!(error = ?e, "connect attempt to olympus failed");
                    }
                }
            }
            attempt += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ipc;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn test_reconnection() {
        // spawn a listener -- the remote end
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server1 = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).unwrap();
            stream.write_all("ok".as_bytes()).unwrap();
        });

        // connect to the remote end
        let mut connection = ipc::Connection::new(addr).unwrap();
        // send a test msg
        connection
            .send(ipc::NewEvent {
                camera: String::from("pisw"),
            })
            .unwrap();

        // listener and connection have dropped
        server1.join().unwrap();

        // re-spawn the listener
        let listener = TcpListener::bind(addr).unwrap();
        let server2 = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).unwrap();
            stream.write_all("ok".as_bytes()).unwrap();
        });

        // send again a msg with the same connection verifying that the connection was
        // re-established
        connection
            .send(ipc::NewEvent {
                camera: String::from("pisw"),
            })
            .unwrap();
        // server2 finished after having read exactly 4 bytes
        server2.join().unwrap();
    }
}
