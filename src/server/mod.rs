use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread::{self, sleep},
    time::Duration,
};

use crate::proto::{self, Messages};

type Cache = Arc<Mutex<HashMap<String, Vec<u8>>>>;

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    cache: Cache, // clients: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn new(addr: &str) -> Self {
        let listener = TcpListener::bind(addr).expect("Failed to open port 7070");

        Server {
            listener: listener,
            cache: Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new())),
        }
    }

    pub fn start_recv(&self) -> std::io::Result<()> {
        for stream in self.listener.incoming() {
            let mut cache = self.cache.clone();
            match stream {
                Ok(mut stream) => {
                    let _ = thread::Builder::new()
                        .spawn(move || match handle_connection(&mut stream, &mut cache) {
                            Err(e) => {
                                println!("{:?}", e)
                            }
                            Ok(_) => {}
                        })
                        .expect("Failed to spawn thread");
                }
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
        }

        Ok(())
    }
}

fn handle_connection(mut stream: &mut TcpStream, cache_entity: &mut Cache) -> Result<(), Error> {
    let mut ws = stream.try_clone().unwrap();
    loop {
        match proto::unmarshal(&mut ws) {
            Ok(msg) => {
                println!("{:?}", msg);

                match msg.command {
                    proto::Command::PING => {
                        let mut ss = stream.try_clone().unwrap();
                        let _ = thread::spawn(move || {
                            sleep(Duration::from_secs(5));
                            proto::marshal(&mut proto::Message::pong(), &mut ss).unwrap();
                        });
                        // sleep(Duration::from_secs(1));
                    }
                    proto::Command::PONG => {
                        let mut ss = stream.try_clone().unwrap();
                        let _ = thread::spawn(move || {
                            sleep(Duration::from_secs(5));
                            proto::marshal(&mut proto::Message::pong(), &mut ss).unwrap();
                        }); // sleep(Duration::from_secs(1));
                    }
                    proto::Command::CONNECTED => {
                        proto::marshal(&mut proto::Message::ping(), &mut stream)?
                    }
                    proto::Command::GET => {
                        let key = String::from_utf8(msg.data).unwrap();

                        let data = match cache_entity.lock().unwrap().get(&key) {
                            Some(x) => x.to_owned(),
                            None => Vec::<u8>::new(),
                        };

                        proto::marshal(
                            &mut proto::Message::recv(Option::Some(data), Option::Some(msg.ts)),
                            &mut stream,
                        )?
                    }
                    proto::Command::PUT => {
                        let data = String::from_utf8(msg.data).unwrap();
                        let data = data.split("||").collect::<Vec<&str>>();
                        let key = data.first().unwrap().clone();

                        cache_entity
                            .lock()
                            .unwrap()
                            .insert(key.to_owned(), data.last().unwrap().clone().into());

                        proto::marshal(
                            &mut proto::Message::recv(Option::None, Option::Some(msg.ts)),
                            &mut stream,
                        )?
                    }
                    proto::Command::DEFAULT => {}
                    _ => {}
                }
            }
            Err(e) if e.kind() == ErrorKind::Unsupported => {}
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                break Ok(());
            }
            Err(e) if e.kind() == ErrorKind::ConnectionReset => {
                break Ok(());
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                break Ok(());
            }
            Err(e) => {
                println!("{:?}", e)
            }
        }
    }
}
