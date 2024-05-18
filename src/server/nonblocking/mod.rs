use std::time::Duration;

use tokio::{
    net::TcpListener,
    sync::{broadcast::channel, Mutex},
    time::interval,
};

use crate::{
    proto::{self, Messages},
    storage::{storage::Storage, Cache},
    wal::{WalWritter, WriteAheadLog},
};

// type Cache = Arc<Mutex<HashMap<String, Vec<u8>>>>;

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    cache: Storage, // clients: Vec<JoinHandle<()>>,
    wal: WriteAheadLog
}

impl Server {
    pub async fn new(addr: &str, cc: Storage) -> Self {
        let listener = TcpListener::bind(addr).await.expect("failed to bind port");
        let mut wal = WriteAheadLog::new("./test.log").await;

        Server {
            listener: listener,
            cache: cc,
            wal: wal,
        }
    }

    pub async fn start_recv(&mut self) -> std::io::Result<()> {
        loop {
            let waltx = self.wal.tx().clone();
            let walw = WalWritter::new(waltx);
            let (mut stream, _sck) = self.listener.accept().await?;

            let cc = self.cache.clone();

            tokio::spawn(async move {
                let (tx, mut rx) = channel::<proto::Message>(10);
                let (mut srx, mut stx) = stream.split();

                let mut ticker = interval(Duration::from_secs(5));

                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            let _ = tx.send(proto::Message::ping()).unwrap();
                        },
                        res = rx.recv() => {
                            match res {
                                Ok(mut msg) => {
                                    let _ = proto::nonblocking::marshal(&mut msg, Box::pin(&mut stx)).await;
                                },
                                Err(e) => eprintln!("{}", e),
                            }
                        },
                        res = proto::nonblocking::unmarshal(Box::pin(&mut srx)) => {
                            match res {
                                Ok(msg) => {
                                    match msg.command {
                                        proto::Command::CONNECTED => {
                                            let _ = tx.send(proto::Message::recv(None, Some(msg.ts)));
                                        }
                                        proto::Command::GET => {
                                            let key = String::from_utf8(msg.data.unwrap()).unwrap();

                                            let data = match cc.read(key).await {
                                                Some(x) => x.to_owned(),
                                                None => Vec::<u8>::new(),
                                            };
                                            let _ = tx.send(proto::Message::recv(Option::Some(data), Option::Some(msg.ts)));
                                        }
                                        proto::Command::PUT => {
                                            let msgg = msg.clone();
                                            let parts = proto::resolve_pair(msg.data.unwrap());

                                            let key = String::from_utf8(parts.first().unwrap().to_vec()).unwrap();

                                            tokio::join!(
                                                cc.write(key.to_owned(), parts.last().unwrap().to_owned().into()),
                                            );

                                            walw.clone().write(&msgg);

                                            let _ = tx.send(proto::Message::recv(Option::None, Option::Some(msg.ts)));
                                        }
                                        proto::Command::KEYS => {
                                            let keys: Vec<String> = cc
                                                .keys()
                                                .await
                                                .map(|x| x.clone())
                                                .unwrap();

                                            let keys = keys.join("\0");

                                            let _ = tx.send(proto::Message::recv(Option::Some(keys.into_bytes()), Option::Some(msg.ts)));
                                        }
                                        proto::Command::DELETE => {
                                            let key = String::from_utf8(msg.data.unwrap()).unwrap();

                                            cc.delete(key).await;

                                            let _ = tx.send(proto::Message::recv(Option::None, Option::Some(msg.ts)));
                                        }
                                        proto::Command::DEFAULT => {}
                                        _ => {}
                                    };
                                },
                                Err(_) =>{},
                            }
                        }
                    }
                    // let _ = handle_connection(&mut stream, &mut cc).await;
                }
            });
        }
    }
}

// async fn handle_connection(
//     mut stream: &mut TcpStream,
//     cache_entity: &mut Cache,
// ) -> Result<(), Error> {
//     let msg = proto::nonblocking::unmarshal(Box::pin(&mut stream)).await?;
//     match msg.command {
//         proto::Command::PING => {
//             let _ = proto::nonblocking::marshal(&mut proto::Message::pong(), Box::pin(&mut stream))
//                 .await;
//         }
//         proto::Command::PONG => {
//             // tokio::time::sleep(Duration::from_secs(5)).await;
//             let _ = proto::nonblocking::marshal(&mut proto::Message::ping(), Box::pin(&mut stream))
//                 .await;

//             // let mut ss = stream.try_clone().unwrap();
//             // let _ = thread::Builder::new().spawn(move || {
//             //     sleep(Duration::from_secs(5));
//             // }); // sleep(Duration::from_secs(1));
//         }
//         proto::Command::CONNECTED => {
//             let _ = proto::nonblocking::marshal(
//                 &mut proto::Message::recv(None, Some(msg.ts)),
//                 Box::pin(&mut stream),
//             )
//             .await;
//             // tokio::time::sleep(Duration::from_secs(5)).await;
//             // let _ = proto::nonblocking::marshal(&mut proto::Message::ping(), Box::pin(&mut stream))
//             //     .await;
//         }
//         proto::Command::GET => {
//             let key = String::from_utf8(msg.data.unwrap()).unwrap();

//             let data = match cache_entity.lock().await.get(&key) {
//                 Some(x) => x.to_owned(),
//                 None => Vec::<u8>::new(),
//             };

//             proto::nonblocking::marshal(
//                 &mut proto::Message::recv(Option::Some(data), Option::Some(msg.ts)),
//                 Box::pin(&mut stream),
//             )
//             .await?
//         }
//         proto::Command::PUT => {
//             let data = String::from_utf8(msg.data.unwrap()).unwrap();
//             let data = data.split("||").collect::<Vec<&str>>();
//             let key = data.first().unwrap().to_owned();

//             cache_entity
//                 .lock()
//                 .await
//                 .insert(key.to_owned(), data.last().unwrap().to_owned().into());

//             proto::nonblocking::marshal(
//                 &mut proto::Message::recv(Option::None, Option::Some(msg.ts)),
//                 Box::pin(&mut stream),
//             )
//             .await?
//         }
//         proto::Command::KEYS => {
//             let keys: Vec<String> = cache_entity
//                 .lock()
//                 .await
//                 .keys()
//                 .map(|x| x.clone())
//                 .collect();

//             let keys = keys.join("||");

//             proto::nonblocking::marshal(
//                 &mut proto::Message::recv(Option::Some(keys.into_bytes()), Option::Some(msg.ts)),
//                 Box::pin(&mut stream),
//             )
//             .await?
//         }
//         proto::Command::DELETE => {
//             let key = String::from_utf8(msg.data.unwrap()).unwrap();

//             cache_entity.lock().await.remove(&key);

//             proto::nonblocking::marshal(
//                 &mut proto::Message::recv(Option::None, Option::Some(msg.ts)),
//                 Box::pin(&mut stream),
//             )
//             .await?
//         }
//         proto::Command::DEFAULT => {}
//         _ => {}
//     };

//     Ok(())
// }