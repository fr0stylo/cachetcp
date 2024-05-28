use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    ops::Add,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedSender},
};

use crate::proto::{self};

type ReceiverQueue = Arc<Mutex<HashMap<u128, UnboundedSender<proto::FrameMessage>>>>;

#[derive(Debug)]
pub struct Client {
    tx: UnboundedSender<proto::FrameMessage>,
    queue: ReceiverQueue,
    msg_idx: Mutex<u128>,
}

impl Client {
    pub async fn new(addr: &str) -> Self {
        let mut sck = TcpStream::connect(addr).await.unwrap();
        sck.set_nodelay(true).unwrap();
        let q = Arc::new(Mutex::new(HashMap::<
            u128,
            UnboundedSender<proto::FrameMessage>,
        >::new()));
        let qq = q.clone();
        let (ctx, mut crx) = unbounded_channel::<proto::FrameMessage>();
        let spawn_sender = ctx.clone();
        tokio::spawn(async move {
            let qq = qq;
            loop {
                let (read, write) = sck.split();
                tokio::select! {
                    msg = crx.recv() => {
                        let _ = match msg {
                            Some(mut msg) => proto::nonblocking::marshal(&mut msg, Box::pin(write)).await,
                            None => Ok(()),
                        };
                    }
                    msg = proto::nonblocking::unmarshal(Box::pin(read)) => {
                        match msg {
                            // proto::Command::PING => {
                            //     let _ = spawn_sender.send(proto::Message::pong());
                            // }
                            // proto::Command::RECV => match qq.lock().unwrap().get(&msg.ts) {
                            //     Some(tx) => {
                            //         let _ = tx.send(msg);
                            //     },
                            //     None => {
                            //         eprint!("chan not found");
                            //     },
                            // }
                            // _ => {}
                            Ok(msg) => match msg.command {
                                proto::CommandMessage::PING() => {
                                let _ = spawn_sender.send(proto::CommandMessage::PONG().into());
                                },
                                proto::CommandMessage::RECV(_) => match qq.lock().unwrap().get(&msg.ts) {
                                        Some(tx) => {
                                            let _ = tx.send(msg);
                                        },
                                        None => {
                                            eprint!("chan not found");
                                        },
                                    },
                                _ => {}
                            },
                            Err(e) if e.kind() == ErrorKind::Unsupported => {}
                            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                                break;
                            }
                            Err(e) if e.kind() == ErrorKind::ConnectionReset => {
                                break;
                            }
                            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                                break;
                            }
                            Err(e) => {
                                eprint!("{:?}", e)
                            }
                        }
                    }
                }
            }
        });

        return Client {
            tx: ctx,
            queue: q,
            msg_idx: Mutex::new(0),
        };
    }

    pub fn ping(&self) -> Result<(), Error> {
        let _ = self.tx.send(proto::CommandMessage::PING().into());
        Ok(())
    }

    async fn rpc(&self, msg: proto::FrameMessage) -> Result<Option<Vec<u8>>, Error> {
        // let t = SystemTime::now();
        let count: u128;
        {
            let mut res = self.msg_idx.lock().unwrap();

            count = *res;
            *res = res.add(1);
        }
        let mut msg = msg.clone();
        msg.ts = count;

        let (tx, mut rx) = unbounded_channel::<proto::FrameMessage>();
        self.queue.lock().unwrap().insert(count, tx);

        let _ = self.tx.send(msg.clone());

        let result = match rx.recv().await {
            Some(result) => match result.command {
                proto::CommandMessage::RECV(data) => data,
                _ => None,
            },
            None => None,
        };

        self.queue.lock().unwrap().remove(&msg.ts);

        return Ok(result);
    }

    pub async fn connected(&self) -> Result<Option<Vec<u8>>, Error> {
        let msg = proto::CommandMessage::CONNECTED().into();

        self.rpc(msg).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let msg = proto::CommandMessage::GET(key.to_owned()).into();

        self.rpc(msg).await
    }

    pub async fn put(
        &self,
        key: &str,
        data: Vec<u8>,
        exp: Option<Duration>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let msg = proto::CommandMessage::PUT(key.to_owned(), data, exp).into();

        self.rpc(msg).await
    }

    pub async fn keys(&self) -> Result<Option<Vec<String>>, Error> {
        let msg = proto::CommandMessage::KEYS().into();

        match self.rpc(msg).await {
            Ok(data) => {
                match data {
                    None => Ok(None),
                    Some(data) => {
                        let pairs: Vec<String> = proto::resolve_pair(data)
                            .into_iter()
                            .map(|x| String::from_utf8(x.to_owned()).unwrap().to_owned())
                            .collect();
                        Ok(Some(pairs))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        let msg = proto::CommandMessage::DELETE(key.to_owned()).into();

        match self.rpc(msg).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
