use rand::prelude::*;
use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    ops::Add,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{unbounded_channel, UnboundedSender},
};

use crate::proto::{self, Messages};

type ReceverQueue = Arc<Mutex<HashMap<u128, UnboundedSender<proto::Message>>>>;

#[derive(Debug)]
pub struct Client {
    tx: UnboundedSender<proto::Message>,
    queue: ReceverQueue,
    msg_idx: Mutex<u128>,
}

impl Client {
    pub async fn new(addr: &str) -> Self {
        let mut sck = TcpStream::connect(addr).await.unwrap();
        sck.set_nodelay(true).unwrap();
        let q = Arc::new(Mutex::new(
            HashMap::<u128, UnboundedSender<proto::Message>>::new(),
        ));
        let qq = q.clone();
        let (ctx, mut crx) = unbounded_channel::<proto::Message>();
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
                            Ok(msg) => match msg.command {
                                proto::Command::PING => {
                                    println!("{:?}", msg);
                                    let _ = spawn_sender.send(proto::Message::pong());
                                }
                                proto::Command::RECV => match qq.lock().unwrap().get(&msg.ts) {
                                    Some(tx) => {
                                        let _ = tx.send(msg);
                                    },
                                    None => {
                                        println!("chan not found");
                                    },
                                }
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
                                println!("{:?}", e)
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
        let _ = self.tx.send(proto::Message::ping());
        Ok(())
    }

    async fn rpc(&self, msg: proto::Message) -> Result<Vec<u8>, Error> {
        // let t = SystemTime::now();
        let count: u128;
        {
            let mut res = self.msg_idx.lock().unwrap();

            count = *res;
            *res = res.add(1);
        }
        let mut msg = msg.clone();
        msg.ts = count;

        let (tx, mut rx) = unbounded_channel::<proto::Message>();
        self.queue.lock().unwrap().insert(count, tx);

        let _ = self.tx.send(msg.clone());

        let result = match rx.recv().await {
            Some(result) => Ok(result.data.or(Some(Vec::new().to_owned())).unwrap()),
            None => Ok(vec![0u8; 0]),
        };

        self.queue.lock().unwrap().remove(&msg.ts);

        return result;
    }

    pub async fn connected(&self) -> Result<Vec<u8>, Error> {
        let msg = proto::Message::connected();

        self.rpc(msg).await
    }

    pub async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let msg = proto::Message::get(key, None);

        self.rpc(msg).await
    }

    pub async fn put(&self, key: &str, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut data = data.clone();

        let msg = proto::Message::put(key, &mut data, None);

        self.rpc(msg).await
    }

    pub async fn keys(&self) -> Result<Vec<String>, Error> {
        let msg = proto::Message::keys(None);

        match self.rpc(msg).await {
            Ok(data) => {
                let pairs: Vec<String> = proto::resolve_pair(data)
                    .into_iter()
                    .map(|x| String::from_utf8(x.to_owned()).unwrap().to_owned())
                    .collect();

                Ok(pairs)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        let msg = proto::Message::delete(key, None);

        match self.rpc(msg).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
