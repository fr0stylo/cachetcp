use std::{sync::Arc, time::Duration};

use tokio::{
    io,
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::interval,
};

use crate::{
    persistance::wal::WalWritter,
    proto::{self},
    storage::storage::Storage,
};

pub async fn initiate_client(
    listener: &TcpListener,
    cc: &Arc<Storage>,
    wal: &WalWritter,
) -> Result<(), io::Error> {
    let (mut stream, _) = listener.accept().await?;
    let cc = cc.clone();
    let wal = wal.clone();
    stream.set_nodelay(true).expect("Failed to set no delay");
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        let (tx, mut rx) = unbounded_channel::<proto::FrameMessage>();
        loop {
            let (tcprx, mut tcptx) = stream.split();
            tokio::select! {
              _ = ticker.tick() => {
                let _ = tx.send(proto::CommandMessage::PING().into()).unwrap();
              },
              res = rx.recv() => {
                match res {
                    Some(mut msg) => {
                          let _ = proto::nonblocking::marshal(&mut msg, Box::pin(&mut tcptx)).await;
                      },
                    None => {},
                }
            },
              msg = proto::nonblocking::unmarshal(Box::pin(tcprx))  => {
                  match msg {
                      Ok(msg) => {
                        handle_message(&msg, &cc, tx.clone(), &wal).await.unwrap();
                      },
                      Err(_) => {}
                }
              }
            }
        }
    });

    Ok(())
}

pub async fn handle_message(
    msg: &proto::FrameMessage,
    cc: &Arc<Storage>,
    rw: UnboundedSender<proto::FrameMessage>,
    wal: &WalWritter,
) -> Result<(), io::Error> {
    match msg.clone().command {
        proto::CommandMessage::CONNECTED() => {
            let _ = rw.send(msg.reply_borrow(None));
        }
        proto::CommandMessage::GET(key) => {
            let data = match cc.read(&key).await {
                Some(x) => x.to_owned(),
                None => Vec::<u8>::new(),
            };
            let _ = rw.send(msg.reply_borrow(Some(data)));
        }
        proto::CommandMessage::PUT(key, data, exp) => {
            match exp {
                Some(x) => {
                    cc.write_ex(&key, data, x).await;
                }
                None => {
                    cc.write(&key, data).await;
                }
            }

            wal.write(&msg);

            let _ = rw.send(msg.reply_borrow(None));
        }
        proto::CommandMessage::KEYS() => {
            let keys: Vec<String> = cc.keys().await.map(|x| x.clone()).unwrap();

            let buf = rmp_serde::encode::to_vec(&keys).unwrap();
            let _ = rw.send(msg.reply_borrow(Some(buf)));
        }
        proto::CommandMessage::DELETE(key) => {
            cc.delete(key).await;
            //     wal.write(&msg.clone());

            let _ = rw.send(msg.reply_borrow(None));
        }
        proto::CommandMessage::RECV(_) => todo!(),
        _ => {}
    };

    Ok(())
}
