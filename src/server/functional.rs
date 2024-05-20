use std::time::Duration;

use tokio::{
    io,
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::interval,
};

use crate::{
    persistance::wal::WalWritter,
    proto::{self, Messages},
    storage::storage::Storage,
};

pub async fn initiate_client(
    listener: &TcpListener,
    cc: Storage,
    wal: WalWritter,
) -> Result<(), io::Error> {
    let (mut stream, _) = listener.accept().await?;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        let (tx, mut rx) = unbounded_channel::<proto::Message>();
        loop {
            let (tcprx, mut tcptx) = stream.split();
            tokio::select! {
              _ = ticker.tick() => {
                let _ = tx.send(proto::Message::ping()).unwrap();
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
                        handle_message(msg, cc.clone(), tx.clone(), &wal).await.unwrap();
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
    msg: proto::Message,
    cc: Storage,
    rw: UnboundedSender<proto::Message>,
    wal: &WalWritter,
) -> Result<(), io::Error> {
    match msg.command {
        proto::Command::CONNECTED => {
            let _ = rw.send(proto::Message::recv(None, Some(msg.ts)));
        }
        proto::Command::GET => {
            let key = String::from_utf8(msg.data.unwrap()).unwrap();

            let data = match cc.read(key).await {
                Some(x) => x.to_owned(),
                None => Vec::<u8>::new(),
            };
            let _ = rw.send(proto::Message::recv(
                Option::Some(data),
                Option::Some(msg.ts),
            ));
        }
        proto::Command::PUT => {
            let msgg = msg.clone();
            let parts = proto::resolve_pair(msg.data.unwrap());

            let key = String::from_utf8(parts.first().unwrap().to_vec()).unwrap();

            tokio::join!(cc.write(key.to_owned(), parts.last().unwrap().to_owned().into()),);

            wal.write(&msgg);

            let _ = rw.send(proto::Message::recv(Option::None, Option::Some(msg.ts)));
        }
        proto::Command::KEYS => {
            let keys: Vec<String> = cc.keys().await.map(|x| x.clone()).unwrap();

            let keys = keys.join("\0");

            let _ = rw.send(proto::Message::recv(
                Option::Some(keys.into_bytes()),
                Option::Some(msg.ts),
            ));
        }
        proto::Command::DELETE => {
            let key = String::from_utf8(msg.clone().data.unwrap()).unwrap();

            cc.delete(key).await;
            wal.write(&msg.clone());

            let _ = rw.send(proto::Message::recv(Option::None, Option::Some(msg.ts)));
        }
        proto::Command::DEFAULT => {}
        _ => {}
    };

    Ok(())
}
