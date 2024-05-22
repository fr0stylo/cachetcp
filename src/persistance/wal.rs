use std::{io::Error, pin::Pin, sync::Arc};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};

use crate::{proto, storage::storage::Storage};

use super::{read_log_entry, write_key_val_buf};

#[derive(Debug)]

pub struct WriteAheadLog {
    fw: Mutex<Pin<Box<File>>>,
    fr: Pin<Box<File>>,
    tx: UnboundedSender<proto::Message>,
    rx: UnboundedReceiver<proto::Message>,
}

impl WriteAheadLog {
    pub async fn new(path: &str) -> Self {
        let fw = Mutex::new(Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await
                .unwrap(),
        ));

        let fr = Box::pin(OpenOptions::new().read(true).open(path).await.unwrap());

        let (tx, rx) = unbounded_channel::<proto::Message>();

        Self { fw, fr, tx, rx }
    }

    pub async fn write(&mut self) -> Result<(), Error> {
        match self.rx.recv().await {
            Some(msg) => {
                let parts = proto::resolve_pair(msg.data.unwrap());

                let key = String::from_utf8(parts.first().unwrap().to_vec()).unwrap();

                self.write_to_log(&key, parts.last().unwrap().to_owned().into(), msg.command)
                    .await
            }
            None => Ok(()),
        }
    }

    pub async fn write_to_log(
        &self,
        key: &str,
        data: Vec<u8>,
        cmd: proto::Command,
    ) -> Result<(), Error> {
        let buf = write_key_val_buf(key, data, Some(cmd))?;

        self.fw.lock().await.write_all(&buf).await
    }

    pub async fn read_log_entry(&self) -> Result<Option<proto::Message>, Error> {
        read_log_entry(self.fr.try_clone().await?).await
        // let mut reader = ;
        // let pos = reader.stream_position().await?;
        // let len = reader.metadata().await.unwrap().len();
        // if pos == len {
        //     return Ok(None);
        // }
        // let key_len = reader.read_u64().await?;
        // let data_len = reader.read_u64().await?;
        // let mut cmd = vec![0u8; 1];
        // reader.read_exact(&mut cmd).await?;

        // let mut key = vec![0u8; key_len.try_into().unwrap()];
        // reader.read_exact(&mut key).await?;

        // let mut data = vec![0u8; data_len.try_into().unwrap()];
        // reader.read_exact(&mut data).await?;

        // let key = String::from_utf8(key).unwrap();

        // match proto::Command::from(cmd[0].into()) {
        //     proto::Command::PUT => Ok(Some(proto::Message::put(&key, &mut data, None))),
        //     proto::Command::DELETE => Ok(Some(proto::Message::delete(&key, None))),
        //     x => Err(Error::new(
        //         ErrorKind::Unsupported,
        //         format!("{:?} unsupported command type for WAL", x),
        //     )),
        // }
    }

    pub async fn replay(&self, cc: &Arc<Storage>) -> Result<u64, Error> {
        let mut result = 0u64;
        loop {
            match self.read_log_entry().await? {
                Some(x) => {
                    result = result + 1;
                    let (key, data) = proto::split_parts(x.data.clone().unwrap());
                    match x.command {
                        proto::Command::PUT => cc.write(&key.unwrap(), data.unwrap()).await,
                        proto::Command::DELETE => cc.delete(key.unwrap()).await,
                        _ => None,
                    };
                }
                None => {
                    break;
                }
            };
        }

        Ok(result)
    }

    pub async fn clear(&mut self) -> Result<(), Error> {
        self.fw.lock().await.set_len(0).await?;
        self.fw.lock().await.rewind().await?;

        let _ = self.fr.rewind().await?;

        Ok(())
    }

    pub fn tx(&mut self) -> UnboundedSender<proto::Message> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone)]
pub struct WalWritter {
    tx: UnboundedSender<proto::Message>,
}

impl WalWritter {
    pub fn new(tx: UnboundedSender<proto::Message>) -> Self {
        Self { tx }
    }

    pub fn write(&self, msg: &proto::Message) {
        let msg = msg.clone();
        let _ = self.tx.send(msg);
    }
}
