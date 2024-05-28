use std::{io::Error, pin::Pin, sync::Arc};
use std::time::Duration;

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};

use crate::{proto, storage::storage::Storage};

use super::{read_log_entry};

#[derive(Debug)]
pub struct WriteAheadLog {
    fw: Mutex<Pin<Box<File>>>,
    fr: Pin<Box<File>>,
    tx: UnboundedSender<proto::FrameMessage>,
    rx: UnboundedReceiver<proto::FrameMessage>,
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

        let (tx, rx) = unbounded_channel::<proto::FrameMessage>();

        Self { fw, fr, tx, rx }
    }

    pub async fn write(&mut self) -> Result<(), Error> {
        match self.rx.recv().await {
            Some(msg) => {
                let buf: Vec<u8> = msg.into();

                self.write_to_log(buf).await
            }
            None => Ok(()),
        }
    }

    pub async fn write_to_log(
        &self,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        // let buf = write_key_val_buf(key, data, Some(cmd))?;
        let mut buf: Vec<u8> = data.len().to_be_bytes().to_vec();
        buf.extend(data);
        self.fw.lock().await.write_all(&buf).await
    }

    pub async fn read_log_entry(&self) -> Result<Option<proto::FrameMessage>, Error> {
        read_log_entry(self.fr.try_clone().await?).await
    }

    pub async fn replay(&self, cc: &Arc<Storage>) -> Result<u64, Error> {
        use proto::CommandMessage::*;
        let mut result = 0u64;
        loop {
            match self.read_log_entry().await? {
                Some(x) => {
                    match x.command {
                        PUT(key, data, exp) => {
                            match exp {
                                None => {
                                    cc.write(&key, data).await
                                }
                                Some(x) => {
                                    cc.write_ex(&key, data, exp.unwrap()).await
                                }
                            }
                        }
                        DELETE(key) => cc.delete(&key).await,
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

    pub fn tx(&mut self) -> UnboundedSender<proto::FrameMessage> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone)]
pub struct WalWritter {
    tx: UnboundedSender<proto::FrameMessage>,
}

impl WalWritter {
    pub fn new(tx: UnboundedSender<proto::FrameMessage>) -> Self {
        Self { tx }
    }

    pub fn write(&self, msg: &proto::FrameMessage) {
        let msg = msg.clone();
        let _ = self.tx.send(msg);
    }
}
