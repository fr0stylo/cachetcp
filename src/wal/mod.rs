use std::{
    io::{Error, ErrorKind},
    pin::Pin,
};

use tokio::{
    fs::{File, OpenOptions},
    sync::broadcast::{channel, Receiver, Sender},
};

use crate::proto;

#[derive(Debug)]

pub struct WriteAheadLog {
    fw: Pin<Box<File>>,
    fr: Pin<Box<File>>,
    tx: Sender<proto::Message>,
    rx: Receiver<proto::Message>,
}

impl WriteAheadLog {
    pub async fn new(path: &str) -> Self {
        let fw = Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await
                .unwrap(),
        );

        let fr = Box::pin(OpenOptions::new().read(true).open(path).await.unwrap());

        let (tx, rx) = channel::<proto::Message>(19);

        Self { fw, fr, tx, rx }
    }

    pub async fn write(&mut self) -> Result<(), Error> {
        match self.rx.recv().await {
            Ok(msg) => self.add_to_log(msg).await,
            Err(_) => Err(Error::new(ErrorKind::UnexpectedEof, "somthing wong")),
        }
    }

    pub async fn add_to_log(&self, log: proto::Message) -> Result<(), Error> {
        let mut log = log.clone();
        proto::nonblocking::marshal(&mut log, Box::pin(self.fw.try_clone().await.unwrap())).await
    }

    pub async fn read_log_entry(&self) -> Result<proto::Message, Error> {
        proto::nonblocking::unmarshal(Box::pin(self.fr.try_clone().await.unwrap())).await
    }

    pub fn tx_mut(&mut self) -> &mut Sender<proto::Message> {
        &mut self.tx
    }
}
