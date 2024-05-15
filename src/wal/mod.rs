use std::{
    io::{Error, ErrorKind},
    mem::size_of,
    pin::Pin,
    sync::Arc,
};

use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
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

trait WAL {
    fn push(msg: proto::Message) -> Result<(), Error>;
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

        // let mut ss = Arc::new(s);
        // tokio::spawn(async move {
        //     loop {
        //         ss.write();
        //     }
        // });
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

    pub async fn write_to_log(&self, key: &str, data: Vec<u8>) -> Result<(), Error> {
        let key_length = key.len();
        let data_length = data.len();

        let len = size_of::<usize>();
        let full_length = len * 2 + key_length + data_length;

        let mut buf = Vec::<u8>::new();
        buf.extend(&full_length.to_be_bytes());
        buf.extend(&data_length.to_be_bytes());
        buf.extend(key.as_bytes());
        buf.extend(data);

        self.fw.try_clone().await.unwrap().write_all(&buf).await
    }

    pub async fn read_log_entry(&self) -> Result<proto::Message, Error> {
        proto::nonblocking::unmarshal(Box::pin(self.fr.try_clone().await.unwrap())).await
    }

    pub fn tx(&mut self) -> Sender<proto::Message> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone)]
pub struct WalWritter {
    tx: Sender<proto::Message>,
}

impl WalWritter {
    pub fn new(tx: Sender<proto::Message>) -> Self {
        Self { tx }
    }

    pub fn write(&self, msg: &proto::Message) {
        let msg = msg.clone();
        let _ = self.tx.send(msg);
    }
}
