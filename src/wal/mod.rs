use std::{io::Error, mem::size_of, pin::Pin};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    sync::Mutex,
};

use crate::proto::{self, Messages};

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

        // let mut ss = Arc::new(s);
        // tokio::spawn(async move {
        //     loop {
        //         ss.write();
        //     }
        // });
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

    // pub async fn add_to_log(&self, log: proto::Message) -> Result<(), Error> {
    //     let mut log = log.clone();
    //     proto::nonblocking::marshal(
    //         &mut log,
    //         Box::pin(self.fw.lock().await.try_clone().await.unwrap()),
    //     )
    //     .await
    // }

    pub async fn write_to_log(
        &self,
        key: &str,
        data: Vec<u8>,
        cmd: proto::Command,
    ) -> Result<(), Error> {
        let key_length = key.len();
        let data_length = data.len();
        println!("{:?}, {:?}", key_length, data_length);
        // let len = size_of::<usize>();
        // let full_length = 8 * 2 + 1 + key_length + data_length;
        let cmd: &[u8; 1] = &[cmd.into()];
        let mut buf = Vec::<u8>::new();
        buf.extend(&key_length.to_be_bytes());
        buf.extend(&data_length.to_be_bytes());
        buf.extend(cmd);
        buf.extend(key.as_bytes());
        buf.extend(data);
        println!("{:?}", buf);

        self.fw
            .lock()
            .await
            .try_clone()
            .await
            .unwrap()
            .write_all(&buf)
            .await?;
        self.fw
            .lock()
            .await
            .try_clone()
            .await
            .unwrap()
            .flush()
            .await
    }

    pub async fn read_log_entry(&self) -> Result<proto::Message, Error> {
        let mut reader = self.fr.try_clone().await.unwrap();

        let key_len = reader.read_u64().await?;
        let data_len = reader.read_u64().await?;
        let mut cmd: [u8; 1] = [0];
        reader.read_exact(&mut cmd).await?;

        println!("{:?}", key_len);
        println!("{:?}", data_len);
        println!("{:?}", cmd);

        let mut key: Vec<u8> = vec![0, key_len.try_into().unwrap()];
        let mut key = key.as_mut_slice();
        println!("{:?}", key.len());
        reader.read_exact(&mut key).await?;
        println!("{:?}", key);

        let mut data: Vec<u8> = vec![0, data_len.try_into().unwrap()];
        let mut data = data.as_mut_slice();
        println!("{:?}", data.len());
        reader.read_exact(&mut data).await?;
        println!("{:?}", data);

        let key = String::from_utf8(key.to_vec()).unwrap();

        Ok(proto::Message::put(&key, &mut data.to_vec(), None))

        // proto::nonblocking::unmarshal(Box::pin(self.fr.try_clone().await.unwrap())).await
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
