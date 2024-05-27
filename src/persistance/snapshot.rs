use std::{io::Error, sync::Arc};

use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::proto::CommandMessage::PUT;
use crate::proto::FrameMessage;
use crate::storage::storage::Storage;

use super::read_log_entry;

pub struct SnapshotCreator {
    path: String,
}

impl SnapshotCreator {
    pub async fn new(path: &str) -> Self {
        Self {
            path: path.to_owned(),
        }
    }

    pub async fn snapshot(&self, cc: &Arc<Storage>) -> Result<u64, Error> {
        let mut fw = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.path.clone())
            .await?;

        let keys = cc.keys().await.unwrap();
        fw.rewind().await?;

        for key in keys.clone().into_iter() {
            let res = cc.read(&key).await.unwrap();
            let cmd: FrameMessage = PUT(key.clone(), res, None).into();
            let cmd: Vec<u8> = cmd.into();
            let mut buf: Vec<u8> = cmd.len().to_be_bytes().to_vec();
            buf.extend(cmd);
            fw.write_all(&buf).await?;
        }

        fw.flush().await?;

        Ok(keys.len().try_into().unwrap())
    }

    pub async fn restore(&self, cc: &Arc<Storage>) -> Result<u64, Error> {
        let mut result = 0u64;
        let reader = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path.clone())
            .await?;
        loop {
            match read_log_entry(reader.try_clone().await?).await? {
                Some(x) => {
                    result = result + 1;
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
}

// async fn snapshot
