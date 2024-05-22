use std::{io::Error, sync::Arc};

use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{proto, storage::storage::Storage};

use super::{read_log_entry, write_key_val_buf};

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
            let res = cc.read(key.clone()).await.unwrap();
            let buf = write_key_val_buf(&key, res, Some(proto::Command::PUT))?;
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
                    let (key, data) = proto::split_parts(x.data.clone().unwrap());
                    match x.command {
                        proto::Command::PUT => cc.write(&key.unwrap(), data.unwrap()).await,
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
