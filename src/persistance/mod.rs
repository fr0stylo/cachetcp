use std::io::{Error, ErrorKind};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::proto::{self, Messages};

pub mod snapshot;
pub mod wal;

fn write_key_val_buf(
    key: &str,
    data: Vec<u8>,
    cmd: Option<proto::Command>,
) -> Result<Vec<u8>, Error> {
    let key_length = key.len();
    let data_length = data.len();
    let cmd: &[u8; 1] = &[cmd.or(Some(proto::Command::DEFAULT)).unwrap().into()];

    let mut buf = Vec::<u8>::new();

    buf.extend(&key_length.to_be_bytes());
    buf.extend(&data_length.to_be_bytes());
    buf.extend(cmd);
    buf.extend(key.as_bytes());
    buf.extend(data);

    Ok(buf)
}

pub async fn read_log_entry(mut reader: File) -> Result<Option<proto::Message>, Error> {
    let pos = reader.stream_position().await?;
    let len = reader.metadata().await.unwrap().len();
    if pos == len {
        return Ok(None);
    }
    let key_len = reader.read_u64().await?;
    let data_len = reader.read_u64().await?;
    let mut cmd = vec![0u8; 1];
    reader.read_exact(&mut cmd).await?;

    let mut key = vec![0u8; key_len.try_into().unwrap()];
    reader.read_exact(&mut key).await?;

    let mut data = vec![0u8; data_len.try_into().unwrap()];
    reader.read_exact(&mut data).await?;

    let key = String::from_utf8(key).unwrap();

    match proto::Command::from(cmd[0].into()) {
        proto::Command::PUT => Ok(Some(proto::Message::put(&key, &mut data, None))),
        proto::Command::DELETE => Ok(Some(proto::Message::delete(&key, None))),
        x => Err(Error::new(
            ErrorKind::Unsupported,
            format!("{:?} unsupported command type for WAL", x),
        )),
    }
}
