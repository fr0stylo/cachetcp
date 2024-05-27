use std::io::Error;
use std::mem::size_of;

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::proto::{self};

pub mod snapshot;
pub mod wal;

pub async fn read_log_entry(mut reader: File) -> Result<Option<proto::FrameMessage>, Error> {
    let pos = reader.stream_position().await?;
    let len = reader.metadata().await.unwrap().len();
    if pos == len {
        return Ok(None);
    }


    let mut buf = vec![0u8; size_of::<usize>()];
    reader.read_exact(&mut buf).await?;
    let size = usize::from_be_bytes(buf.as_slice().try_into().unwrap());
    if size == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; size];
    reader.read_exact(&mut buf).await?;

    let cmd: proto::FrameMessage = rmp_serde::from_slice(buf.as_slice()).unwrap();

    Ok(Some(cmd))
}
