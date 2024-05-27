use std::{io::Error, mem::size_of, pin::Pin, time::SystemTime};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::FrameMessage;

pub async fn unmarshal<T: AsyncRead>(mut reader: Pin<Box<T>>) -> Result<FrameMessage, Error> {
    let mut buf = [0u8; size_of::<usize>()];
    reader.read_exact(&mut buf).await?;
    let size = usize::from_be_bytes(buf);

    let mut buf = vec![0u8; size.into()];
    reader.read_exact(&mut buf).await?;
    let fm: FrameMessage = buf.into();

    return Ok(fm);
}

pub async fn marshal<T: AsyncWrite>(msg: &FrameMessage, mut w: Pin<Box<T>>) -> Result<(), Error> {
    let buf: Vec<u8> = msg.into();

    let size = buf.len();

    w.write_all(size.to_be_bytes().as_slice()).await?;
    w.write_all(&buf).await?;
    // w.flush().await?;
    Ok(())
}
