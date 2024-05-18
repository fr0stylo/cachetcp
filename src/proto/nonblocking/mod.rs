use std::{
    io::{Error, ErrorKind},
    pin::Pin,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Message, VERSION};

pub async fn unmarshal<T: AsyncRead>(mut reader: Pin<Box<T>>) -> Result<Message, Error> {
    // let mut reader = Box::pin(reader);
    let mut msg = Message::default();

    let mut buf = [0; 28];
    reader.read_exact(&mut buf).await?;
    if buf[0..2].ne(&[225, 225]) {
        return Err(Error::new(ErrorKind::Unsupported, "empty"));
    }

    // let mut buf = [0; 1];
    // reader.read_exact(&mut buf)?;
    msg.version = u8::from_be_bytes(buf[2..3].try_into().unwrap());

    if msg.version != VERSION {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "version is not supported",
        ));
    }

    // buf = [0; 1];
    // reader.read_exact(&mut buf)?;
    msg.command = u8::from_be_bytes(buf[3..4].try_into().unwrap()).into();

    // let mut buf = [0; 16];
    // reader.read_exact(&mut buf)?;
    msg.ts = u128::from_be_bytes(buf[4..20].try_into().unwrap()).into();

    // let mut buf = [0; 8];
    // reader.read_exact(&mut buf)?;
    msg.length = usize::from_be_bytes(buf[20..28].try_into().unwrap());

    if msg.length >= buf.len().try_into().unwrap() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "incorrent data provided",
        ));
    }

    let mut v: Vec<u8> = vec![0; msg.length.try_into().unwrap()];
    let buf = v.as_mut_slice();
    reader.read_exact(buf).await?;
    // let mut v: Vec<u8> = vec![0];
    // reader.read_to_end(&mut v);
    msg.data = Some(buf.to_vec());

    return Ok(msg);
}

pub async fn marshal<T: AsyncWrite>(msg: &mut Message, mut w: Pin<Box<T>>) -> Result<(), Error> {
    let mut buf = vec![];
    buf.write(&[225, 225]).await?;
    buf.write(&msg.version.to_be_bytes()).await?;

    let cmd: u8 = msg.command.into();
    buf.write(&[cmd]).await?;
    buf.write(&msg.ts.to_be_bytes()).await?;
    buf.write(&msg.length.to_be_bytes()).await?;
    match &msg.data {
        Some(x) => {
            buf.write(x).await?;
        }
        None => {}
    }

    w.write_all(&buf).await?;

    Ok(())
}

