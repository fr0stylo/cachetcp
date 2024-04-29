use std::{
    io::{BufReader, BufWriter, Error, ErrorKind, Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

static VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct Message {
    pub version: u8,
    pub command: Command,
    pub ts: u128,
    length: usize,
    pub data: Vec<u8>,
}

impl Message {
    pub fn separator() -> Vec<u8> {
        "||".into()
    }
}

pub trait Messages {
    fn put(key: &str, data: &mut Vec<u8>, ts: Option<u128>) -> Message;
    fn ping() -> Message;
    fn pong() -> Message;
    fn connected() -> Message;
    fn get(key: &str, ts: Option<u128>) -> Message;
    fn get_resp(data: Vec<u8>, ts: Option<u128>) -> Message;
    fn recv(data: Option<Vec<u8>>, ts: Option<u128>) -> Message;
}

impl Messages for Message {
    fn ping() -> Message {
        Message {
            version: VERSION,
            command: Command::PING,
            length: 0,
            data: vec![0, 0],
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
    fn pong() -> Message {
        Message {
            version: VERSION,
            command: Command::PONG,
            length: 0,
            data: vec![0, 0],
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
    fn connected() -> Message {
        Message {
            version: VERSION,
            command: Command::CONNECTED,
            length: 0,
            data: vec![0, 0],
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
    fn get(key: &str, ts: Option<u128>) -> Message {
        let ts = ts
            .or(Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ))
            .unwrap();

        Message {
            version: VERSION,
            command: Command::GET,
            length: key.len(),
            data: key.into(),
            ts: ts,
        }
    }
    fn get_resp(data: Vec<u8>, ts: Option<u128>) -> Message {
        let ts = ts
            .or(Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ))
            .unwrap();

        Message {
            version: VERSION,
            command: Command::GET,
            length: data.len(),
            data: data.into(),
            ts: ts,
        }
    }
    fn put(key: &str, data: &mut Vec<u8>, ts: Option<u128>) -> Message {
        let ts = ts
            .or(Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ))
            .unwrap();

        let mut buf: Vec<u8> = key.into();
        buf.append(&mut Message::separator());
        buf.append(data);

        Message {
            version: VERSION,
            command: Command::PUT,
            ts: ts,
            length: buf.len(),
            data: buf,
        }
    }
    fn recv(data: Option<Vec<u8>>, ts: Option<u128>) -> Message {
        let ts = ts
            .or(Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            ))
            .unwrap();

        let data = data.or(Some(Vec::<u8>::new())).unwrap();

        Message {
            version: VERSION,
            command: Command::RECV,
            ts: ts,
            length: data.len(),
            data: data,
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Message {
            version: 0,
            command: Command::default(),
            length: 0,
            data: vec![0],
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Command {
    DEFAULT,
    CONNECTED,
    PING,
    PONG,
    GET,
    PUT,
    RECV,
}

impl Into<Command> for u8 {
    fn into(self) -> Command {
        match self {
            1 => Command::CONNECTED,
            2 => Command::PING,
            3 => Command::PONG,
            4 => Command::GET,
            5 => Command::PUT,
            6 => Command::RECV,
            _ => panic!("Incorret enum value"),
        }
    }
}

impl From<Command> for u8 {
    fn from(c: Command) -> u8 {
        c as u8
    }
}

impl Default for Command {
    fn default() -> Self {
        Command::DEFAULT
    }
}

pub fn unmarshal(reader: &mut dyn Read) -> Result<Message, Error> {
    let mut reader = BufReader::new(reader);
    let mut msg = Message::default();

    let mut buf = [0; 28];
    reader.read_exact(&mut buf)?;

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
    reader.read_exact(buf)?;
    // let mut v: Vec<u8> = vec![0];
    // reader.read_to_end(&mut v);
    msg.data = buf.try_into().unwrap();

    return Ok(msg);
}

pub fn marshal(msg: &mut Message, w: &mut dyn Write) -> Result<(), Error> {
    let mut w = BufWriter::new(w);
    w.write(&[225, 225])?;
    w.write(&msg.version.to_be_bytes())?;

    let cmd: u8 = msg.command.into();
    w.write(&[cmd])?;
    w.write(&msg.ts.to_be_bytes())?;

    w.write(&msg.length.to_be_bytes())?;
    w.write(&msg.data)?;

    Ok(())
}
