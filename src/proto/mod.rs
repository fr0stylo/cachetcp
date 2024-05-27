use serde::{Deserialize, Serialize};
use std::{
    io::{BufReader, BufWriter, Error, Read, Write},
    mem::size_of,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod nonblocking;

pub static VERSION: u8 = 1;


pub fn resolve_pair(input: Vec<u8>) -> Vec<Vec<u8>> {
    input.into_iter().fold(Vec::new(), |mut acc, x| {
        if x == 0 || acc.is_empty() {
            acc.push(Vec::new());
        }
        acc.last_mut().unwrap().push(x);
        acc
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum CommandMessage {
    CONNECTED(),
    PING(),
    PONG(),
    GET(String),
    PUT(String, Vec<u8>, Option<Duration>),
    DELETE(String),

    KEYS(),

    RECV(Option<Vec<u8>>),
}

impl From<CommandMessage> for Vec<u8> {
    fn from(c: CommandMessage) -> Vec<u8> {
        rmp_serde::to_vec(&c).unwrap()
    }
}

impl Into<CommandMessage> for Vec<u8> {
    fn into(self) -> CommandMessage {
        rmp_serde::from_slice(&self.as_slice()).unwrap()
    }
}

impl From<CommandMessage> for FrameMessage {
    fn from(c: CommandMessage) -> FrameMessage {
        FrameMessage {
            command: c,
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            version: 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrameMessage {
    pub ts: u128,
    version: u8,
    pub command: CommandMessage,
}

impl FrameMessage {
    pub fn reply(self, data: Option<Vec<u8>>) -> FrameMessage {
        FrameMessage {
            ts: self.ts,
            version: VERSION,
            command: CommandMessage::RECV(data),
        }
    }

    pub fn reply_borrow(&self, data: Option<Vec<u8>>) -> FrameMessage {
        FrameMessage {
            ts: self.ts,
            version: VERSION,
            command: CommandMessage::RECV(data),
        }
    }
}

impl From<FrameMessage> for Vec<u8> {
    fn from(c: FrameMessage) -> Vec<u8> {
        rmp_serde::to_vec(&c).unwrap()
    }
}

impl Into<FrameMessage> for Vec<u8> {
    fn into(self) -> FrameMessage {
        rmp_serde::from_slice(&self.as_slice()).unwrap()
    }
}

impl From<&FrameMessage> for Vec<u8> {
    fn from(c: &FrameMessage) -> Vec<u8> {
        rmp_serde::to_vec(c).unwrap()
    }
}

pub fn testing() {
    let buf: Vec<u8> = FrameMessage {
        ts: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        version: VERSION,
        command: CommandMessage::GET("Testing".to_owned()),
    }
        .try_into()
        .unwrap();
    println!("{:?}", buf);

    let cmd: FrameMessage = buf.try_into().unwrap();
    println!("{:?}", cmd);
}
