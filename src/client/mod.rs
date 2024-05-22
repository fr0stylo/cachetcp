use rand::prelude::*;
use std::{
    collections::HashMap,
    io::{BufWriter, Error, ErrorKind},
    net::TcpStream,
    sync::{
        mpsc::{channel, Sender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime},
};

use crate::proto::{self, Messages};

pub mod asyncronius;

type ReceverQueue = Arc<Mutex<HashMap<u128, Sender<proto::Message>>>>;

#[derive(Debug)]
pub struct Client {
    sock: TcpStream,
    queue: ReceverQueue,
    timeout: u64,
}

impl Client {
    pub fn new(addr: &str, timeout: Option<u64>) -> Self {
        let sck = TcpStream::connect(addr).unwrap();
        sck.set_nodelay(true).unwrap();
        let q = Arc::new(Mutex::new(HashMap::<u128, Sender<proto::Message>>::new()));
        let qq = q.clone();

        let tsck = sck.try_clone().unwrap();

        let _ = thread::Builder::new()
            .stack_size(32 * 1024)
            .name("worker".into())
            .spawn(move || message_handle(tsck, qq))
            .unwrap();

        return Client {
            sock: sck,
            queue: q,
            timeout: timeout.or(Some(1)).unwrap(),
        };
    }

    pub fn ping(&self) -> Result<(), Error> {
        let mut ww = self.sock.try_clone().unwrap();
        let mut w = BufWriter::new(&mut ww);

        proto::marshal(&mut proto::Message::ping(), &mut w)?;

        Ok(())
    }

    pub fn connected(&self) -> Result<Vec<u8>, Error> {
        let msg = proto::Message::connected();

        self.rpc(msg)
    }

    fn rpc(&self, msg: proto::Message) -> Result<Vec<u8>, Error> {
        let t = SystemTime::now();

        let mut msg = msg.clone();
        let mut ww = self.sock.try_clone().unwrap();
        let (tx, rx) = channel::<proto::Message>();

        self.queue.lock().unwrap().insert(msg.ts, tx);

        proto::marshal(&mut msg, &mut ww)?;

        let result = match rx.recv_timeout(Duration::from_secs(self.timeout)) {
            Ok(result) => {
                print!("{:?}", t.elapsed());

                Ok(result.data.or(Some(Vec::new().to_owned())).unwrap())
            }
            Err(e) => Err(Error::new(ErrorKind::TimedOut, e)),
        };

        self.queue.lock().unwrap().remove(&msg.ts);

        return result;
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let mut rnd = rand::thread_rng();

        let msg = proto::Message::get(key, rnd.gen());

        self.rpc(msg)
    }

    pub fn put(&self, key: &str, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut data = data.clone();

        let mut rnd = rand::thread_rng();

        let msg = proto::Message::put(key, &mut data, rnd.gen());

        self.rpc(msg)
    }

    pub fn keys(&self) -> Result<Vec<String>, Error> {
        let mut rnd = rand::thread_rng();

        let msg = proto::Message::keys(rnd.gen());

        match self.rpc(msg) {
            Ok(data) => {
                let pairs: Vec<String> = proto::resolve_pair(data)
                    .into_iter()
                    .map(|x| String::from_utf8(x.to_owned()).unwrap().to_owned())
                    .collect();

                Ok(pairs)
            }
            Err(e) => Err(e),
        }
    }

    pub fn delete(&self, key: &str) -> Result<(), Error> {
        let mut rnd = rand::thread_rng();

        let msg = proto::Message::delete(key, rnd.gen());

        match self.rpc(msg) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn message_handle(mut sock: TcpStream, q: ReceverQueue) -> std::io::Result<()> {
    let mut binding = sock.try_clone().unwrap();

    loop {
        match proto::unmarshal(&mut binding) {
            Ok(msg) => match msg.command {
                proto::Command::PING => {
                    println!("{:?}", msg);
                    proto::marshal(&mut proto::Message::pong(), &mut sock)?;
                }
                proto::Command::RECV => match q.lock().unwrap().get(&msg.ts) {
                    Some(tx) => tx.send(msg).unwrap(),
                    None => eprintln!("chan not found"),
                },
                _ => {}
            },
            Err(e) if e.kind() == ErrorKind::Unsupported => {}
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                break Ok(());
            }
            Err(e) if e.kind() == ErrorKind::ConnectionReset => {
                break Ok(());
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                break Ok(());
            }
            Err(e) => {
                eprintln!("{:?}", e)
            }
        }
    }
}
