use rand::prelude::*;
use std::{
    collections::HashMap,
    io::{BufWriter, Error, ErrorKind},
    net::TcpStream,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime},
};

use crate::proto::{self, Messages};

type ReceverQueue = Arc<Mutex<HashMap<u128, SyncSender<proto::Message>>>>;
#[derive(Debug)]
pub struct Client {
    sock: TcpStream,
    queue: ReceverQueue,
    // t: JoinHandle<std::io::Result<()>>,
}

impl Client {
    pub fn new(addr: &str) -> Self {
        let sck = TcpStream::connect(addr).unwrap();
        sck.set_nodelay(true).unwrap();
        sck.set_nonblocking(false).unwrap();
        let q = Arc::new(Mutex::new(
            HashMap::<u128, SyncSender<proto::Message>>::new(),
        ));
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
            // t: t,
        };
    }

    pub fn ping(&self) -> Result<(), Error> {
        let mut ww = self.sock.try_clone().unwrap();
        let mut w = BufWriter::new(&mut ww);

        proto::marshal(&mut proto::Message::ping(), &mut w)?;

        Ok(())
    }

    fn rpc(&self, msg: proto::Message) -> Result<Vec<u8>, Error> {
        let t = SystemTime::now();

        let mut msg = msg.clone();
        let mut ww = self.sock.try_clone().unwrap();
        let (tx, rx) = sync_channel::<proto::Message>(1);

        {
            self.queue.lock().unwrap().insert(msg.ts, tx);
        }

        proto::marshal(&mut msg, &mut ww)?;

        let result = match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(result) => {
                print!("{:?}", t.elapsed());

                Ok(result.data)
            }
            Err(e) => {
                eprintln!("{}", e);
                Err(Error::new(ErrorKind::Unsupported, "recv error"))
            }
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
}

fn message_handle(mut sock: TcpStream, q: ReceverQueue) -> std::io::Result<()> {
    let mut binding = sock.try_clone().unwrap();
    // let mut read = BufReader::new(&mut sock);
    // let mut w = BufWriter::new(&mut binding);
    proto::marshal(&mut proto::Message::connected(), &mut sock)?;

    loop {
        match proto::unmarshal(&mut binding) {
            Ok(msg) => match msg.command {
                proto::Command::PING => {
                    proto::marshal(&mut proto::Message::pong(), &mut sock)?;
                }
                proto::Command::RECV => match q.lock().unwrap().get(&msg.ts) {
                    Some(tx) => tx.send(msg).unwrap(),
                    None => println!("chan not found"),
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
                println!("{:?}", e)
            }
        }
    }
}
