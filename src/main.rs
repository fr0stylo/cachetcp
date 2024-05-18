use std::{sync::Arc, thread::sleep, time::Duration};

use cachetcp::{
    client, server,
    storage::storage::Storage,
    wal::{self, WalWritter},
};
use clap::{arg, command, Parser};
use rand::Rng;
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    client: bool,

    #[arg(short, long, default_value_t = false)]
    wal_replay: bool,

    #[arg(short, long, default_value_t = ("127.0.0.1:7070".to_string()))]
    addr: String,

    #[arg(long, default_value_t = ("./wal.log".to_string()))]
    wal: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    if args.wal_replay {
        let v = wal::WriteAheadLog::new(&args.wal).await;
        loop {
            match v.read_log_entry().await? {
                Some(x) => {
                    println!("{:?}", x);
                }
                None => {
                    break;
                }
            };
        }

        return Ok(());
    }

    if args.client {
        let c = client::Client::new(&args.addr);
        let c = Arc::new(c);
        let cc: Arc<client::Client> = c.clone();

        let mut i: u128 = 1;
        c.connected().unwrap();

        loop {
            println!("{:?}", cc.keys()?);
            println!("{:?}", cc.get("1")?);
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            let s = i.to_be_bytes();
            println!("{:?}", cc.put("1", s.into())?);
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            i = i + 1;
        }
    }

    let storage = Storage::new();
    let mut wal = wal::WriteAheadLog::new(&args.wal).await;
    let w = WalWritter::new(wal.tx());

    let replayed = wal
        .replay(storage.clone())
        .await
        .expect("Failed to regenerate from wal");
    println!("Regenerated from WAL: {:?}", replayed);

    let listener = TcpListener::bind(args.addr.clone())
        .await
        .expect("failed to bind port");

    println!("Starting server on {}", args.addr);

    loop {
        tokio::select! {
            _ = wal.write() => {},
            _ = server::functional::initiate_client(&listener, storage.clone(), w.clone()) => {}
        }
    }
}
