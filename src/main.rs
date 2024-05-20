use std::{sync::Arc, thread::sleep, time::Duration};

use cachetcp::{
    client,
    persistance::{
        snapshot,
        wal::{self, WalWritter},
    },
    server,
    storage::storage::Storage,
};
use clap::{arg, command, Parser};
use rand::Rng;
use tokio::{join, net::TcpListener, time::interval};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    client: bool,

    #[arg(short, long, default_value_t = false)]
    wal_replay: bool,

    #[arg(short, long, default_value_t = (r#"127.0.0.1:7070"#.to_string()))]
    addr: String,

    #[arg(long, default_value_t = ("./wal.log".to_string()))]
    wal: String,

    #[arg(long, default_value_t = ("./storage.log".to_string()))]
    snapshot: String,

    #[arg(long, default_value_t = 2)]
    snapshot_internal: u64,
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

        let mut i: u64 = 1;
        c.connected().unwrap();

        loop {
            println!("{:?}", cc.get(format!("{:?}", i).as_str()).unwrap());
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            let s = i.to_be_bytes();
            println!("{:?}", cc.put(format!("{}", i).as_str(), s.into()).unwrap());
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            i = i + 1;
        }
    }

    let storage = Storage::new();
    let ss = snapshot::SnapshotCreator::new(&args.snapshot).await;
    let mut wal = wal::WriteAheadLog::new(&args.wal).await;
    let w = WalWritter::new(wal.tx());

    let keys = ss
        .restore(storage.clone())
        .await
        .expect("failed snapshot restore");
    let replayed = wal
        .replay(storage.clone())
        .await
        .expect("Failed to regenerate from wal");
    println!("Regenerated keys snapshot: {:?}", keys);
    println!("Regenerated WAL entries: {:?}", replayed);

    let listener = TcpListener::bind(args.addr.clone())
        .await
        .expect("failed to bind port");

    println!("Starting server on {}", args.addr);

    let mut ticker = interval(Duration::from_secs(args.snapshot_internal * 60));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let keys = ss.snapshot(storage.clone()).await?;
                wal.clear().await?;

                println!("Snapshot created: {:?} keys saved", keys);
            },
            _ = wal.write() => {},
            _ = server::functional::initiate_client(&listener, storage.clone(), w.clone()) => {}
        }
    }
}
