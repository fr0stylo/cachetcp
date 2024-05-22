use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, SystemTime},
};

use cachetcp::{
    client,
    persistance::{
        snapshot,
        wal::{self, WalWritter},
    },
    server,
    storage::{expirable::ExpirationController, storage::Storage},
};
use clap::{arg, command, Parser};
use rand::{thread_rng, Rng};
use tokio::{
    net::TcpListener,
    time::{interval, timeout},
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = false)]
    client: bool,

    #[arg(short, long, default_value_t = false)]
    wal_replay: bool,

    #[arg(short, long, default_value_t = ("0.0.0.0:7070".to_string()))]
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
        // let v = wal::WriteAheadLog::new(&args.wal).await;
        // loop {
        //     match v.read_log_entry().await? {
        //         Some(x) => {
        //             println!("{:?}", x);
        //         }
        //         None => {
        //             break;
        //         }
        //     };
        // }
        let ctrl = ExpirationController::new();
        let t = SystemTime::now();
        loop {
            let val = thread_rng().gen::<u64>() % 20;
            ctrl.add_expiration(format!("{}", val).as_str(), Duration::from_secs(val))
                .await;

            tokio::select! {
                key = timeout(Duration::from_secs(1), ctrl.wait()) => {
                    println!("{:?} : {:?}", t.elapsed(), key);
                },
                else => {
                    println!("{:?}",val);
                    ctrl.add_expiration(format!("{}",val).as_str(), Duration::from_secs(val)).await;
                }
            }
        }

        // return Ok(());
    }

    if args.client {
        let c = client::asyncronius::Client::new(&args.addr).await;

        let mut i: u64 = 1;
        let _ = c.connected().await;

        loop {
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            let s = i.to_be_bytes();
            let t = SystemTime::now();
            println!("{:?}", c.put(format!("{}", i).as_str(), s.into()).await);
            print!("{:?}", t.elapsed());
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));

            let t = SystemTime::now();
            println!("{:?}", c.get(format!("{:?}", i).as_str()).await);
            print!("{:?}", t.elapsed());
            i = i + 1;
        }
    }

    // let ctrl = Arc::new(ExpirationController::new());
    let storage = Storage::new();
    let storage = Arc::new(storage);
    let ss = snapshot::SnapshotCreator::new(&args.snapshot).await;
    let mut wal = wal::WriteAheadLog::new(&args.wal).await;
    let w = WalWritter::new(wal.tx());

    let keys = ss.restore(&storage).await.expect("failed snapshot restore");
    let replayed = wal
        .replay(&storage)
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
                let keys = ss.snapshot(&storage).await?;
                wal.clear().await?;

                println!("Snapshot created: {:?} keys saved", keys);
            },
            _ = storage.expire() => {},
            _ = wal.write() => {},
            _ = server::functional::initiate_client(&listener, &storage, w.clone()) => {}
        }
    }
}
