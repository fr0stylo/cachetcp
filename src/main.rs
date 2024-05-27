use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, SystemTime},
};

use cachetcp::proto::testing;
use cachetcp::{
    cli, client,
    persistance::{
        snapshot,
        wal::{self, WalWritter},
    },
    server,
    storage::storage::Storage,
};
use clap::Parser;
use rand::{thread_rng, Rng};
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::{fs, net::TcpListener, time::interval};

async fn handle_server(args: &cli::Args) -> Result<(), std::io::Error> {
    let storage = Arc::new(Storage::new());
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
            // _ = storage.expire() => {},
            res = wal.write() => {
                res.expect("Failed to write WAL entry")
            },
            res = server::functional::initiate_client(&listener, &storage, &w) => {
                res.expect("Failed initiate client")
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TestData {
    #[serde(alias = "postId")]
    post_id: u64,
    id: u64,
    name: String,
    email: String,
    body: String,
}

async fn handle_client(args: &cli::Args) -> Result<(), std::io::Error> {
    

    let test_data = fs::read_to_string("./test_data.json").await?;
    let test_data: Vec<TestData> = serde_json::from_str(test_data.as_str())?;

    let c = client::asyncronius::Client::new(&args.addr).await;

    let mut i: u64 = 1;
    let _ = c.connected().await;
    let mut rng = thread_rng();

    loop {
        sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
        let s = i.to_be_bytes();
        let buf = serde_json::to_vec(&test_data.choose(&mut rng)).unwrap();
        let t = SystemTime::now();
        let res = c.put(format!("{}", i).as_str(), buf, None).await;

        println!("PUT {:?} {:?}", t.elapsed(), res);
        sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));

        let t = SystemTime::now();
        let res = c.get(format!("{:?}", i).as_str()).await;
        let buf: TestData = serde_json::from_slice(res.unwrap().as_slice()).unwrap();
        println!("GET {:?} {:?}", t.elapsed(), buf);

        i = i + 1;
    }
}

async fn handle_testing(args: &cli::Args) -> Result<(), std::io::Error> {
    // let ctrl = ExpirationController::new();
    // let t = SystemTime::now();
    // loop {
    //     let val = thread_rng().gen::<u64>() % 20;
    //     ctrl.add_expiration(format!("{}", val).as_str(), Duration::from_secs(val))
    //         .await;
    //
    //     tokio::select! {
    //         key = timeout(Duration::from_secs(1), ctrl.wait()) => {
    //             println!("{:?} : {:?}", t.elapsed(), key);
    //         },
    //         else => {
    //             println!("{:?}",val);
    //             ctrl.add_expiration(format!("{}",val).as_str(), Duration::from_secs(val)).await;
    //         }
    //     }
    // }
    testing();
    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = cli::Args::parse();

    match args.subcommand {
        cli::Runtime::Client => handle_client(&args).await,
        // cli::Runtime::Client => cli::start_interactive(),
        cli::Runtime::Testing => handle_testing(&args).await,
        cli::Runtime::Server => handle_server(&args).await,
        _ => handle_server(&args).await,
    }
}
