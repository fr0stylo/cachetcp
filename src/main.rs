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
            println!("{:?}", v.read_log_entry().await?);
        }
    }

    if args.client {
        let c = client::Client::new(&args.addr);
        let c = Arc::new(c);
        let cc: Arc<client::Client> = c.clone();

        let mut i: u16 = 1;
        c.connected().unwrap();

        loop {
            println!("{:?}", cc.keys()?);
            let s = i.to_be_bytes();
            println!("{:?}", cc.put("1", s.into())?);
            return Ok(());
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            println!("{:?}", cc.get("1")?);
            sleep(Duration::from_millis(rand::thread_rng().gen_range(10..50)));
            i = i + 1;
        }
    }

    let storage = Storage::new();
    let mut wal = wal::WriteAheadLog::new(&args.wal).await;
    let w = WalWritter::new(wal.tx());

    let listener = TcpListener::bind(args.addr)
        .await
        .expect("failed to bind port");
    loop {
        tokio::select! {
            _ = wal.write() => {},
            _ = server::functional::initiate_client(&listener, storage.clone(), w.clone()) => {}
        }
    }
    // v.add_to_log(proto::Message::ping()).await?;
    // v.add_to_log(proto::Message::ping()).await?;
    // v.add_to_log(proto::Message::get("123", Option::None))
    //     .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::put(
    //     "asdf",
    //     &mut vec![1, 2, 2],
    //     Option::None,
    // ))
    // .await?;
    // v.add_to_log(proto::Message::ping()).await?;
}
