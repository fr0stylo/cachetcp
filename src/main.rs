use std::{env, sync::Arc, thread::sleep, time::Duration};

use cachetcp::{
    client,
    server,
};
use rand::Rng;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let addr = "127.0.0.1:7070";

    if args.len() > 1 {
        let c = client::Client::new(addr);
        let c = Arc::new(c);
        let cc = c.clone();

        let mut i = 0;
        c.connected().unwrap();
    
        loop {
            println!("{:?}", cc.keys()?);
            println!("{:?}", cc.put("1", vec![i])?);
            sleep(Duration::from_millis(
                rand::thread_rng().gen_range(10..1000),
            ));
            println!("{:?}", cc.get("1")?);
            sleep(Duration::from_millis(
                rand::thread_rng().gen_range(10..1000),
            ));
            i = i + 1;
        }
    } else {
        server::nonblocking::Server::new(addr)
            .await
            .start_recv()
            .await?;

        //     // server(addr)
    }

    // let v = wal::WriteAheadLog::new("./test.log").await;

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

    // loop {
    //     println!("{:?}", v.read_log_entry().await?);
    // }

    Ok(())
}
