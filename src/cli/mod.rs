use clap::{command, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Runtime,

    #[clap(short, long, default_value_t=("0.0.0.0:7070").to_string(), global = true )]
    pub addr: String,

    #[clap(long, default_value_t = ("./wal.log".to_string()))]
    pub wal: String,

    #[clap(long, default_value_t = ("./storage.log".to_string()))]
    pub snapshot: String,

    #[arg(long, default_value_t = 2)]
    pub snapshot_internal: u64,
}

#[derive(Debug, Subcommand)]
pub enum Runtime {
    Server,
    Client,
    Testing,
}
