use clap::Parser;
use redis::server::cli::Args;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let _addr = format!("{}:{}", args.host, args.port).parse::<SocketAddr>()?;

    Ok(())
}
