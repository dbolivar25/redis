use clap::Parser;
use log;
use redis::{
    common::protocol::ServerProtoCodec,
    server::{cli::Args, connection, kv_store::KVStoreHandle},
};
use std::error::Error;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args {
        host,
        port,
        master: _,
    } = Args::parse();

    log4rs::init_file("config/log4rs.yml", Default::default())?;

    let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
    let kv_store = KVStoreHandle::new();

    log::info!("Listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let stream = Framed::new(stream, ServerProtoCodec);
                let kv_store = kv_store.clone();

                tokio::spawn(async move {
                    connection::handle(stream, addr, kv_store).await;
                });
            }
            _ = tokio::signal::ctrl_c() => {
                log::info!("Shutting down");
                break;
            }
        }
    }

    Ok(())
}
