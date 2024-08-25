use clap::Parser;
use futures::StreamExt;
use redis::{
    common::protocol::ServerProtoCodec,
    server::{cli::Args, connection, kv_store::KVStoreHandle},
};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let bind_addr = format!("{}:{}", args.host, args.port).parse::<SocketAddr>()?;

    let listener = TcpListener::bind(bind_addr).await?;
    let kv_store = KVStoreHandle::new();

    println!("\nListening on {}", listener.local_addr()?);

    loop {
        let (tcp, addr) = listener.accept().await?;
        let (sink, stream) = Framed::new(tcp, ServerProtoCodec).split();
        let kv_store = kv_store.clone();

        println!("\nAccepted connection from {}", addr);

        tokio::spawn(connection::handle(kv_store, sink, stream));
    }
}
