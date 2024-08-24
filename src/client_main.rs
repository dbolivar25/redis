use clap::Parser;
use futures::{SinkExt, StreamExt};
use redis::{
    client::cli::{Args, Commands, OptTTL},
    common::{
        protocol::{ProtocolCodec, Request, TTL},
        resp3::RESP3Value,
    },
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let addr = format!("{}:{}", args.host, args.port);

    let tcp = TcpStream::connect(addr).await?;
    let (mut sink, mut stream) = Framed::new(tcp, ProtocolCodec).split();

    match args.command {
        Commands::Ping => {
            let request = Request::Ping;

            sink.send(request).await?;
            let response = stream.next().await;

            println!("{:?}", response);
        }
        Commands::Echo { message } => {
            let request = Request::Echo(RESP3Value::BulkString(message.into_bytes()));

            sink.send(request).await?;
            let response = stream.next().await;

            println!("{:?}", response);
        }
        Commands::Set { key, value, ttl } => {
            let ttl = match ttl {
                Some(ttl) => match ttl {
                    OptTTL::Ex { seconds } => TTL::Seconds(seconds),
                    OptTTL::Px { milliseconds } => TTL::Milliseconds(milliseconds),
                },
                None => TTL::Persist,
            };
            let request = Request::Set(
                RESP3Value::BulkString(key.into_bytes()),
                RESP3Value::BulkString(value.into_bytes()),
                ttl,
            );

            sink.send(request).await?;
            let response = stream.next().await;

            println!("{:?}", response);
        }
        Commands::Get { key } => {
            let request = Request::Get(RESP3Value::BulkString(key.into_bytes()));

            sink.send(request).await?;
            let response = stream.next().await;

            println!("{:?}", response);
        }
        Commands::Del { key } => {
            let request = Request::Del(RESP3Value::BulkString(key.into_bytes()));

            sink.send(request).await?;
            let response = stream.next().await;

            println!("{:?}", response);
        }
    }

    Ok(())
}
