use clap::Parser;
use futures::{SinkExt, StreamExt};
use redis::{
    client::cli::{Args, Commands, TTLOpt},
    common::{
        protocol::{ClientProtoCodec, Request, TTL},
        resp3::RESP3Value,
    },
};
use tokio::{net::TcpStream, time::Instant};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let addr = format!("{}:{}", args.host, args.port);

    let tcp = TcpStream::connect(addr).await?;
    let (mut sink, mut stream) = Framed::new(tcp, ClientProtoCodec).split();

    match args.command {
        Commands::Ping => {
            let request = Request::Ping;

            let start = Instant::now();
            sink.send(request).await?;
            let response = stream.next().await;
            let time = start.elapsed();

            println!("{:?} in {:?}", response, time);
        }
        Commands::Echo { message } => {
            let request = Request::Echo(RESP3Value::BulkString(message.into_bytes()));

            let start = Instant::now();
            sink.send(request).await?;
            let response = stream.next().await;
            let time = start.elapsed();

            println!("{:?} in {:?}", response, time);
        }
        Commands::Set { key, value, ttl } => {
            let ttl = match ttl {
                Some(TTLOpt::Ex { seconds }) => TTL::Seconds(seconds),
                Some(TTLOpt::Px { milliseconds }) => TTL::Milliseconds(milliseconds),
                None => TTL::Persist,
            };
            let request = Request::Set(
                RESP3Value::BulkString(key.into_bytes()),
                RESP3Value::BulkString(value.into_bytes()),
                ttl,
            );

            let start = Instant::now();
            sink.send(request).await?;
            let response = stream.next().await;
            let time = start.elapsed();

            println!("{:?} in {:?}", response, time);
        }
        Commands::Get { key } => {
            let request = Request::Get(RESP3Value::BulkString(key.into_bytes()));

            let start = Instant::now();
            sink.send(request).await?;
            let response = stream.next().await;
            let time = start.elapsed();

            println!("{:?} in {:?}", response, time);
        }
        Commands::Del { key } => {
            let request = Request::Del(RESP3Value::BulkString(key.into_bytes()));

            let start = Instant::now();
            sink.send(request).await?;
            let response = stream.next().await;
            let time = start.elapsed();

            println!("{:?} in {:?}", response, time);
        }
    }

    Ok(())
}
