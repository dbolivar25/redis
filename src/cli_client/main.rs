use clap::Parser;
use futures::{SinkExt, StreamExt};
use redis::{
    cli_client::cli::{Args, Commands, TTLOpt},
    common::{
        codec::{encode_request, RESP3Codec, Request, TTL},
        resp3::RESP3Value,
    },
};
use tokio::{net::TcpStream, time::Instant};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        host,
        port,
        command,
    } = Args::parse();

    let addr = format!("{host}:{port}");

    let tcp = TcpStream::connect(addr).await?;
    let (mut sink, mut stream) = Framed::new(tcp, RESP3Codec).split();

    let request = match command {
        Commands::Ping => Request::Ping,
        Commands::Echo { message } => Request::Echo(RESP3Value::BulkString(message.into_bytes())),
        Commands::Set { key, value, ttl } => {
            let ttl = ttl.map(|ttl| match ttl {
                TTLOpt::Ex { seconds } => TTL::Seconds(seconds),
                TTLOpt::Px { milliseconds } => TTL::Milliseconds(milliseconds),
            });
            Request::Set(
                RESP3Value::BulkString(key.into_bytes()),
                RESP3Value::BulkString(value.into_bytes()),
                ttl,
            )
        }
        Commands::Get { key } => Request::Get(RESP3Value::BulkString(key.into_bytes())),
        Commands::Del { key } => Request::Del(RESP3Value::BulkString(key.into_bytes())),
        Commands::Psync { repl_id, offset } => Request::PSync(
            RESP3Value::BulkString(repl_id.into_bytes()),
            RESP3Value::BulkString(offset.into_bytes()),
        ),
    };

    let request = encode_request(&request);

    let start = Instant::now();

    sink.send(request).await?;
    let response = stream.next().await;

    if let Some(Ok(response)) = response {
        let time = start.elapsed();
        println!("{response} in {time:?}");

        if let RESP3Value::SimpleString(s) = &response {
            if s.starts_with("FULLRESYNC") || s == "CONTINUE" {
                println!("Receiving replication data...");
                while let Ok(Some(data)) = tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    stream.next()
                ).await {
                    if let Ok(d) = data {
                        println!("  {d}");
                    }
                }
            }
        }
    } else {
        eprintln!("Failed to receive response");
    }

    Ok(())
}
