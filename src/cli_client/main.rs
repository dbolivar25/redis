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
        Commands::Incr { key } => Request::Incr(RESP3Value::BulkString(key.into_bytes())),
        Commands::Decr { key } => Request::Decr(RESP3Value::BulkString(key.into_bytes())),
        Commands::Incrby { key, delta } => {
            Request::IncrBy(RESP3Value::BulkString(key.into_bytes()), delta)
        }
        Commands::Decrby { key, delta } => {
            Request::DecrBy(RESP3Value::BulkString(key.into_bytes()), delta)
        }
        Commands::Append { key, value } => Request::Append(
            RESP3Value::BulkString(key.into_bytes()),
            RESP3Value::BulkString(value.into_bytes()),
        ),
        Commands::Strlen { key } => Request::StrLen(RESP3Value::BulkString(key.into_bytes())),
        Commands::Exists { keys } => {
            Request::Exists(keys.into_iter().map(|k| RESP3Value::BulkString(k.into_bytes())).collect())
        }
        Commands::Keys { pattern } => Request::Keys(RESP3Value::BulkString(pattern.into_bytes())),
        Commands::Rename { key, newkey } => Request::Rename(
            RESP3Value::BulkString(key.into_bytes()),
            RESP3Value::BulkString(newkey.into_bytes()),
        ),
        Commands::Type { key } => Request::Type(RESP3Value::BulkString(key.into_bytes())),
        Commands::Expire { key, seconds } => {
            Request::Expire(RESP3Value::BulkString(key.into_bytes()), seconds)
        }
        Commands::Pexpire { key, milliseconds } => {
            Request::PExpire(RESP3Value::BulkString(key.into_bytes()), milliseconds)
        }
        Commands::Expireat { key, timestamp } => {
            Request::ExpireAt(RESP3Value::BulkString(key.into_bytes()), timestamp)
        }
        Commands::Ttl { key } => Request::Ttl(RESP3Value::BulkString(key.into_bytes())),
        Commands::Pttl { key } => Request::PTtl(RESP3Value::BulkString(key.into_bytes())),
        Commands::Persist { key } => Request::Persist(RESP3Value::BulkString(key.into_bytes())),
        Commands::Mget { keys } => {
            Request::MGet(keys.into_iter().map(|k| RESP3Value::BulkString(k.into_bytes())).collect())
        }
        Commands::Mset { pairs } => {
            if pairs.len() % 2 != 0 {
                eprintln!("MSET requires an even number of arguments (key value pairs)");
                std::process::exit(1);
            }
            let pairs: Vec<(RESP3Value, RESP3Value)> = pairs
                .chunks(2)
                .map(|chunk| {
                    (
                        RESP3Value::BulkString(chunk[0].clone().into_bytes()),
                        RESP3Value::BulkString(chunk[1].clone().into_bytes()),
                    )
                })
                .collect();
            Request::MSet(pairs)
        }
        Commands::Dbsize => Request::DbSize,
        Commands::Flushdb => Request::FlushDb,
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
