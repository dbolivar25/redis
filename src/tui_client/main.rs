use clap::Parser;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use redis::{
    cli_client::cli::{Commands, TTLOpt},
    common::{
        codec::{encode_request, RESP3Codec, Request, TTL},
        resp3::RESP3Value,
    },
    tui_client::cli::Args,
};
use std::error::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Instant,
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args { host, port, .. } = Args::parse();
    let addr = format!("{}:{}", host, port);
    let tcp = TcpStream::connect(addr).await?;
    let (mut sink, mut stream) = Framed::new(tcp, RESP3Codec).split();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("\nShutting down.");
        std::process::exit(0);
    });

    let mut input = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    println!("Redis Shell Client. Type 'exit' to quit.");
    loop {
        stdout.write_all(b"> ").await?;
        stdout.flush().await?;

        let line = match input.next_line().await? {
            Some(line) => line.trim().to_string(),
            None => break,
        };

        if line.to_lowercase() == "exit" {
            break;
        }

        let tokens = line.split_whitespace().collect_vec();
        if tokens.is_empty() {
            continue;
        }

        let command = match tokens[0].to_lowercase().as_str() {
            "ping" => Commands::Ping,
            "echo" => {
                if tokens.len() < 2 {
                    println!("Usage: ECHO <message>");
                    continue;
                }
                Commands::Echo {
                    message: tokens[1..].join(" "),
                }
            }
            "set" => {
                if tokens.len() < 3 {
                    println!("Usage: SET <key> <value> [EX <seconds> | PX <milliseconds>]");
                    continue;
                }
                let ttl = if tokens.len() > 3 {
                    match tokens[3].to_lowercase().as_str() {
                        "ex" => {
                            if let Ok(seconds) = tokens.get(4).unwrap_or(&"0").parse() {
                                Some(TTLOpt::Ex { seconds })
                            } else {
                                println!("Invalid EX value");
                                continue;
                            }
                        }
                        "px" => {
                            if let Ok(milliseconds) = tokens.get(4).unwrap_or(&"0").parse() {
                                Some(TTLOpt::Px { milliseconds })
                            } else {
                                println!("Invalid PX value");
                                continue;
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                Commands::Set {
                    key: tokens[1].to_string(),
                    value: tokens[2].to_string(),
                    ttl,
                }
            }
            "get" => {
                if tokens.len() != 2 {
                    println!("Usage: GET <key>");
                    continue;
                }
                Commands::Get {
                    key: tokens[1].to_string(),
                }
            }
            "del" => {
                if tokens.len() != 2 {
                    println!("Usage: DEL <key>");
                    continue;
                }
                Commands::Del {
                    key: tokens[1].to_string(),
                }
            }
            _ => {
                println!("Unknown command: {}", tokens[0]);
                continue;
            }
        };

        let request = match command {
            Commands::Ping => Request::Ping,
            Commands::Echo { message } => {
                Request::Echo(RESP3Value::BulkString(message.into_bytes()))
            }
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
        };

        let request = encode_request(&request);

        let start = Instant::now();

        sink.send(request).await?;
        let response = stream.next().await;

        if let Some(Ok(response)) = response {
            let time = start.elapsed();
            println!("{} in {:?}\n", response, time);
        } else {
            eprintln!("Failed to receive response");
        }
    }

    println!("Shutting down.");
    Ok(())
}