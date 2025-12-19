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
use rustyline::{
    completion::FilenameCompleter,
    error::ReadlineError,
    highlight::{Highlighter, MatchingBracketHighlighter},
    hint::HistoryHinter,
    Completer, CompletionType, Config, EditMode, Editor, Helper, Hinter, Validator,
};
use std::borrow::Cow::{self, Borrowed, Owned};
use std::error::Error;
use tokio::{net::TcpStream, time::Instant};
use tokio_util::codec::Framed;

/// A helper struct for the TUI client.
/// It contains a `FilenameCompleter`, a `MatchingBracketHighlighter`,
/// a `HistoryHinter`, and a colored prompt. Basically, it makes everything pretty.
#[derive(Helper, Completer, Hinter, Validator)]
struct RedisTUIHelper {
    #[rustyline(Completer)]
    completer: FilenameCompleter,
    highlighter: MatchingBracketHighlighter,
    #[rustyline(Hinter)]
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl Highlighter for RedisTUIHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args { host, port } = Args::parse();
    let addr = format!("{host}:{port}");
    let tcp = TcpStream::connect(addr).await?;
    let (mut sink, mut stream) = Framed::new(tcp, RESP3Codec).split();

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Vi)
        .build();
    let h = RedisTUIHelper {
        completer: FilenameCompleter::new(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter::new(),
        colored_prompt: "".to_owned(),
        // validator: MatchingBracketValidator::new(),
    };

    let mut rl = Editor::with_config(config)?;
    rl.set_helper(Some(h));

    loop {
        let p = "redis> ";
        rl.helper_mut().expect("No helper").colored_prompt = format!("\x1b[1;32m{p}\x1b[0m");
        let readline = rl.readline(p);

        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;

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
                                    if let Ok(milliseconds) = tokens.get(4).unwrap_or(&"0").parse()
                                    {
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
                    "psync" => {
                        if tokens.len() != 3 {
                            println!("Usage: PSYNC <repl_id> <offset>");
                            println!("  Example: PSYNC ? -1  (full sync)");
                            println!("  Example: PSYNC <id> 5  (partial sync from offset 5)");
                            continue;
                        }
                        Commands::Psync {
                            repl_id: tokens[1].to_string(),
                            offset: tokens[2].to_string(),
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
                            println!("Done.");
                        }
                    }
                } else {
                    eprintln!("Failed to receive response");
                }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }

    println!("Shutting down.");
    Ok(())
}
