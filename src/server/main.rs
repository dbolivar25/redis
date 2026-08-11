use clap::Parser;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Root},
    Config,
};
use redis::server::{cli::Args, server::Server};
use std::error::Error;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args { host, port, master } = Args::parse();

    let config = log4rs::config::load_config_file("config/log4rs.yml", Default::default())
        .unwrap_or_else(|_| {
            let stdout = Appender::builder().build(
                "stdout",
                Box::new(
                    ConsoleAppender::builder()
                        .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new(
                            "{l} - {m}{n}",
                        )))
                        .build(),
                ),
            );

            let root = Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Info);

            Config::builder()
                .appender(stdout)
                .build(root)
                .expect("that default config is correct.")
        });

    log4rs::init_config(config).expect("that log4rs config will not fail.");

    let tcp_listener = TcpListener::bind(format!("{host}:{port}")).await?;

    let server = Server::new(tcp_listener);

    if let Some(master) = master {
        server.spawn_master_replication(master);
    }

    server.run().await?;

    Ok(())
}
