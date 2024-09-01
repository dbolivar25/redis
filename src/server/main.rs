use clap::Parser;
use redis::server::{cli::Args, connection::ConnectionType, server::Server};
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args { host, port, master } = Args::parse();

    log4rs::init_file("config/log4rs.yml", Default::default())?;

    let tcp_listener = TcpListener::bind(format!("{}:{}", host, port)).await?;

    let server = Server::new(tcp_listener);

    if let Some(master) = master {
        let master_stream = TcpStream::connect(master).await?;
        let master_addr = master_stream.local_addr()?;

        server
            .add_connection(master_stream, master_addr, ConnectionType::Master)
            .await?;
    }

    server.run().await?;

    Ok(())
}
