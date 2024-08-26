use clap::Parser;
use log;
use redis::server::{
    cli::Args, connection::ConnectionHandle, connection_manager::ConnectionManagerHandle,
    kv_store::KVStoreHandle,
};
use std::error::Error;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args {
        host,
        port,
        master: _,
    } = Args::parse();

    log4rs::init_file("config/log4rs.yml", Default::default())?;

    let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
    let (kv_store_master, kv_store_shutdown_complete) = KVStoreHandle::new();
    let (conn_manager_master, conn_manager_shutdown_complete) = ConnectionManagerHandle::new();

    log::info!("Listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let kv_store = kv_store_master.clone();
                let conn_manager = conn_manager_master.clone();

                let (connection, connection_shutdown_complete) = ConnectionHandle::new(stream, addr, kv_store, conn_manager);

                let _ = conn_manager_master.add_client(connection, connection_shutdown_complete).await.inspect_err(|err| {
                    log::error!("Failed to add client: {:?}", err)
                });
            }
            _ = tokio::signal::ctrl_c() => {
                print!("\x08\x08"); // Erase ^C
                break;
            }
            else => {
                break;
            }
        }
    }

    tokio::spawn(async move {
        tokio::select!(
            _ = tokio::signal::ctrl_c() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {}
        );
        std::process::exit(1);
    });

    log::info!("Shutting down. Ctrl-C to force shutdown.");

    let _ = conn_manager_master
        .shutdown()
        .await
        .inspect_err(|err| log::error!("Failed to shut down connection manager: {:?}", err));
    let _ = conn_manager_shutdown_complete.await;

    let _ = kv_store_master
        .shutdown()
        .await
        .inspect_err(|err| log::error!("Failed to shut down KV store: {:?}", err));
    let _ = kv_store_shutdown_complete.await;

    Ok(())
}
