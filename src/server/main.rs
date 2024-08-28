use clap::Parser;
use futures::{SinkExt, StreamExt};
use redis::{
    common::{
        codec::{encode_request, RESP3Codec, Request},
        resp3::RESP3Value,
    },
    server::{
        cli::Args, connection::ConnectionHandle, connection_manager::ConnectionManagerHandle,
        kv_store::KVStoreHandle,
    },
};
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args { host, port, master } = Args::parse();

    log4rs::init_file("config/log4rs.yml", Default::default())?;

    let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
    let (kv_store_master, kv_store_shutdown_complete) = KVStoreHandle::new();
    let (conn_manager_master, conn_manager_shutdown_complete) = ConnectionManagerHandle::new();

    if let Some(master) = master {
        let master_stream = TcpStream::connect(master).await?;
        let master_addr = master_stream.local_addr()?;

        let mut master_stream = Framed::new(master_stream, RESP3Codec);
        let kv_store = kv_store_master.clone();
        let conn_manager = conn_manager_master.clone();

        let ping_req = encode_request(&Request::Ping);
        let _ = master_stream
            .send(ping_req)
            .await
            .inspect_err(|err| log::error!("Failed to send PING to master: {:?}", err));

        let response = master_stream.next().await;

        if let Some(Ok(RESP3Value::SimpleString(response))) = response {
            if response != "PONG" {
                log::error!("Failed to PING master: {:?}", response);
                return Ok(());
            }
        } else {
            log::error!("Failed to PING master: {:?}", response);
            return Ok(());
        }

        let psync_req = encode_request(&Request::PSync(
            RESP3Value::BulkString(b"?".to_vec()),
            RESP3Value::BulkString(b"-1".to_vec()),
        ));

        let _ = master_stream
            .send(psync_req)
            .await
            .inspect_err(|err| log::error!("Failed to send PSYNC to master: {:?}", err));

        let response = master_stream.next().await;

        if let Some(Ok(RESP3Value::SimpleString(response))) = response {
            if response != "CONTINUE" {
                log::error!("Failed to PSYNC master: {:?}", response);
                return Ok(());
            }
        } else {
            log::error!("Failed to PSYNC master: {:?}", response);
            return Ok(());
        }

        let (connection, _connection_shutdown_complete) =
            ConnectionHandle::new(master_stream, master_addr, kv_store, conn_manager);

        let _ = conn_manager_master
            .add_master(connection)
            .await
            .inspect_err(|err| log::error!("Failed to add master: {:?}", err));
    }

    log::info!("Listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let stream = Framed::new(stream, RESP3Codec);
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
