use std::{error::Error, net::SocketAddr};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::codec::Framed;

use crate::{common::codec::RESP3Codec, server::connection::ConnectionType};

use super::{
    connection::ConnectionHandle, connection_manager::ConnectionManagerHandle,
    kv_store::KVStoreHandle,
};

pub struct Server {
    tcp_listener: TcpListener,
    kv_store: KVStoreHandle,
    kv_store_shutdown_complete: oneshot::Receiver<()>,
    conn_manager: ConnectionManagerHandle,
    conn_manager_shutdown_complete: oneshot::Receiver<()>,
}

impl Server {
    pub fn new(tcp_listener: TcpListener) -> Self {
        let (kv_store, kv_store_shutdown_complete) = KVStoreHandle::new();
        let (conn_manager, conn_manager_shutdown_complete) = ConnectionManagerHandle::new();
        Self {
            tcp_listener,
            kv_store,
            kv_store_shutdown_complete,
            conn_manager,
            conn_manager_shutdown_complete,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        log::info!("Listening on {}", self.tcp_listener.local_addr()?);

        loop {
            tokio::select! {
                Ok((stream, addr)) = self.tcp_listener.accept() => {
                    self.add_connection(stream, addr, ConnectionType::Client)
                    .await
                    .inspect_err(|err| log::error!("Failed to add client: {:?}", err))?;
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
                _ = tokio::signal::ctrl_c() => {
                    log::info!("Forcing shutdown.");
                },
                _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
                    log::error!("Timed out after 15 seconds. Forcing shutdown.");
                }
            );
            std::process::exit(1);
        });

        log::info!("Shutting down. Ctrl-C to force shutdown.");

        if let Ok(_) = self
            .conn_manager
            .shutdown()
            .await
            .inspect_err(|err| log::error!("Failed to shut down connection manager: {:?}", err))
        {
            let _ = self.conn_manager_shutdown_complete.await;
        }

        if let Ok(_) = self
            .kv_store
            .shutdown()
            .await
            .inspect_err(|err| log::error!("Failed to shut down KV store: {:?}", err))
        {
            let _ = self.kv_store_shutdown_complete.await;
        }

        Ok(())
    }

    pub async fn add_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        conn_type: ConnectionType,
    ) -> Result<(), Box<dyn Error>> {
        let stream = Framed::new(stream, RESP3Codec);
        let kv_store = self.kv_store.clone();
        let conn_manager = self.conn_manager.clone();

        let (connection, connection_shutdown_complete) =
            ConnectionHandle::new(stream, addr, kv_store, conn_manager);

        if let ConnectionType::Master = conn_type {
            let _ = connection
                .set_conn_type(conn_type)
                .await
                .inspect_err(|err| log::error!("Failed to set connection type: {:?}", err))?;
        }

        let _ = self
            .conn_manager
            .add_client(connection, connection_shutdown_complete)
            .await
            .inspect_err(|err| log::error!("Failed to add client: {:?}", err))?;

        Ok(())
    }
}
