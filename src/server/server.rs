use super::{
    connection::{ConnectionHandle, ReplicaState},
    connection_manager::ConnectionManagerHandle,
    kv_store::KVStoreHandle,
};
use crate::{common::codec::RESP3Codec, server::connection::ConnectionType};
use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::codec::Framed;

/// The server struct is responsible for listening for incoming connections and
/// managing the lifecycle of the KV store and connection manager. It is also an abstraction for
/// enabling testing of the server.
pub struct Server {
    tcp_listener: TcpListener,
    kv_store: KVStoreHandle,
    kv_store_shutdown_complete: oneshot::Receiver<()>,
    conn_manager: ConnectionManagerHandle,
    conn_manager_shutdown_complete: oneshot::Receiver<()>,
}

impl Server {
    /// Create a new server instance with the given TCP listener.
    /// The server will create a new KV store and connection manager.
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

    /// Run the server. This will listen for incoming connections and manage the lifecycle of the
    /// KV store and connection manager.
    /// The server will shut down gracefully when a SIGINT is received.
    /// If the server does not shut down within 15 seconds, it will force a shutdown.
    /// This function will return an error if the server fails to listen on the given address.
    /// Otherwise, it will return Ok(()) when the server has shut down.
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

        if self
            .conn_manager
            .shutdown()
            .await
            .inspect_err(|err| log::error!("Failed to shut down connection manager: {:?}", err))
            .is_ok()
        {
            let _ = self.conn_manager_shutdown_complete.await;
        }

        if self
            .kv_store
            .shutdown()
            .await
            .inspect_err(|err| log::error!("Failed to shut down KV store: {:?}", err))
            .is_ok()
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
            connection
                .set_conn_type(conn_type)
                .await
                .inspect_err(|err| log::error!("Failed to set connection type: {:?}", err))?;
        }

        self.conn_manager
            .add_client(connection, connection_shutdown_complete)
            .await
            .inspect_err(|err| log::error!("Failed to add client: {:?}", err))?;

        Ok(())
    }

    pub fn spawn_master_replication(&self, master_addr: String) {
        let kv_store = self.kv_store.clone();
        let conn_manager = self.conn_manager.clone();

        tokio::spawn(async move {
            let state = Arc::new(ReplicaState::new());
            let mut backoff = Duration::from_millis(100);
            const MAX_BACKOFF: Duration = Duration::from_secs(30);

            loop {
                log::info!("Connecting to master at {}", master_addr);

                match TcpStream::connect(&master_addr).await {
                    Ok(stream) => {
                        let addr = stream
                            .peer_addr()
                            .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());

                        let framed = Framed::new(stream, RESP3Codec);

                        let (handle, shutdown_complete) = ConnectionHandle::new_replica(
                            framed,
                            addr,
                            kv_store.clone(),
                            conn_manager.clone(),
                            state.clone(),
                        );

                        if let Err(e) = handle.set_conn_type(ConnectionType::Master).await {
                            log::error!("Failed to set connection type: {e}");
                        } else {
                            backoff = Duration::from_millis(100);
                            let _ = shutdown_complete.await;
                            log::warn!("Master connection lost");
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to connect to master: {e}");
                    }
                }

                log::info!("Reconnecting in {:?}...", backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
    }
}
