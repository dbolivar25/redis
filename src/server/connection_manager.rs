use super::connection::ConnectionHandle;
use crate::common::codec::Request;
use anyhow::Result;
use futures::future;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

fn generate_repl_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    format!("{nanos:032x}{pid:08x}")
}

/// ConnectionManager is responsible for managing all connections to the server.
/// It keeps track of the master connection, client connections, and replica connections.
/// It also provides an API for adding, removing, and broadcasting messages to connections.
/// The ConnectionManager has a corresponding handle struct that is used to interact with the ConnectionManager.
pub struct ConnectionManager {
    receiver: mpsc::Receiver<ConnectionManagerMessage>,
    master: Option<(ConnectionHandle, oneshot::Receiver<()>)>,
    clients: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
    replicas: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
    repl_id: String,
}

/// ConnectionManagerMessage is an enum that represents the different types of messages that can be sent to the ConnectionManager.
pub enum ConnectionManagerMessage {
    AddClient {
        connection: ConnectionHandle,
        connection_shutdown_complete: oneshot::Receiver<()>,
    },
    SetMaster {
        addr: SocketAddr,
    },
    SetReplica {
        addr: SocketAddr,
    },
    RemoveConnection {
        addr: SocketAddr,
    },
    Broadcast {
        request: Request,
    },
    GetReplId {
        respond_to: oneshot::Sender<String>,
    },
    Shutdown,
}

impl ConnectionManager {
    /// Creates a new ConnectionManager with the given receiver, master connection, client connections, and replica connections.
    /// The receiver is used to receive messages from the ConnectionManagerHandle. The master connection is an optional connection
    /// that represents the master server. The client connections are a vector of connections that represent the clients. The replica
    /// connections are a vector of connections that represent the replicas.
    pub fn new(
        receiver: mpsc::Receiver<ConnectionManagerMessage>,
        master: Option<(ConnectionHandle, oneshot::Receiver<()>)>,
        clients: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
        replicas: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
        repl_id: String,
    ) -> Self {
        ConnectionManager {
            receiver,
            master,
            clients,
            replicas,
            repl_id,
        }
    }

    /// Handles the given message by performing the appropriate action based on the message type.
    pub async fn handle_message(&mut self, msg: ConnectionManagerMessage) {
        match msg {
            ConnectionManagerMessage::AddClient {
                connection,
                connection_shutdown_complete,
            } => {
                self.clients
                    .push((connection, connection_shutdown_complete));
            }
            ConnectionManagerMessage::SetMaster { addr } => {
                if let Some(idx) = self.clients.iter().position(|(c, _)| c.addr == addr) {
                    let entry = self.clients.remove(idx);
                    if let Some((existing_master, existing_master_shutdown)) =
                        self.master.replace(entry)
                    {
                        log::warn!("Replacing existing master connection");
                        existing_master
                            .shutdown()
                            .await
                            .inspect_err(|err| {
                                log::error!(
                                    "Failed to shut down existing master connection: {:?}",
                                    err
                                )
                            })
                            .ok();
                        let _ = existing_master_shutdown.await;
                    }
                }
            }
            ConnectionManagerMessage::SetReplica { addr } => {
                if let Some(idx) = self.clients.iter().position(|(c, _)| c.addr == addr) {
                    let entry = self.clients.remove(idx);
                    self.replicas.push(entry);
                }
            }
            ConnectionManagerMessage::RemoveConnection { addr } => {
                if let Some(index) = self.clients.iter().position(|(c, _)| c.addr == addr) {
                    self.clients.remove(index);
                } else if let Some(index) = self.replicas.iter().position(|(c, _)| c.addr == addr) {
                    self.replicas.remove(index);
                }
            }
            ConnectionManagerMessage::Broadcast { request } => {
                let mut to_disconnect = vec![];

                for (replica, _) in &self.replicas {
                    if let Err(e) = replica.try_forward_request(request.clone()) {
                        log::warn!(
                            "Replica {} buffer full ({e}), scheduling disconnect for resync",
                            replica.addr
                        );
                        to_disconnect.push(replica.addr);
                    }
                }

                for addr in to_disconnect {
                    if let Some(idx) = self.replicas.iter().position(|(r, _)| r.addr == addr) {
                        let (handle, _shutdown_rx) = self.replicas.remove(idx);
                        tokio::spawn(async move {
                            if let Err(e) = handle.shutdown().await {
                                log::error!("Failed to shutdown lagging replica: {e}");
                            }
                        });
                    }
                }
            }
            ConnectionManagerMessage::GetReplId { respond_to } => {
                respond_to.send(self.repl_id.clone()).ok();
            }
            ConnectionManagerMessage::Shutdown => {
                let client_shutdowns =
                    future::join_all(self.clients.iter().map(|(client, _)| client.shutdown()));

                let replica_shutdowns =
                    future::join_all(self.replicas.iter().map(|(replica, _)| replica.shutdown()));

                let _ = tokio::join!(client_shutdowns, replica_shutdowns);

                if let Some((master_connection, _)) = self.master.as_ref() {
                    master_connection
                        .shutdown()
                        .await
                        .inspect_err(|err| {
                            log::error!("Failed to shut down master connection: {:?}", err)
                        })
                        .ok();
                }

                self.receiver.close();
            }
        }
    }
}

/// Runs the ConnectionManager by receiving messages from the receiver and handling them.
/// The ConnectionManager will continue to run until it receives a shutdown message. When the ConnectionManager
/// receives a shutdown message, it will shut down all connections and send a message to the on_shutdown_complete
/// sender to indicate that it has completed shutting down.
async fn run_connection_manager(
    mut connection_manager: ConnectionManager,
    on_shutdown_complete: oneshot::Sender<()>,
) {
    log::info!("Connection manager started");

    loop {
        tokio::select! {
            Some(msg) = connection_manager.receiver.recv() => {
                connection_manager.handle_message(msg).await;
            }
            else => {
                break;
            }
        }
    }

    let connection_shutdown_channels = connection_manager
        .clients
        .into_iter()
        .chain(connection_manager.replicas)
        .map(|(_, s)| s)
        .collect::<Vec<_>>();

    let _ = future::join_all(connection_shutdown_channels).await;

    if let Some((_, master_shutdown)) = connection_manager.master.take() {
        let _ = master_shutdown.await;
    }

    log::info!("Connection manager shut down");
    on_shutdown_complete.send(()).ok();
}

/// ConnectionManagerHandle is a handle that is used to interact with the ConnectionManager.
#[derive(Clone)]
pub struct ConnectionManagerHandle {
    sender: mpsc::Sender<ConnectionManagerMessage>,
}

impl ConnectionManagerHandle {
    pub fn new() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(512);
        let (on_shutdown_complete, shutdown_complete) = oneshot::channel();

        let repl_id = generate_repl_id();
        log::info!("Generated replication ID: {}", repl_id);
        let connection_manager = ConnectionManager::new(receiver, None, vec![], vec![], repl_id);
        tokio::spawn(run_connection_manager(
            connection_manager,
            on_shutdown_complete,
        ));
        (ConnectionManagerHandle { sender }, shutdown_complete)
    }

    /// Sets the master connection to the given address.
    pub async fn set_master(&self, addr: SocketAddr) -> Result<()> {
        let msg = ConnectionManagerMessage::SetMaster { addr };
        self.sender.send(msg).await?;
        Ok(())
    }

    /// Adds a client connection to the ConnectionManager.
    pub async fn add_client(
        &self,
        connection: ConnectionHandle,
        connection_shutdown_complete: oneshot::Receiver<()>,
    ) -> Result<()> {
        let msg = ConnectionManagerMessage::AddClient {
            connection,
            connection_shutdown_complete,
        };
        self.sender.send(msg).await?;
        Ok(())
    }

    /// Marks a connection as a replica.
    pub async fn set_replica(&self, addr: SocketAddr) -> Result<()> {
        let msg = ConnectionManagerMessage::SetReplica { addr };
        self.sender.send(msg).await?;
        Ok(())
    }

    /// Removes a connection from the ConnectionManager.
    pub async fn remove_connection(&self, addr: SocketAddr) -> Result<()> {
        let msg = ConnectionManagerMessage::RemoveConnection { addr };
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn broadcast(&self, request: Request) -> Result<()> {
        let msg = ConnectionManagerMessage::Broadcast { request };
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn get_repl_id(&self) -> Result<String> {
        let (respond_to, response) = oneshot::channel();
        let msg = ConnectionManagerMessage::GetReplId { respond_to };
        self.sender.send(msg).await?;
        response.await.map_err(Into::into)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let msg = ConnectionManagerMessage::Shutdown;
        self.sender.send(msg).await?;
        Ok(())
    }
}
