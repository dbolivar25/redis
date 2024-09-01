use super::connection::ConnectionHandle;
use crate::common::codec::Request;
use anyhow::Result;
use futures::future;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};

pub struct ConnectionManager {
    receiver: mpsc::Receiver<ConnectionManagerMessage>,
    master: Option<(ConnectionHandle, oneshot::Receiver<()>)>,
    clients: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
    replicas: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
}

#[derive(Debug)]
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
    Shutdown,
}

impl ConnectionManager {
    pub fn new(
        receiver: mpsc::Receiver<ConnectionManagerMessage>,
        master: Option<(ConnectionHandle, oneshot::Receiver<()>)>,
        clients: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
        replicas: Vec<(ConnectionHandle, oneshot::Receiver<()>)>,
    ) -> Self {
        ConnectionManager {
            receiver,
            master,
            clients,
            replicas,
        }
    }

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
                future::join_all(
                    self.replicas
                        .iter()
                        .map(|(replica, _)| replica.forward_request(request.clone())),
                )
                .await;
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

#[derive(Clone)]
pub struct ConnectionManagerHandle {
    sender: mpsc::Sender<ConnectionManagerMessage>,
}

impl ConnectionManagerHandle {
    pub fn new() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(32);
        let (on_shutdown_complete, shutdown_complete) = oneshot::channel();

        let connection_manager = ConnectionManager::new(receiver, None, vec![], vec![]);
        tokio::spawn(run_connection_manager(
            connection_manager,
            on_shutdown_complete,
        ));
        (ConnectionManagerHandle { sender }, shutdown_complete)
    }

    pub async fn set_master(&self, addr: SocketAddr) -> Result<()> {
        let msg = ConnectionManagerMessage::SetMaster { addr };
        self.sender.send(msg).await?;
        Ok(())
    }

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

    pub async fn set_replica(&self, addr: SocketAddr) -> Result<()> {
        let msg = ConnectionManagerMessage::SetReplica { addr };
        self.sender.send(msg).await?;
        Ok(())
    }

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

    pub async fn shutdown(&self) -> Result<()> {
        let msg = ConnectionManagerMessage::Shutdown;
        self.sender.send(msg).await?;
        Ok(())
    }
}
