use super::{connection_manager::ConnectionManagerHandle, kv_store::KVStoreHandle};
use crate::common::{
    codec::{decode_request, decode_snapshot, encode_request, encode_snapshot, RESP3Codec, Request, TTL},
    resp3::RESP3Value,
};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::codec::Framed;

/// Represents a connection to a client, master, or replica.
/// The connection is bidirectional, with the server sending responses to the client
/// and the client sending requests to the server. This struct is implemented as an actor which has
/// a cooresponding handle struct that can be used to send messages to the actor.
pub struct Connection {
    receiver: mpsc::Receiver<ConnectionMessage>,
    stream: Framed<TcpStream, RESP3Codec>,
    addr: SocketAddr,
    conn_type: ConnectionType,
    kv_store: KVStoreHandle,
    conn_manager: ConnectionManagerHandle,
}

/// Represents the type of connection.
#[derive(Debug)]
pub enum ConnectionType {
    Master,
    Replica,
    Client,
}

/// Represents the messages that can be sent to a connection actor.
#[derive(Debug)]
pub enum ConnectionMessage {
    ForwardRequest { request: Request },
    SetConnType { conn_type: ConnectionType },
    Shutdown,
}

impl Connection {
    /// Creates a new connection actor.
    pub fn new(
        receiver: mpsc::Receiver<ConnectionMessage>,
        stream: Framed<TcpStream, RESP3Codec>,
        addr: SocketAddr,
        kv_store: KVStoreHandle,
        conn_manager: ConnectionManagerHandle,
    ) -> Self {
        let conn_type = ConnectionType::Client;

        Connection {
            receiver,
            stream,
            conn_type,
            addr,
            kv_store,
            conn_manager,
        }
    }

    /// Handles a message sent to the connection actor.
    async fn handle_message(&mut self, msg: ConnectionMessage) {
        match msg {
            ConnectionMessage::ForwardRequest { request } => {
                log::info!("Forwarding request: {}", request);

                let resp3 = encode_request(&request);

                let _ = self
                    .stream
                    .send(resp3)
                    .await
                    .inspect_err(|err| log::error!("Failed to send request: {:?}", err));
            }
            ConnectionMessage::SetConnType { conn_type } => {
                self.conn_type = conn_type;

                if let ConnectionType::Master = self.conn_type {
                    let ping_req = encode_request(&Request::Ping);
                    let _ =
                        self.stream.send(ping_req).await.inspect_err(|err| {
                            log::error!("Failed to send PING to master: {:?}", err)
                        });

                    let response = self.stream.next().await;

                    if let Some(Ok(RESP3Value::SimpleString(response))) = response {
                        if response != "PONG" {
                            log::error!("Failed to PING master: {:?}", response);
                            return;
                        }
                    } else {
                        log::error!("Failed to PING master: {:?}", response);
                        return;
                    }

                    let psync_req = encode_request(&Request::PSync(
                        RESP3Value::BulkString(b"?".to_vec()),
                        RESP3Value::BulkString(b"-1".to_vec()),
                    ));

                    let _ = self.stream.send(psync_req).await.inspect_err(|err| {
                        log::error!("Failed to send PSYNC to master: {:?}", err)
                    });

                    let response = self.stream.next().await;

                    match response {
                        Some(Ok(RESP3Value::SimpleString(s))) if s.starts_with("FULLRESYNC") => {
                            let parts: Vec<&str> = s.split_whitespace().collect();
                            if parts.len() >= 3 {
                                log::info!(
                                    "Full sync from master: repl_id={}, offset={}",
                                    parts[1],
                                    parts[2]
                                );
                            }

                            let snapshot_response = self.stream.next().await;
                            match snapshot_response {
                                Some(Ok(snapshot_data)) => {
                                    match decode_snapshot(snapshot_data) {
                                        Ok(entries) => {
                                            log::info!("Loading {} keys from master", entries.len());
                                            if let Err(e) = self.kv_store.load_snapshot(entries).await {
                                                log::error!("Failed to load snapshot: {e}");
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("Failed to decode snapshot: {e}");
                                            return;
                                        }
                                    }
                                }
                                other => {
                                    log::error!("Failed to receive snapshot from master: {:?}", other);
                                    return;
                                }
                            }
                        }
                        Some(Ok(RESP3Value::SimpleString(s))) if s == "CONTINUE" => {
                            log::info!("Partial sync from master (CONTINUE)");
                        }
                        other => {
                            log::error!("Unexpected PSYNC response: {:?}", other);
                            return;
                        }
                    }

                    self.conn_manager
                        .set_master(self.addr)
                        .await
                        .inspect_err(|err| log::error!("Failed to add master: {:?}", err))
                        .ok();
                }
            }
            ConnectionMessage::Shutdown => {
                self.receiver.close();
            }
        }
    }
}

/// Runs the connection actor.
async fn run_connection(mut connection: Connection, on_shutdown_complete: oneshot::Sender<()>) {
    log::info!("Connection established for {}", connection.addr);

    loop {
        tokio::select! {
            msg = connection.receiver.recv() => match msg {
                Some(msg) => connection.handle_message(msg).await,
                None => {
                    break;
                }
            },
            result = connection.stream.next() => {
                let request = match result {
                    Some(Ok(request)) => match decode_request(request) {
                        Ok(request) => request,
                        Err(err) => {
                            log::error!("Failed to decode request: {:?}", err);
                            continue;
                        }
                    }
                    Some(Err(err)) => {
                        log::error!("Failed to read from socket: {:?}", err);
                        break;
                    }
                    None => {
                        break;
                    }
                };

                log::info!("Received request: {}", request);

                let response = match request {
                    Request::Ping => RESP3Value::SimpleString("PONG".to_string()),
                    Request::Echo(value) => value,
                    Request::Set(key, value, ttl) => {
                        let expiration = ttl.as_ref().map(|expiration| Instant::now() + match expiration {
                            TTL::Seconds(ttl) => Duration::from_secs(*ttl),
                            TTL::Milliseconds(ttl) => Duration::from_millis(*ttl),
                        });

                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Set(key.clone(), value.clone(), ttl));
                        let set_future = connection.kv_store.set(key, value, expiration);

                        let (_broadcast_result, set_result) = tokio::join!(broadcast_future, set_future);

                        match set_result {
                            Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to set key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Get(key) => {
                        let res = connection.kv_store.get(key).await;

                        match res {
                            Ok(value) => value.unwrap_or(RESP3Value::Null),
                            Err(err) => {
                                log::error!("Failed to get key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Del(key) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Del(key.clone()));
                        let del_future = connection.kv_store.del(key);

                        let (_broadcast_res, del_res) = tokio::join!(broadcast_future, del_future);

                        match del_res {
                            Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to del key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::PSync(_repl_id, _offset) => {
                        let repl_id = connection
                            .conn_manager
                            .get_repl_id()
                            .await
                            .unwrap_or_else(|_| "?".to_string());

                        let current_offset = connection
                            .kv_store
                            .get_offset()
                            .await
                            .unwrap_or(0);

                        let fullresync = RESP3Value::SimpleString(
                            format!("FULLRESYNC {repl_id} {current_offset}")
                        );

                        if let Err(e) = connection.stream.send(fullresync).await {
                            log::error!("Failed to send FULLRESYNC: {e}");
                            continue;
                        }

                        let snapshot_data = connection
                            .kv_store
                            .snapshot()
                            .await
                            .unwrap_or_default();

                        log::info!("Sending snapshot with {} keys to replica {}", 
                                   snapshot_data.len(), connection.addr);

                        let snapshot_msg = encode_snapshot(&snapshot_data);
                        if let Err(e) = connection.stream.send(snapshot_msg).await {
                            log::error!("Failed to send snapshot: {e}");
                            continue;
                        }

                        connection
                            .conn_manager
                            .set_replica(connection.addr)
                            .await
                            .inspect_err(|err| log::error!("Failed to set replica: {:?}", err))
                            .ok();

                        continue;
                    }
                };

                if let ConnectionType::Master = connection.conn_type {
                    continue;
                }

                log::info!("Sending response: {response}");

                let _ = connection
                    .stream
                    .send(response)
                    .await
                    .inspect_err(|err| log::error!("Failed to send response: {:?}", err));
            }
            else => {
                break;
            }
        }
    }

    log::info!("Connection closed for {}", connection.addr);

    on_shutdown_complete.send(()).ok();
}

/// Represents a handle to a connection actor. The handle can be used to send messages to the actor.
/// The handle is clonable and can be shared across threads.
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    sender: mpsc::Sender<ConnectionMessage>,
}

impl ConnectionHandle {
    /// Creates a new connection handle and starts the connection actor.
    /// Returns a tuple containing the handle and a oneshot receiver that will be signalled when the
    /// connection actor has shutdown.
    pub fn new(
        stream: Framed<TcpStream, RESP3Codec>,
        addr: SocketAddr,
        kv_store: KVStoreHandle,
        conn_manager: ConnectionManagerHandle,
    ) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(128);
        let (on_shutdown_complete, shutdown_complete) = oneshot::channel();

        let connection = Connection::new(receiver, stream, addr, kv_store, conn_manager);
        tokio::spawn(run_connection(connection, on_shutdown_complete));
        (ConnectionHandle { addr, sender }, shutdown_complete)
    }

    pub async fn forward_request(&self, request: Request) -> Result<()> {
        let msg = ConnectionMessage::ForwardRequest { request };
        self.sender.send(msg).await?;
        Ok(())
    }

    pub fn try_forward_request(&self, request: Request) -> Result<(), tokio::sync::mpsc::error::TrySendError<ConnectionMessage>> {
        let msg = ConnectionMessage::ForwardRequest { request };
        self.sender.try_send(msg)
    }

    /// Sets the connection type of the connection actor. This is used to set the connection type of
    /// the connection actor to master or replica.
    pub async fn set_conn_type(&self, conn_type: ConnectionType) -> Result<()> {
        let msg = ConnectionMessage::SetConnType { conn_type };
        self.sender.send(msg).await?;
        Ok(())
    }

    /// Shuts down the connection actor.
    pub async fn shutdown(&self) -> Result<()> {
        let msg = ConnectionMessage::Shutdown;
        self.sender.send(msg).await?;
        Ok(())
    }
}

impl PartialEq for ConnectionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for ConnectionHandle {}
