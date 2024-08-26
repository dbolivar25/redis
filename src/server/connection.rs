use super::{connection_manager::ConnectionManagerHandle, kv_store::KVStoreHandle};
use crate::common::{
    protocol::{Request, ServerProtoCodec},
    resp3::RESP3Value,
};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

pub struct Connection {
    receiver: mpsc::Receiver<ConnectionMessage>,
    stream: Framed<TcpStream, ServerProtoCodec>,
    addr: SocketAddr,
    kv_store: KVStoreHandle,
    conn_manager: ConnectionManagerHandle,
}

#[derive(Debug)]
pub enum ConnectionMessage {
    ForwardRequest { request: Request },
    Shutdown,
}

impl Connection {
    pub fn new(
        receiver: mpsc::Receiver<ConnectionMessage>,
        stream: TcpStream,
        addr: SocketAddr,
        kv_store: KVStoreHandle,
        conn_manager: ConnectionManagerHandle,
    ) -> Self {
        let stream = Framed::new(stream, ServerProtoCodec);

        Connection {
            receiver,
            stream,
            addr,
            kv_store,
            conn_manager,
        }
    }

    async fn handle_message(&mut self, msg: ConnectionMessage) {
        match msg {
            ConnectionMessage::ForwardRequest { request } => {
                log::info!("Forwarding request: {}", request);

                todo!()
            }
            ConnectionMessage::Shutdown {} => {
                self.receiver.close();
            }
        }
    }
}

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
                    Some(Ok(request)) => request,
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
                    Request::Set(key, value, expiration) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Set(key.clone(), value.clone(), expiration.clone()));
                        let set_future = connection.kv_store.set(key, value, expiration);

                        let (_brodcast_result, set_result) = tokio::join!(broadcast_future, set_future);

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

                        let res = tokio::try_join!(broadcast_future, del_future);

                        match res {
                            Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to del key: {:?}", err);
                                continue;
                            }
                        }
                    }
                };

                log::info!("Sending response: {}", response);

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

#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    sender: mpsc::Sender<ConnectionMessage>,
}

impl ConnectionHandle {
    pub fn new(
        stream: TcpStream,
        addr: SocketAddr,
        kv_store: KVStoreHandle,
        conn_manager: ConnectionManagerHandle,
    ) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(16);
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
