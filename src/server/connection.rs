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
    master_repl_id: Option<String>,
    replica_offset: u64,
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
            master_repl_id: None,
            replica_offset: 0,
        }
    }

    /// Handles a message sent to the connection actor.
    async fn handle_message(&mut self, msg: ConnectionMessage) {
        match msg {
            ConnectionMessage::ForwardRequest { request } => {
                log::info!("Forwarding request: {}", request);

                let resp3 = encode_request(&request);

                match tokio::time::timeout(Duration::from_secs(5), self.stream.send(resp3)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        log::error!("Write error to {}: {e}, closing connection", self.addr);
                        self.receiver.close();
                    }
                    Err(_) => {
                        log::error!("Write timeout to {}, closing connection", self.addr);
                        self.receiver.close();
                    }
                }
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

                    let (repl_id, offset) = match &self.master_repl_id {
                        Some(id) => (id.clone(), self.replica_offset.to_string()),
                        None => ("?".to_string(), "-1".to_string()),
                    };

                    log::info!("Sending PSYNC {} {}", repl_id, offset);

                    let psync_req = encode_request(&Request::PSync(
                        RESP3Value::BulkString(repl_id.into_bytes()),
                        RESP3Value::BulkString(offset.into_bytes()),
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
                                self.master_repl_id = Some(parts[1].to_string());
                                self.replica_offset = parts[2].parse().unwrap_or(0);
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

                let is_from_master = matches!(connection.conn_type, ConnectionType::Master);

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
                            .broadcast(Request::Set(key.clone(), value.clone(), ttl.clone()));
                        let set_future = connection.kv_store.set(key, value, expiration);

                        let (_broadcast_result, set_result) = tokio::join!(broadcast_future, set_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

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

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match del_res {
                            Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to del key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::PSync(repl_id_val, offset_val) => {
                        let our_repl_id = connection
                            .conn_manager
                            .get_repl_id()
                            .await
                            .unwrap_or_else(|_| "?".to_string());

                        let current_offset = connection
                            .kv_store
                            .get_offset()
                            .await
                            .unwrap_or(0);

                        let replica_repl_id = match &repl_id_val {
                            RESP3Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                            _ => "?".to_string(),
                        };
                        let replica_offset: i64 = match &offset_val {
                            RESP3Value::BulkString(b) => {
                                String::from_utf8_lossy(b).parse().unwrap_or(-1)
                            }
                            _ => -1,
                        };

                        let can_partial = replica_repl_id == our_repl_id && replica_offset >= 0;

                        if can_partial {
                            let from_offset = replica_offset as u64;
                            if let Ok(Some(backlog)) = connection.kv_store.get_backlog_from(from_offset).await {
                                log::info!(
                                    "Partial sync for replica {}: sending {} commands from offset {}",
                                    connection.addr, backlog.len(), from_offset
                                );

                                let continue_msg = RESP3Value::SimpleString("CONTINUE".to_string());
                                if let Err(e) = connection.stream.send(continue_msg).await {
                                    log::error!("Failed to send CONTINUE: {e}");
                                    continue;
                                }

                                for cmd in backlog {
                                    let encoded = encode_request(&cmd);
                                    if let Err(e) = connection.stream.send(encoded).await {
                                        log::error!("Failed to send backlog command: {e}");
                                        break;
                                    }
                                }

                                connection
                                    .conn_manager
                                    .set_replica(connection.addr)
                                    .await
                                    .inspect_err(|err| log::error!("Failed to set replica: {:?}", err))
                                    .ok();

                                continue;
                            }
                        }

                        log::info!(
                            "Full sync for replica {}: repl_id match={}, offset={}",
                            connection.addr, replica_repl_id == our_repl_id, replica_offset
                        );

                        let fullresync = RESP3Value::SimpleString(
                            format!("FULLRESYNC {our_repl_id} {current_offset}")
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
                    Request::Incr(key) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Incr(key.clone()));
                        let incr_future = connection.kv_store.incr_by(key, 1);

                        let (_broadcast_res, incr_res) = tokio::join!(broadcast_future, incr_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match incr_res {
                            Ok(Ok(value)) => RESP3Value::Integer(value),
                            Ok(Err(e)) => RESP3Value::SimpleError(format!("ERR {e}")),
                            Err(err) => {
                                log::error!("Failed to incr key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Decr(key) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Decr(key.clone()));
                        let decr_future = connection.kv_store.incr_by(key, -1);

                        let (_broadcast_res, decr_res) = tokio::join!(broadcast_future, decr_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match decr_res {
                            Ok(Ok(value)) => RESP3Value::Integer(value),
                            Ok(Err(e)) => RESP3Value::SimpleError(format!("ERR {e}")),
                            Err(err) => {
                                log::error!("Failed to decr key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::IncrBy(key, delta) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::IncrBy(key.clone(), delta));
                        let incr_future = connection.kv_store.incr_by(key, delta);

                        let (_broadcast_res, incr_res) = tokio::join!(broadcast_future, incr_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match incr_res {
                            Ok(Ok(value)) => RESP3Value::Integer(value),
                            Ok(Err(e)) => RESP3Value::SimpleError(format!("ERR {e}")),
                            Err(err) => {
                                log::error!("Failed to incrby key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::DecrBy(key, delta) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::DecrBy(key.clone(), delta));
                        let decr_future = connection.kv_store.incr_by(key, -delta);

                        let (_broadcast_res, decr_res) = tokio::join!(broadcast_future, decr_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match decr_res {
                            Ok(Ok(value)) => RESP3Value::Integer(value),
                            Ok(Err(e)) => RESP3Value::SimpleError(format!("ERR {e}")),
                            Err(err) => {
                                log::error!("Failed to decrby key: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Append(key, value) => {
                        let append_value = match &value {
                            RESP3Value::BulkString(b) => b.clone(),
                            RESP3Value::SimpleString(s) => s.as_bytes().to_vec(),
                            _ => vec![],
                        };

                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Append(key.clone(), value));
                        let append_future = connection.kv_store.append(key, append_value);

                        let (_broadcast_res, append_res) = tokio::join!(broadcast_future, append_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match append_res {
                            Ok(len) => RESP3Value::Integer(len as i64),
                            Err(err) => {
                                log::error!("Failed to append: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::StrLen(key) => {
                        match connection.kv_store.strlen(key).await {
                            Ok(len) => RESP3Value::Integer(len as i64),
                            Err(err) => {
                                log::error!("Failed to strlen: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Exists(keys) => {
                        match connection.kv_store.exists(keys).await {
                            Ok(count) => RESP3Value::Integer(count as i64),
                            Err(err) => {
                                log::error!("Failed to exists: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Keys(pattern) => {
                        let pattern_str = match &pattern {
                            RESP3Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                            RESP3Value::SimpleString(s) => s.clone(),
                            _ => "*".to_string(),
                        };

                        match connection.kv_store.keys(pattern_str).await {
                            Ok(keys) => RESP3Value::Array(keys),
                            Err(err) => {
                                log::error!("Failed to keys: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Rename(key, new_key) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Rename(key.clone(), new_key.clone()));
                        let rename_future = connection.kv_store.rename(key, new_key);

                        let (_broadcast_res, rename_res) = tokio::join!(broadcast_future, rename_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match rename_res {
                            Ok(Ok(())) => RESP3Value::SimpleString("OK".to_string()),
                            Ok(Err(e)) => RESP3Value::SimpleError(format!("ERR {e}")),
                            Err(err) => {
                                log::error!("Failed to rename: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Type(key) => {
                        match connection.kv_store.get_type(key).await {
                            Ok(type_str) => RESP3Value::SimpleString(type_str),
                            Err(err) => {
                                log::error!("Failed to type: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Expire(key, secs) => {
                        let expire_at = Instant::now() + Duration::from_secs(secs);

                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Expire(key.clone(), secs));
                        let expire_future = connection.kv_store.set_expire(key, expire_at);

                        let (_broadcast_res, expire_res) = tokio::join!(broadcast_future, expire_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match expire_res {
                            Ok(true) => RESP3Value::Integer(1),
                            Ok(false) => RESP3Value::Integer(0),
                            Err(err) => {
                                log::error!("Failed to expire: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::PExpire(key, ms) => {
                        let expire_at = Instant::now() + Duration::from_millis(ms);

                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::PExpire(key.clone(), ms));
                        let expire_future = connection.kv_store.set_expire(key, expire_at);

                        let (_broadcast_res, expire_res) = tokio::join!(broadcast_future, expire_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match expire_res {
                            Ok(true) => RESP3Value::Integer(1),
                            Ok(false) => RESP3Value::Integer(0),
                            Err(err) => {
                                log::error!("Failed to pexpire: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::ExpireAt(key, timestamp) => {
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let secs_from_now = timestamp.saturating_sub(now_unix);
                        let expire_at = Instant::now() + Duration::from_secs(secs_from_now);

                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::ExpireAt(key.clone(), timestamp));
                        let expire_future = connection.kv_store.set_expire(key, expire_at);

                        let (_broadcast_res, expire_res) = tokio::join!(broadcast_future, expire_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match expire_res {
                            Ok(true) => RESP3Value::Integer(1),
                            Ok(false) => RESP3Value::Integer(0),
                            Err(err) => {
                                log::error!("Failed to expireat: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Ttl(key) => {
                        match connection.kv_store.get_ttl(key).await {
                            Ok(None) => RESP3Value::Integer(-2),
                            Ok(Some(None)) => RESP3Value::Integer(-1),
                            Ok(Some(Some(duration))) => RESP3Value::Integer(duration.as_secs() as i64),
                            Err(err) => {
                                log::error!("Failed to ttl: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::PTtl(key) => {
                        match connection.kv_store.get_ttl(key).await {
                            Ok(None) => RESP3Value::Integer(-2),
                            Ok(Some(None)) => RESP3Value::Integer(-1),
                            Ok(Some(Some(duration))) => RESP3Value::Integer(duration.as_millis() as i64),
                            Err(err) => {
                                log::error!("Failed to pttl: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::Persist(key) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::Persist(key.clone()));
                        let persist_future = connection.kv_store.persist(key);

                        let (_broadcast_res, persist_res) = tokio::join!(broadcast_future, persist_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match persist_res {
                            Ok(true) => RESP3Value::Integer(1),
                            Ok(false) => RESP3Value::Integer(0),
                            Err(err) => {
                                log::error!("Failed to persist: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::MGet(keys) => {
                        match connection.kv_store.mget(keys).await {
                            Ok(values) => {
                                let arr: Vec<RESP3Value> = values
                                    .into_iter()
                                    .map(|v| v.unwrap_or(RESP3Value::Null))
                                    .collect();
                                RESP3Value::Array(arr)
                            }
                            Err(err) => {
                                log::error!("Failed to mget: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::MSet(pairs) => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::MSet(pairs.clone()));
                        let mset_future = connection.kv_store.mset(pairs);

                        let (_broadcast_res, mset_res) = tokio::join!(broadcast_future, mset_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match mset_res {
                            Ok(()) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to mset: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::DbSize => {
                        match connection.kv_store.dbsize().await {
                            Ok(size) => RESP3Value::Integer(size as i64),
                            Err(err) => {
                                log::error!("Failed to dbsize: {:?}", err);
                                continue;
                            }
                        }
                    }
                    Request::FlushDb => {
                        let broadcast_future = connection
                            .conn_manager
                            .broadcast(Request::FlushDb);
                        let flush_future = connection.kv_store.flushdb();

                        let (_broadcast_res, flush_res) = tokio::join!(broadcast_future, flush_future);

                        if is_from_master {
                            connection.replica_offset += 1;
                        }

                        match flush_res {
                            Ok(()) => RESP3Value::SimpleString("OK".to_string()),
                            Err(err) => {
                                log::error!("Failed to flushdb: {:?}", err);
                                continue;
                            }
                        }
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
        let (sender, receiver) = mpsc::channel(1024);
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
