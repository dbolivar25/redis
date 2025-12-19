use crate::common::codec::Request;
use crate::common::resp3::RESP3Value;
use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval_at, Instant, Interval};

pub type SnapshotEntry = (RESP3Value, RESP3Value, Option<Duration>);

const BACKLOG_MAX_ENTRIES: usize = 10_000;

#[derive(Clone)]
struct BacklogEntry {
    offset: u64,
    request: Request,
}

/// A key-value store that supports setting, getting, and deleting key-value pairs.
/// Each key-value pair can have an optional expiration time.
/// The key-value store periodically removes expired key-value pairs.
pub struct KVStore {
    receiver: mpsc::Receiver<KVStoreMessage>,
    active_expiration_interval: Interval,
    kv_store: HashMap<Key, Value>,
    offset: u64,
    backlog: VecDeque<BacklogEntry>,
}

type Key = RESP3Value;
type Value = (RESP3Value, Option<Instant>);

/// Messages that can be sent to the KVStore.
pub enum KVStoreMessage {
    Set {
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    },
    Get {
        key: Key,
        respond_to: oneshot::Sender<Option<RESP3Value>>,
    },
    Del {
        key: Key,
    },
    GetOffset {
        respond_to: oneshot::Sender<u64>,
    },
    Snapshot {
        respond_to: oneshot::Sender<Vec<SnapshotEntry>>,
    },
    GetBacklogFrom {
        from_offset: u64,
        respond_to: oneshot::Sender<Option<Vec<Request>>>,
    },
    Shutdown,
}

impl KVStore {
    /// Create a new KVStore. The KVStore will be initialized with the given key-value pairs.
    pub fn new(
        receiver: mpsc::Receiver<KVStoreMessage>,
        active_expiration_interval: Interval,
        kv_store: HashMap<Key, Value>,
    ) -> Self {
        KVStore {
            receiver,
            active_expiration_interval,
            kv_store,
            offset: 0,
            backlog: VecDeque::new(),
        }
    }

    fn handle_message(&mut self, msg: KVStoreMessage) {
        match msg {
            KVStoreMessage::Set { key, value, ttl } => {
                self.offset += 1;
                
                let ttl_for_backlog = ttl.map(|d| {
                    if d.as_secs() > 0 && d.subsec_millis() == 0 {
                        crate::common::codec::TTL::Seconds(d.as_secs())
                    } else {
                        crate::common::codec::TTL::Milliseconds(d.as_millis() as u64)
                    }
                });
                let request = Request::Set(key.clone(), value.0.clone(), ttl_for_backlog);
                self.backlog.push_back(BacklogEntry {
                    offset: self.offset,
                    request,
                });
                if self.backlog.len() > BACKLOG_MAX_ENTRIES {
                    self.backlog.pop_front();
                }
                
                self.kv_store.insert(key, value);
            }
            KVStoreMessage::Get { key, respond_to } => match self.kv_store.get(&key).cloned() {
                Some((_value, Some(expiry))) if expiry < Instant::now() => {
                    self.kv_store.remove(&key);
                    let _ = respond_to
                        .send(None)
                        .inspect_err(|err| log::error!("Failed to send response: {:?}", err));
                }
                value => {
                    let value = value.map(|(value, _)| value);
                    let _ = respond_to
                        .send(value)
                        .inspect_err(|err| log::error!("Failed to send response: {:?}", err));
                }
            },
            KVStoreMessage::Del { key } => {
                self.offset += 1;
                
                let request = Request::Del(key.clone());
                self.backlog.push_back(BacklogEntry {
                    offset: self.offset,
                    request,
                });
                if self.backlog.len() > BACKLOG_MAX_ENTRIES {
                    self.backlog.pop_front();
                }
                
                self.kv_store.remove(&key);
            }
            KVStoreMessage::GetOffset { respond_to } => {
                respond_to.send(self.offset).ok();
            }
            KVStoreMessage::Snapshot { respond_to } => {
                let now = Instant::now();
                let snapshot: Vec<SnapshotEntry> = self
                    .kv_store
                    .iter()
                    .filter_map(|(key, (value, expiry))| {
                        if let Some(exp) = expiry {
                            if *exp <= now {
                                return None;
                            }
                        }
                        let remaining_ttl = expiry.map(|e| e.saturating_duration_since(now));
                        Some((key.clone(), value.clone(), remaining_ttl))
                    })
                    .collect();
                respond_to.send(snapshot).ok();
            }
            KVStoreMessage::GetBacklogFrom { from_offset, respond_to } => {
                let result = if from_offset > self.offset {
                    None
                } else if self.backlog.is_empty() {
                    if from_offset == self.offset {
                        Some(vec![])
                    } else {
                        None
                    }
                } else {
                    let min_off = self.backlog.front().unwrap().offset;
                    if from_offset < min_off.saturating_sub(1) {
                        None
                    } else {
                        let commands: Vec<Request> = self.backlog
                            .iter()
                            .filter(|e| e.offset > from_offset)
                            .map(|e| e.request.clone())
                            .collect();
                        Some(commands)
                    }
                };
                respond_to.send(result).ok();
            }
            KVStoreMessage::Shutdown => {
                self.receiver.close();
            }
        }
    }

    /// Remove all expired key-value pairs.
    fn remove_expired(&mut self, instant: Instant) {
        self.kv_store
            .retain(|_, (_, expiry)| expiry.is_none_or(|expiry| expiry > instant));
    }
}

/// Run the KVStore until it is shut down. The KVStore will handle messages and remove expired key-value pairs.
/// When the KVStore is shut down, a message will be sent to the `on_shutdown_complete` sender.
/// The KVStore will not accept any more messages after it is shut down but will finish all of the
/// in-flight requests.
async fn run_kv_store(mut kv_store: KVStore, on_shutdown_complete: oneshot::Sender<()>) {
    log::info!("KV store started");

    loop {
        tokio::select! {
            msg = kv_store.receiver.recv() => match msg {
                Some(msg) => kv_store.handle_message(msg),
                None => break,
            },
            now = kv_store.active_expiration_interval.tick() => kv_store.remove_expired(now),
            else => {
                break;
            }
        }
    }

    log::info!("KV store shut down");

    on_shutdown_complete.send(()).ok();
}

/// A handle to the KVStore that can be used to set, get, and delete key-value pairs.
#[derive(Clone)]
pub struct KVStoreHandle {
    sender: mpsc::Sender<KVStoreMessage>,
}

impl KVStoreHandle {
    /// Create a new KVStoreHandle and a oneshot receiver that will be signalled when the KVStore is shut down.
    pub fn new() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(128);
        let (on_shutdown_complete, shutdown_complete) = oneshot::channel();
        let active_expiration_interval_period = Duration::from_millis(200);
        let active_expiration_interval = interval_at(
            Instant::now() + active_expiration_interval_period,
            active_expiration_interval_period,
        );
        let kv_store = HashMap::new();

        let kv_store = KVStore::new(receiver, active_expiration_interval, kv_store);
        tokio::spawn(run_kv_store(kv_store, on_shutdown_complete));
        (KVStoreHandle { sender }, shutdown_complete)
    }

    pub async fn set(
        &self,
        key: RESP3Value,
        value: RESP3Value,
        expiration: Option<Instant>,
    ) -> Result<()> {
        let ttl = expiration.map(|exp| exp.saturating_duration_since(Instant::now()));
        let value = (value, expiration);
        let msg = KVStoreMessage::Set { key, value, ttl };
        self.sender.send(msg).await?;
        Ok(())
    }

    /// Get the value for a key in the KVStore.
    pub async fn get(&self, key: Key) -> Result<Option<RESP3Value>> {
        let (respond_to, response) = oneshot::channel();
        let msg = KVStoreMessage::Get { key, respond_to };
        self.sender.send(msg).await?;
        response.await.map_err(Into::into)
    }

    pub async fn del(&self, key: Key) -> Result<()> {
        let msg = KVStoreMessage::Del { key };
        self.sender.send(msg).await?;
        Ok(())
    }

    pub async fn get_offset(&self) -> Result<u64> {
        let (respond_to, response) = oneshot::channel();
        let msg = KVStoreMessage::GetOffset { respond_to };
        self.sender.send(msg).await?;
        response.await.map_err(Into::into)
    }

    pub async fn get_backlog_from(&self, from_offset: u64) -> Result<Option<Vec<Request>>> {
        let (respond_to, response) = oneshot::channel();
        let msg = KVStoreMessage::GetBacklogFrom { from_offset, respond_to };
        self.sender.send(msg).await?;
        response.await.map_err(Into::into)
    }

    pub async fn snapshot(&self) -> Result<Vec<SnapshotEntry>> {
        let (respond_to, response) = oneshot::channel();
        let msg = KVStoreMessage::Snapshot { respond_to };
        self.sender.send(msg).await?;
        response.await.map_err(Into::into)
    }

    pub async fn load_snapshot(&self, entries: Vec<SnapshotEntry>) -> Result<()> {
        for (key, value, ttl) in entries {
            let expiration = ttl.map(|d| Instant::now() + d);
            self.set(key, value, expiration).await?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let msg = KVStoreMessage::Shutdown;
        self.sender.send(msg).await?;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn test_set_and_get() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value = RESP3Value::BulkString(b"test_value".to_vec());

    kv_store
        .set(key.clone(), value.clone(), None)
        .await
        .unwrap();

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, Some(value));

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_set_with_ttl_and_get() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value = RESP3Value::BulkString(b"test_value".to_vec());

    kv_store
        .set(
            key.clone(),
            value.clone(),
            Some(Instant::now() + Duration::from_millis(45)),
        )
        .await
        .unwrap();

    let result = kv_store.get(key.clone()).await.unwrap();
    assert_eq!(result, Some(value));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, None);

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_del() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value = RESP3Value::BulkString(b"test_value".to_vec());

    kv_store.set(key.clone(), value, None).await.unwrap();
    kv_store.del(key.clone()).await.unwrap();

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, None);

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_multiple_sets() {
    let (kv_store, _) = KVStoreHandle::new();

    let key1 = RESP3Value::BulkString(b"key1".to_vec());
    let value1 = RESP3Value::BulkString(b"value1".to_vec());
    let key2 = RESP3Value::BulkString(b"key2".to_vec());
    let value2 = RESP3Value::BulkString(b"value2".to_vec());

    kv_store
        .set(key1.clone(), value1.clone(), None)
        .await
        .unwrap();
    kv_store
        .set(key2.clone(), value2.clone(), None)
        .await
        .unwrap();

    let result1 = kv_store.get(key1).await.unwrap();
    let result2 = kv_store.get(key2).await.unwrap();

    assert_eq!(result1, Some(value1));
    assert_eq!(result2, Some(value2));

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_overwrite_value() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value1 = RESP3Value::BulkString(b"value1".to_vec());
    let value2 = RESP3Value::BulkString(b"value2".to_vec());

    kv_store.set(key.clone(), value1, None).await.unwrap();
    kv_store
        .set(key.clone(), value2.clone(), None)
        .await
        .unwrap();

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, Some(value2));

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_get_non_existent_key() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"non_existent_key".to_vec());

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, None);

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_del_non_existent_key() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"non_existent_key".to_vec());

    // Deleting a non-existent key should not cause an error
    kv_store.del(key).await.unwrap();

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_ttl_milliseconds() {
    let (kv_store, _) = KVStoreHandle::new();

    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value = RESP3Value::BulkString(b"test_value".to_vec());

    kv_store
        .set(
            key.clone(),
            value.clone(),
            Some(Instant::now() + Duration::from_millis(15)),
        )
        .await
        .unwrap();

    let result = kv_store.get(key.clone()).await.unwrap();
    assert_eq!(result, Some(value));

    tokio::time::sleep(Duration::from_millis(20)).await;

    let result = kv_store.get(key).await.unwrap();
    assert_eq!(result, None);

    kv_store.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_shutdown() {
    let (kv_store, shutdown_complete) = KVStoreHandle::new();

    kv_store.shutdown().await.unwrap();

    // Wait for the shutdown to complete
    shutdown_complete.await.unwrap();

    // Attempting to use the KVStore after shutdown should result in an error
    let key = RESP3Value::BulkString(b"test_key".to_vec());
    let value = RESP3Value::BulkString(b"test_value".to_vec());

    let result = kv_store.set(key.clone(), value, None).await;
    assert!(result.is_err());

    let result = kv_store.get(key).await;
    assert!(result.is_err());
}
