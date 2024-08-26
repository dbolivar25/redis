use crate::common::{protocol::TTL, resp3::RESP3Value};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval_at, Duration, Instant, Interval};

pub struct KVStore {
    receiver: mpsc::Receiver<KVStoreMessage>,
    active_expiration_interval: Interval,
    kv_store: HashMap<Key, Value>,
}

type Key = RESP3Value;
type Value = (RESP3Value, Option<Instant>);

#[derive(Debug)]
pub enum KVStoreMessage {
    Set {
        key: Key,
        value: Value,
    },
    Get {
        key: Key,
        respond_to: oneshot::Sender<Option<RESP3Value>>,
    },
    Del {
        key: Key,
    },
}

impl KVStore {
    pub fn new(
        receiver: mpsc::Receiver<KVStoreMessage>,
        active_expiration_interval: Interval,
    ) -> Self {
        KVStore {
            receiver,
            active_expiration_interval,
            kv_store: HashMap::new(),
        }
    }

    fn handle_message(&mut self, msg: KVStoreMessage) {
        match msg {
            KVStoreMessage::Set { key, value } => {
                let _ = self.kv_store.insert(key, value);
            }
            KVStoreMessage::Get { key, respond_to } => {
                let value = self.kv_store.get(&key).cloned();

                if let Some((_, Some(expiry))) = value {
                    if expiry < Instant::now() {
                        self.kv_store.remove(&key);
                    }
                }

                let value = value.map(|(value, _)| value);
                let _ = respond_to
                    .send(value)
                    .inspect_err(|err| log::error!("Failed to send response: {:?}", err));
            }
            KVStoreMessage::Del { key } => {
                let _ = self.kv_store.remove(&key);
            }
        }
    }

    fn remove_expired(&mut self, now: Instant) {
        self.kv_store
            .retain(|_, (_, expiry)| expiry.map_or(true, |expiry| expiry > now));
    }
}

async fn run_kv_store(mut kv_store: KVStore) {
    loop {
        tokio::select! {
            msg = kv_store.receiver.recv() => match msg {
                Some(msg) => kv_store.handle_message(msg),
                None => break,
            },
            now = kv_store.active_expiration_interval.tick() => kv_store.remove_expired(now),
        }
    }
}

#[derive(Clone)]
pub struct KVStoreHandle {
    sender: mpsc::Sender<KVStoreMessage>,
}

impl KVStoreHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let active_expiration_interval_period = Duration::from_secs(1);
        let active_expiration_interval = interval_at(
            Instant::now() + active_expiration_interval_period,
            active_expiration_interval_period,
        );

        let kv_store = KVStore::new(receiver, active_expiration_interval);
        tokio::spawn(run_kv_store(kv_store));
        Self { sender }
    }

    pub async fn set(&self, key: RESP3Value, value: RESP3Value, ttl: Option<TTL>) -> Result<()> {
        let value = (
            value,
            ttl.map(|ttl| match ttl {
                TTL::Seconds(ttl) => Instant::now() + Duration::from_secs(ttl),
                TTL::Milliseconds(ttl) => Instant::now() + Duration::from_millis(ttl),
            }),
        );
        let msg = KVStoreMessage::Set { key, value };
        self.sender.send(msg).await?;
        Ok(())
    }

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
}
