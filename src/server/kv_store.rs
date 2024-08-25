use crate::common::{protocol::TTL, resp3::RESP3Value};
use anyhow::Result;
use std::time::Instant;
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, oneshot};

pub struct KVStore {
    receiver: mpsc::Receiver<KVStoreMessage>,
    kv_store: HashMap<Key, Value>,
}

type Key = RESP3Value;
type Value = (RESP3Value, Option<Instant>);

pub enum KVStoreMessage {
    Set {
        key: Key,
        value: Value,
        // respond_to: oneshot::Sender<()>,
    },
    Get {
        key: Key,
        respond_to: oneshot::Sender<Option<RESP3Value>>,
    },
    Del {
        key: Key,
        // respond_to: oneshot::Sender<Option<RESP3Value>>,
    },
}

impl KVStore {
    pub fn new(receiver: mpsc::Receiver<KVStoreMessage>) -> Self {
        KVStore {
            receiver,
            kv_store: HashMap::new(),
        }
    }

    fn handle_message(&mut self, msg: KVStoreMessage) {
        match msg {
            KVStoreMessage::Set {
                key,
                value,
                // respond_to,
            } => {
                let _old_value = self.kv_store.insert(key, value);
                // let _ = respond_to
                //     .send(())
                //     .inspect_err(|err| eprintln!("Failed to send response: {:?}", err));
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
                    .inspect_err(|err| eprintln!("Failed to send response: {:?}", err));
            }
            KVStoreMessage::Del {
                key,
                // respond_to
            } => {
                let _old_value = self.kv_store.remove(&key);
                // let old_value = old_value.map(|(value, _)| value);
                // let _ = respond_to
                //     .send(old_value)
                //     .inspect_err(|err| eprintln!("Failed to send response: {:?}", err));
            }
        }
    }
}

async fn run_kv_store(mut kv_store: KVStore) {
    while let Some(msg) = kv_store.receiver.recv().await {
        kv_store.handle_message(msg);
    }
}

#[derive(Clone)]
pub struct KVStoreHandle {
    sender: mpsc::Sender<KVStoreMessage>,
}

impl KVStoreHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let kv_store = KVStore::new(receiver);
        tokio::spawn(run_kv_store(kv_store));
        Self { sender }
    }

    pub async fn set(&self, key: RESP3Value, value: RESP3Value, ttl: TTL) -> Result<()> {
        // let (send, recv) = oneshot::channel();
        let value = (
            value,
            match ttl {
                TTL::Milliseconds(ms) => Some(Instant::now() + Duration::from_millis(ms)),
                TTL::Seconds(s) => Some(Instant::now() + Duration::from_secs(s)),
                TTL::Persist => None,
            },
        );
        let msg = KVStoreMessage::Set {
            key,
            value,
            // respond_to: send,
        };
        self.sender.send(msg).await?;
        // recv.await?;
        Ok(())
    }

    pub async fn get(&self, key: Key) -> Result<Option<RESP3Value>> {
        let (send, recv) = oneshot::channel();
        let msg = KVStoreMessage::Get {
            key,
            respond_to: send,
        };
        self.sender.send(msg).await?;
        Ok(recv.await?)
    }

    pub async fn del(&self, key: Key) -> Result<()> {
        // let (send, recv) = oneshot::channel();
        let msg = KVStoreMessage::Del {
            key,
            // respond_to: send,
        };
        self.sender.send(msg).await?;
        // recv.await?;
        Ok(())
    }
}
