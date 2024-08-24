use crate::common::resp3::RESP3Value;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct RedisService {
    kv_store: Arc<DashMap<RESP3Value, (RESP3Value, Instant)>>,
}

impl RedisService {
    pub fn new() -> Self {
        RedisService {
            kv_store: Arc::new(DashMap::new()),
        }
    }

    fn clean_expired_keys(&self) {
        self.kv_store
            .retain(|_, (_, expiry)| *expiry > Instant::now());
    }
}
