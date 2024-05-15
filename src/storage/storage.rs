use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use super::CacheStorage;

#[derive(Debug, Clone)]
pub struct Storage {
    cc: CacheStorage,
}

impl Storage {
    pub fn new() -> Self {
        let cc = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
        Self { cc }
    }

    pub async fn write(&self, key: String, data: Vec<u8>) -> Option<Vec<u8>> {
        self.cc.lock().await.insert(key, data)
    }

    pub async fn read(&self, key: String) -> Option<Vec<u8>> {
        match self.cc.lock().await.get(&key) {
            Some(x) => Some(x.to_owned()),
            None => None,
        }
    }

    pub async fn keys(&self) -> Option<Vec<String>> {
        Some(self.cc.lock().await.keys().map(|x| x.clone()).collect())
    }

    pub async fn delete(&self, key: String) -> Option<Vec<u8>> {
        self.cc.lock().await.remove(&key)
    }
}
