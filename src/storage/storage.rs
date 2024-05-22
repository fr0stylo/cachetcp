use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use super::{expirable::ExpirationController, CacheStorage};

#[derive(Debug, Clone)]
pub struct Storage {
    cc: CacheStorage,
    expiration: Arc<ExpirationController>,
}

impl Storage {
    pub fn new() -> Self {
        let cc = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
        let exc = Arc::new(ExpirationController::new());
        Self {
            cc,
            expiration: exc,
        }
    }

    pub async fn write(&self, key: &str, data: Vec<u8>) -> Option<Vec<u8>> {
        self.cc.lock().await.insert(key.to_owned(), data)
    }

    pub async fn write_ex(&self, key: String, data: Vec<u8>, exp: Duration) -> Option<Vec<u8>> {
        let (_, res) = tokio::join!(
            self.expiration
                .add_expiration(&key, Duration::from_secs(10)),
            self.write(&key, data),
        );

        res
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

    pub async fn expire(&self) {
        match self.expiration.wait().await {
            Some(key) => {
                println!("Expired: {:?}", key);
                self.delete(key.to_owned()).await;
            }
            None => {}
        }
    }
}
