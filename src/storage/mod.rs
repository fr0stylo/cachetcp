use std::{collections::HashMap, fmt::Debug, sync::Arc};

use tokio::sync::Mutex;

use crate::wal::WriteAheadLog;

use self::storage::Storage;

type CacheStorage = Arc<Mutex<HashMap<String, Vec<u8>>>>;

pub mod storage;

pub trait Cache: Clone + Debug {
    fn write(&self, key: String, data: Vec<u8>) -> Option<Vec<u8>>;
    fn read(&self, key: String) -> Option<Vec<u8>>;
    fn keys(&self) -> Option<Vec<String>>;
    fn delete(&self, key: String) -> Option<Vec<u8>>;
}

pub struct WalCache {
    storage: Storage,
    wal: WriteAheadLog,
}

impl WalCache {
    pub fn new(wal: WriteAheadLog) -> Self {
        let s = Storage::new();
        Self { storage: s, wal }
    }

    
}
