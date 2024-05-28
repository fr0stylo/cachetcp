use std::{collections::HashMap, sync::Arc, time::Duration};
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::proto::CommandMessage::DELETE;
use crate::proto::FrameMessage;

use super::CacheStorage;

#[derive(Debug, Clone, Eq, PartialOrd, PartialEq)]
struct Entry {
    expires_at: Option<Instant>,
    value: Vec<u8>,
    key: String,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        return self.expires_at.cmp(&other.expires_at).reverse();
    }

    fn max(self, other: Self) -> Self
        where
            Self: Sized,
    {
        std::cmp::max_by(self, other, Ord::cmp)
    }

    fn min(self, other: Self) -> Self
        where
            Self: Sized,
    {
        std::cmp::min_by(self, other, Ord::cmp)
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    cc: CacheStorage,
    stash: Arc<Mutex<HashMap<String, Entry>>>,
}

impl Storage {
    pub fn new() -> Self {
        let cc = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
        Self {
            cc,
            stash: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn write(&self, key: &str, data: Vec<u8>) -> Option<Vec<u8>> {
        let res = self.stash.lock().await.insert(key.to_owned(), Entry { key: key.clone().to_owned(), value: data.clone(), expires_at: None });
        println!("{:?}", res);
        return None;
    }

    pub async fn write_ex(&self, key: &str, data: Vec<u8>, exp: Duration) -> Option<Vec<u8>> {
        let res = self.stash.lock().await.insert(key.to_owned(), Entry { key: key.clone().to_owned(), value: data.clone(), expires_at: Some(Instant::now().add(exp)) });

        return None;
    }

    pub async fn read(&self, key: &str) -> Option<Vec<u8>> {
        match self.stash.lock().await.get(key.clone()) {
            Some(x) => Some(x.clone().value),
            None => None,
        }
    }

    pub async fn keys(&self) -> Option<Vec<String>> {
        Some(self.stash.lock().await.keys().map(|x| x.clone()).collect())
    }

    pub async fn delete(&self, key: &str) -> Option<Vec<u8>> {
        match self.stash.lock().await.remove(key.clone()) {
            None => None,
            Some(x) => Some(x.value)
        }
    }

    async fn wait(&self) -> Vec<String> {
        let mut g = self.stash.lock().await;
        let res = g.extract_if(|x, entry| {
            match entry.expires_at {
                None => { false }
                Some(x) => {
                    Instant::now() >= x
                }
            }
        });

        let keys: Vec<String> = res.into_iter().map(|(x, _)| { x }).collect();

        keys
    }

    pub async fn expire(&self, mut tx: &UnboundedSender<FrameMessage>) {
        self.wait().await.iter().for_each(|x| {
            tx.send(DELETE(x.clone()).into()).unwrap();
        })
    }
}
