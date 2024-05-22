use std::{
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    sync::Mutex,
    time::{timeout, Instant},
};

#[derive(PartialEq, Eq, Debug, PartialOrd, Clone)]
struct Expirable {
    id: String,
    expires_at: Instant,
}

impl Ord for Expirable {
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

impl Expirable {
    fn new(id: String, expires_at: Instant) -> Self {
        Self { id, expires_at }
    }
}

type PriotiryQueue = Arc<Mutex<Vec<Expirable>>>;

impl Future for Expirable {
    type Output = String;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<String> {
        if Instant::now() >= self.expires_at {
            Poll::Ready(self.id.clone())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpirationController {
    queue: PriotiryQueue,
}

// pub trait ExpirationHandler {
//     fn new() -> Self;

//     async fn add_expiration(&self, key: &str, exp: Duration);

//     async fn wait(&self) -> Option<String>;
// }

impl ExpirationController {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_expiration(&self, key: &str, exp: Duration) {
        self.queue
            .lock()
            .await
            .push(Expirable::new(key.to_owned(), Instant::now() + exp));
    }

    pub async fn wait(&self) -> Option<String> {
        let mut g = self.queue.lock().await;
        let mut q = BinaryHeap::from_iter(g.iter());
        let item = q.pop();
        match item {
            Some(item) => match timeout(Duration::from_millis(1), item.clone()).await {
                Ok(x) => {
                    *g = q.into_iter().map(|x| x.clone()).collect();
                    Some(x)
                }
                Err(_) => None,
            },
            None => None,
        }
    }
}
