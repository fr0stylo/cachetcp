use std::{
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::time::Instant;

#[derive(PartialEq, Eq, Debug, PartialOrd)]
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

type PriotiryQueue = BinaryHeap<Expirable>;

impl Future for &Expirable {
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

pub struct ExpirationController {
    queue: PriotiryQueue,
}

impl ExpirationController {
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }

    pub fn add_expiration(&mut self, key: &str, exp: Duration) {
        self.queue
            .push(Expirable::new(key.to_owned(), Instant::now() + exp));
    }

    pub async fn wait(&mut self) -> Option<String> {
        match self.queue.peek() {
            Some(next) => {
                let res = next.await;
                self.queue.pop();
                Some(res)
            }
            None => None,
        }
    }
}
