use std::time::Duration;

use crate::OptionalSend;

pub struct Backoff {
    inner: Box<dyn Iterator<Item = Duration> + Send + 'static>,
}

impl Backoff {
    pub fn new(iter: impl Iterator<Item = Duration> + OptionalSend + 'static) -> Self {
        Self { inner: Box::new(iter) }
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
