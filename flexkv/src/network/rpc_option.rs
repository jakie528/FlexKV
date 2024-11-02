use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RPCOption {
    hard_ttl: Duration,

    pub(crate) snapshot_chunk_size: Option<usize>,
}

impl RPCOption {
    pub fn new(hard_ttl: Duration) -> Self {
        Self {
            hard_ttl,
            snapshot_chunk_size: None,
        }
    }

    pub fn soft_ttl(&self) -> Duration {
        self.hard_ttl * 3 / 4
    }

    pub fn hard_ttl(&self) -> Duration {
        self.hard_ttl
    }

    pub fn snapshot_chunk_size(&self) -> Option<usize> {
        self.snapshot_chunk_size
    }
}
