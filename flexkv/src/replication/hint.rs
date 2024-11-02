
#[derive(Clone, Debug, Default)]
pub(crate) struct ReplicationHint {
    n: u64,

    ttl: u64,
}

impl ReplicationHint {
    pub(crate) fn new(n: u64, ttl: u64) -> Self {
        Self { n, ttl }
    }

    pub(crate) fn get(&mut self) -> Option<u64> {
        if self.ttl > 0 {
            self.ttl -= 1;
            Some(self.n)
        } else {
            None
        }
    }
}
