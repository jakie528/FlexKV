
pub(crate) struct Balancer {
    total: u64,

    raft_msg: u64,
}

impl Balancer {
    pub(crate) fn new(total: u64) -> Self {
        Self {
            total,
            raft_msg: total / 10,
        }
    }

    pub(crate) fn raft_msg(&self) -> u64 {
        self.raft_msg
    }

    pub(crate) fn notify(&self) -> u64 {
        self.total - self.raft_msg
    }

    pub(crate) fn increase_notify(&mut self) {
        self.raft_msg = self.raft_msg * 15 / 16;
        if self.raft_msg == 0 {
            self.raft_msg = 1;
        }
    }

    pub(crate) fn increase_raft_msg(&mut self) {
        self.raft_msg = self.raft_msg * 17 / 16;

        if self.raft_msg > self.total / 2 {
            self.raft_msg = self.total / 2;
        }
    }
}
