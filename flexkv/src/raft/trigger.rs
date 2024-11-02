
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::error::Fatal;
use crate::raft::RaftInner;
use crate::RaftTypeConfig;

pub struct Trigger<'r, C>
where C: RaftTypeConfig
{
    raft_inner: &'r RaftInner<C>,
}

impl<'r, C> Trigger<'r, C>
where C: RaftTypeConfig
{
    pub(in crate::raft) fn new(raft_inner: &'r RaftInner<C>) -> Self {
        Self { raft_inner }
    }

    pub async fn elect(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Elect, "trigger_elect").await
    }

    pub async fn heartbeat(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Heartbeat, "trigger_heartbeat").await
    }

    pub async fn snapshot(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Snapshot, "trigger_snapshot").await
    }

    pub async fn purge_log(&self, upto: u64) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::PurgeLog { upto }, "purge_log").await
    }
}
