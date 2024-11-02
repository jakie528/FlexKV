use crate::CommittedLeaderId;
use crate::LogId;
use crate::NodeId;

pub trait RaftLogId<NID: NodeId> {
    fn leader_id(&self) -> &CommittedLeaderId<NID> {
        self.get_log_id().committed_leader_id()
    }

    fn get_log_id(&self) -> &LogId<NID>;

    fn set_log_id(&mut self, log_id: &LogId<NID>);
}
