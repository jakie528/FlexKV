use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;

pub(crate) trait LogStateReader<NID: NodeId> {
    fn prev_log_id(&self, index: u64) -> Option<LogId<NID>> {
        if index == 0 {
            None
        } else {
            self.get_log_id(index - 1)
        }
    }

    fn has_log_id(&self, log_id: &LogId<NID>) -> bool {
        if log_id.index < self.committed().next_index() {
            return true;
        }

        if let Some(local) = self.get_log_id(log_id.index) {
            *log_id == local
        } else {
            false
        }
    }

    fn get_log_id(&self, index: u64) -> Option<LogId<NID>>;

    fn last_log_id(&self) -> Option<&LogId<NID>>;

    fn committed(&self) -> Option<&LogId<NID>>;

    fn io_applied(&self) -> Option<&LogId<NID>>;

    fn io_snapshot_last_log_id(&self) -> Option<&LogId<NID>>;

    fn io_purged(&self) -> Option<&LogId<NID>>;

    fn snapshot_last_log_id(&self) -> Option<&LogId<NID>>;

    fn purge_upto(&self) -> Option<&LogId<NID>>;

    fn last_purged_log_id(&self) -> Option<&LogId<NID>>;
}
