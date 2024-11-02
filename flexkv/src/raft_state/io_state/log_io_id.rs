use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::CommittedLeaderId;
use crate::LogId;
use crate::NodeId;

#[derive(Debug, Clone, Copy)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct LogIOId<NID: NodeId> {
    pub(crate) committed_leader_id: CommittedLeaderId<NID>,

    pub(crate) log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> fmt::Display for LogIOId<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "by_leader({}):{}", self.committed_leader_id, self.log_id.display())
    }
}

impl<NID: NodeId> LogIOId<NID> {
    pub(crate) fn new(committed_leader_id: impl Into<CommittedLeaderId<NID>>, log_id: Option<LogId<NID>>) -> Self {
        Self {
            committed_leader_id: committed_leader_id.into(),
            log_id,
        }
    }
}
