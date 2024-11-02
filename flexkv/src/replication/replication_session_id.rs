use std::fmt::Display;
use std::fmt::Formatter;

use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::Vote;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplicationSessionId<NID: NodeId> {
    pub(crate) vote: Vote<NID>,

    pub(crate) membership_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> Display for ReplicationSessionId<NID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.vote, self.membership_log_id.summary())
    }
}

impl<NID: NodeId> ReplicationSessionId<NID> {
    pub(crate) fn new(vote: Vote<NID>, membership_log_id: Option<LogId<NID>>) -> Self {
        Self {
            vote,
            membership_log_id,
        }
    }

    pub(crate) fn vote_ref(&self) -> &Vote<NID> {
        &self.vote
    }
}
