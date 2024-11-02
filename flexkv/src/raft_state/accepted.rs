use crate::LeaderId;
use crate::LogId;
use crate::NodeId;

#[derive(Debug, Clone)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct Accepted<NID: NodeId> {
    leader_id: LeaderId<NID>,

    log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> Accepted<NID> {
    pub(crate) fn new(leader_id: LeaderId<NID>, log_id: Option<LogId<NID>>) -> Self {
        Self { leader_id, log_id }
    }
    
    #[allow(dead_code)]
    pub(crate) fn leader_id(&self) -> &LeaderId<NID> {
        &self.leader_id
    }

    pub(crate) fn last_accepted_log_id(&self, leader_id: &LeaderId<NID>) -> Option<&LogId<NID>> {
        if leader_id == &self.leader_id {
            self.log_id.as_ref()
        } else {
            None
        }
    }
}
