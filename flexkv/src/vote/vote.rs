use std::cmp::Ordering;
use std::fmt::Formatter;

use crate::vote::leader_id::CommittedLeaderId;
use crate::LeaderId;
use crate::MessageSummary;
use crate::NodeId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct Vote<NID: NodeId> {
    pub leader_id: LeaderId<NID>,

    pub committed: bool,
}

impl<NID: NodeId> PartialOrd for Vote<NID> {
    #[inline]
    fn partial_cmp(&self, other: &Vote<NID>) -> Option<Ordering> {
        match PartialOrd::partial_cmp(&self.leader_id, &other.leader_id) {
            Some(Ordering::Equal) => PartialOrd::partial_cmp(&self.committed, &other.committed),
            None => {
                match (self.committed, other.committed) {
                    (false, false) => None,
                    (true, false) => Some(Ordering::Greater),
                    (false, true) => Some(Ordering::Less),
                    (true, true) => {
                        unreachable!("two incomparable leaders can not be both committed: {} {}", self, other)
                    }
                }
            }
            cmp => cmp,
        }
    }
}

impl<NID: NodeId> std::fmt::Display for Vote<NID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.leader_id,
            if self.is_committed() {
                "committed"
            } else {
                "uncommitted"
            }
        )
    }
}

impl<NID: NodeId> MessageSummary<Vote<NID>> for Vote<NID> {
    fn summary(&self) -> String {
        format!("{}", self)
    }
}

impl<NID: NodeId> Vote<NID> {
    pub fn new(term: u64, node_id: NID) -> Self {
        Self {
            leader_id: LeaderId::new(term, node_id),
            committed: false,
        }
    }

    pub fn new_committed(term: u64, node_id: NID) -> Self {
        Self {
            leader_id: LeaderId::new(term, node_id),
            committed: true,
        }
    }

    pub fn commit(&mut self) {
        self.committed = true
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }

    pub fn leader_id(&self) -> &LeaderId<NID> {
        &self.leader_id
    }

    pub(crate) fn committed_leader_id(&self) -> Option<CommittedLeaderId<NID>> {
        if self.is_committed() || self.leader_id().term == 0 {
            Some(self.leader_id().to_committed())
        } else {
            None
        }
    }

    pub(crate) fn is_same_leader(&self, leader_id: &CommittedLeaderId<NID>) -> bool {
        self.leader_id().is_same_as_committed(leader_id)
    }
}
