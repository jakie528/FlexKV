use std::fmt;

use crate::NodeId;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct LeaderId<NID>
where NID: NodeId
{
    pub term: u64,
    pub node_id: NID,
}

impl<NID: NodeId> LeaderId<NID> {
    pub fn new(term: u64, node_id: NID) -> Self {
        Self { term, node_id }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn voted_for(&self) -> Option<NID> {
        Some(self.node_id.clone())
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_committed(&self) -> CommittedLeaderId<NID> {
        self.clone()
    }

    pub(crate) fn is_same_as_committed(&self, other: &CommittedLeaderId<NID>) -> bool {
        self == other
    }
}

impl<NID: NodeId> fmt::Display for LeaderId<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}-N{}", self.term, self.node_id)
    }
}

pub type CommittedLeaderId<NID> = LeaderId<NID>;
