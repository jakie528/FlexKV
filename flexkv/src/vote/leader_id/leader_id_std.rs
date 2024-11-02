use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use crate::display_ext::DisplayOptionExt;
use crate::NodeId;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct LeaderId<NID>
where NID: NodeId
{
    pub term: u64,

    pub voted_for: Option<NID>,
}

impl<NID: NodeId> PartialOrd for LeaderId<NID> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match PartialOrd::partial_cmp(&self.term, &other.term) {
            Some(Ordering::Equal) => {
                match (&self.voted_for, &other.voted_for) {
                    (None, None) => Some(Ordering::Equal),
                    (Some(_), None) => Some(Ordering::Greater),
                    (None, Some(_)) => Some(Ordering::Less),
                    (Some(a), Some(b)) => {
                        if a == b {
                            Some(Ordering::Equal)
                        } else {
                            None
                        }
                    }
                }
            }
            cmp => cmp,
        }
    }
}

impl<NID: NodeId> LeaderId<NID> {
    pub fn new(term: u64, node_id: NID) -> Self {
        Self {
            term,
            voted_for: Some(node_id),
        }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn voted_for(&self) -> Option<NID> {
        self.voted_for.clone()
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_committed(&self) -> CommittedLeaderId<NID> {
        CommittedLeaderId::new(self.term, NID::default())
    }

    pub(crate) fn is_same_as_committed(&self, other: &CommittedLeaderId<NID>) -> bool {
        self.term == other.term
    }
}

impl<NID: NodeId> fmt::Display for LeaderId<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}-N{}", self.term, self.voted_for.display())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[derive(PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct CommittedLeaderId<NID> {
    pub term: u64,
    p: PhantomData<NID>,
}

impl<NID: NodeId> fmt::Display for CommittedLeaderId<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.term)
    }
}

impl<NID: NodeId> CommittedLeaderId<NID> {
    pub fn new(term: u64, node_id: NID) -> Self {
        let _ = node_id;
        Self { term, p: PhantomData }
    }
}
