use std::fmt;

use crate::display_ext::DisplayOption;
use crate::LogId;
use crate::Membership;
use crate::MessageSummary;
use crate::Node;
use crate::NodeId;

#[derive(Clone, Debug, Default)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct StoredMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    log_id: Option<LogId<NID>>,

    membership: Membership<NID, N>,
}

impl<NID, N> StoredMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    pub fn new(log_id: Option<LogId<NID>>, membership: Membership<NID, N>) -> Self {
        Self { log_id, membership }
    }

    pub fn log_id(&self) -> &Option<LogId<NID>> {
        &self.log_id
    }

    pub fn membership(&self) -> &Membership<NID, N> {
        &self.membership
    }

    pub fn voter_ids(&self) -> impl Iterator<Item = NID> {
        self.membership.voter_ids()
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NID, &N)> {
        self.membership.nodes()
    }
}

impl<NID, N> fmt::Display for StoredMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{log_id:{}, {}}}",
            DisplayOption(&self.log_id),
            self.membership.summary()
        )
    }
}

impl<NID, N> MessageSummary<StoredMembership<NID, N>> for StoredMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn summary(&self) -> String {
        self.to_string()
    }
}
