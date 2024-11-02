use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

use crate::log_id::RaftLogId;
use crate::node::Node;
use crate::quorum::Joint;
use crate::quorum::QuorumSet;
use crate::LogId;
use crate::Membership;
use crate::MessageSummary;
use crate::NodeId;
use crate::StoredMembership;

#[derive(Clone, Default, Eq)]
pub struct EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    stored_membership: Arc<StoredMembership<NID, N>>,

    quorum_set: Joint<NID, Vec<NID>, Vec<Vec<NID>>>,

    voter_ids: BTreeSet<NID>,
}

impl<NID, N> Debug for EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectiveMembership")
            .field("log_id", self.log_id())
            .field("membership", self.membership())
            .field("voter_ids", &self.voter_ids)
            .finish()
    }
}

impl<NID, N> PartialEq for EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn eq(&self, other: &Self) -> bool {
        self.stored_membership == other.stored_membership && self.voter_ids == other.voter_ids
    }
}

impl<NID, N, LID> From<(&LID, Membership<NID, N>)> for EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
    LID: RaftLogId<NID>,
{
    fn from(v: (&LID, Membership<NID, N>)) -> Self {
        EffectiveMembership::new(Some(v.0.get_log_id().clone()), v.1)
    }
}

impl<NID, N> EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    pub(crate) fn new_arc(log_id: Option<LogId<NID>>, membership: Membership<NID, N>) -> Arc<Self> {
        Arc::new(Self::new(log_id, membership))
    }

    pub fn new(log_id: Option<LogId<NID>>, membership: Membership<NID, N>) -> Self {
        let voter_ids = membership.voter_ids().collect();

        let configs = membership.get_joint_config();
        let mut joint = vec![];
        for c in configs {
            joint.push(c.iter().cloned().collect::<Vec<_>>());
        }

        let quorum_set = Joint::from(joint);

        Self {
            stored_membership: Arc::new(StoredMembership::new(log_id, membership)),
            quorum_set,
            voter_ids,
        }
    }

    pub(crate) fn new_from_stored_membership(stored: StoredMembership<NID, N>) -> Self {
        Self::new(stored.log_id().clone(), stored.membership().clone())
    }

    pub(crate) fn stored_membership(&self) -> &Arc<StoredMembership<NID, N>> {
        &self.stored_membership
    }

    pub fn log_id(&self) -> &Option<LogId<NID>> {
        self.stored_membership.log_id()
    }

    pub fn membership(&self) -> &Membership<NID, N> {
        self.stored_membership.membership()
    }
}

impl<NID, N> EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    #[allow(dead_code)]
    pub(crate) fn is_voter(&self, nid: &NID) -> bool {
        self.membership().is_voter(nid)
    }

    pub fn voter_ids(&self) -> impl Iterator<Item = NID> + '_ {
        self.voter_ids.iter().cloned()
    }

    pub(crate) fn learner_ids(&self) -> impl Iterator<Item = NID> + '_ {
        self.membership().learner_ids()
    }

    pub fn get_node(&self, node_id: &NID) -> Option<&N> {
        self.membership().get_node(node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NID, &N)> {
        self.membership().nodes()
    }

    pub fn get_joint_config(&self) -> &Vec<Vec<NID>> {
        self.quorum_set.children()
    }
}

impl<NID, N> MessageSummary<EffectiveMembership<NID, N>> for EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn summary(&self) -> String {
        format!(
            "{{log_id:{:?} membership:{}}}",
            self.log_id(),
            self.membership().summary()
        )
    }
}

impl<NID, N> QuorumSet<NID> for EffectiveMembership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    type Iter = std::collections::btree_set::IntoIter<NID>;

    fn is_quorum<'a, I: Iterator<Item = &'a NID> + Clone>(&self, ids: I) -> bool {
        self.quorum_set.is_quorum(ids)
    }

    fn ids(&self) -> Self::Iter {
        self.quorum_set.ids()
    }
}
