use core::fmt;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::error::ChangeMembershipError;
use crate::error::EmptyMembership;
use crate::error::LearnerNotFound;
use crate::membership::IntoNodes;
use crate::node::Node;
use crate::quorum::AsJoint;
use crate::quorum::FindCoherent;
use crate::quorum::Joint;
use crate::quorum::QuorumSet;
use crate::ChangeMembers;
use crate::MessageSummary;
use crate::NodeId;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    configs: Vec<BTreeSet<NID>>,

    nodes: BTreeMap<NID, N>,
}

impl<NID, N> From<BTreeMap<NID, N>> for Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn from(b: BTreeMap<NID, N>) -> Self {
        let member_ids = b.keys().cloned().collect::<BTreeSet<NID>>();
        Membership::new_unchecked(vec![member_ids], b)
    }
}

impl<NID, N> fmt::Display for Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{voters:[",)?;

        for (i, c) in self.configs.iter().enumerate() {
            if i > 0 {
                write!(f, ",",)?;
            }

            write!(f, "{{",)?;
            for (i, node_id) in c.iter().enumerate() {
                if i > 0 {
                    write!(f, ",",)?;
                }
                write!(f, "{node_id}:")?;

                if let Some(n) = self.get_node(node_id) {
                    write!(f, "{n:?}")?;
                } else {
                    write!(f, "None")?;
                }
            }
            write!(f, "}}")?;
        }
        write!(f, "]")?;

        let all_node_ids = self.nodes.keys().cloned().collect::<BTreeSet<_>>();
        let members = self.voter_ids().collect::<BTreeSet<_>>();

        write!(f, ", learners:[")?;

        for (learner_cnt, learner_id) in all_node_ids.difference(&members).enumerate() {
            if learner_cnt > 0 {
                write!(f, ",")?;
            }

            write!(f, "{learner_id}:")?;
            if let Some(n) = self.get_node(learner_id) {
                write!(f, "{n:?}")?;
            } else {
                write!(f, "None")?;
            }
        }
        write!(f, "]}}")?;
        Ok(())
    }
}

impl<NID, N> MessageSummary<Membership<NID, N>> for Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID, N> Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    pub fn new<T>(config: Vec<BTreeSet<NID>>, nodes: T) -> Self
    where T: IntoNodes<NID, N> {
        let voter_ids = config.as_joint().ids().collect::<BTreeSet<_>>();
        let nodes = Self::extend_nodes(nodes.into_nodes(), &voter_ids.into_nodes());

        Membership { configs: config, nodes }
    }

    pub fn is_in_joint_consensus(&self) -> bool {
        self.configs.len() > 1
    }

    pub fn get_joint_config(&self) -> &Vec<BTreeSet<NID>> {
        &self.configs
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NID, &N)> {
        self.nodes.iter()
    }

    pub fn get_node(&self, node_id: &NID) -> Option<&N> {
        self.nodes.get(node_id)
    }

    pub fn voter_ids(&self) -> impl Iterator<Item = NID> {
        self.configs.as_joint().ids()
    }

    pub fn learner_ids(&self) -> impl Iterator<Item = NID> + '_ {
        self.nodes.keys().filter(|x| !self.is_voter(x)).cloned()
    }
}

impl<NID, N> Membership<NID, N>
where
    N: Node,
    NID: NodeId,
{
    pub(crate) fn contains(&self, node_id: &NID) -> bool {
        self.nodes.contains_key(node_id)
    }

    pub(crate) fn is_voter(&self, node_id: &NID) -> bool {
        for c in self.configs.iter() {
            if c.contains(node_id) {
                return true;
            }
        }
        false
    }

    pub(crate) fn new_unchecked<T>(configs: Vec<BTreeSet<NID>>, nodes: T) -> Self
    where T: IntoNodes<NID, N> {
        let nodes = nodes.into_nodes();
        Membership { configs, nodes }
    }

    pub(crate) fn extend_nodes(old: BTreeMap<NID, N>, new: &BTreeMap<NID, N>) -> BTreeMap<NID, N> {
        let mut res = old;

        for (k, v) in new.iter() {
            if res.contains_key(k) {
                continue;
            }
            res.insert(k.clone(), v.clone());
        }

        res
    }

    pub(crate) fn ensure_valid(&self) -> Result<(), ChangeMembershipError<NID>> {
        self.ensure_non_empty_config()?;
        self.ensure_voter_nodes().map_err(|nid| LearnerNotFound { node_id: nid })?;
        Ok(())
    }

    pub(crate) fn ensure_non_empty_config(&self) -> Result<(), EmptyMembership> {
        for c in self.get_joint_config().iter() {
            if c.is_empty() {
                return Err(EmptyMembership {});
            }
        }

        Ok(())
    }

    pub(crate) fn ensure_voter_nodes(&self) -> Result<(), NID> {
        for voter_id in self.voter_ids() {
            if !self.nodes.contains_key(&voter_id) {
                return Err(voter_id);
            }
        }

        Ok(())
    }

    pub(crate) fn next_coherent(&self, goal: BTreeSet<NID>, retain: bool) -> Self {
        let config = Joint::from(self.configs.clone()).find_coherent(goal).children().clone();

        let mut nodes = self.nodes.clone();

        if !retain {
            let old_voter_ids = self.configs.as_joint().ids().collect::<BTreeSet<_>>();
            let new_voter_ids = config.as_joint().ids().collect::<BTreeSet<_>>();

            for node_id in old_voter_ids.difference(&new_voter_ids) {
                nodes.remove(node_id);
            }
        };

        Membership::new_unchecked(config, nodes)
    }

    pub(crate) fn change(
        mut self,
        change: ChangeMembers<NID, N>,
        retain: bool,
    ) -> Result<Self, ChangeMembershipError<NID>> {

        let last = self.get_joint_config().last().cloned().unwrap_or_default();

        let new_membership = match change {
            ChangeMembers::AddVoterIds(add_voter_ids) => {
                let new_voter_ids = last.union(&add_voter_ids).cloned().collect::<BTreeSet<_>>();
                self.next_coherent(new_voter_ids, retain)
            }
            ChangeMembers::AddVoters(add_voters) => {
                self.nodes = Self::extend_nodes(self.nodes, &add_voters);

                let add_voter_ids = add_voters.keys().cloned().collect::<BTreeSet<_>>();
                let new_voter_ids = last.union(&add_voter_ids).cloned().collect::<BTreeSet<_>>();
                self.next_coherent(new_voter_ids, retain)
            }
            ChangeMembers::RemoveVoters(remove_voter_ids) => {
                let new_voter_ids = last.difference(&remove_voter_ids).cloned().collect::<BTreeSet<_>>();
                self.next_coherent(new_voter_ids, retain)
            }
            ChangeMembers::ReplaceAllVoters(all_voter_ids) => self.next_coherent(all_voter_ids, retain),
            ChangeMembers::AddNodes(add_nodes) => {
                for (node_id, node) in add_nodes.into_iter() {
                    self.nodes.entry(node_id).or_insert(node);
                }
                self
            }
            ChangeMembers::SetNodes(set_nodes) => {
                for (node_id, node) in set_nodes.into_iter() {
                    self.nodes.insert(node_id, node);
                }
                self
            }
            ChangeMembers::RemoveNodes(remove_node_ids) => {
                for node_id in remove_node_ids.iter() {
                    self.nodes.remove(node_id);
                }
                self
            }
            ChangeMembers::ReplaceAllNodes(all_nodes) => {
                self.nodes = all_nodes;
                self
            }
        };

        new_membership.ensure_valid()?;

        Ok(new_membership)
    }

    pub(crate) fn to_quorum_set(&self) -> Joint<NID, Vec<NID>, Vec<Vec<NID>>> {
        let mut qs = vec![];
        for c in self.get_joint_config().iter() {
            qs.push(c.iter().cloned().collect::<Vec<_>>());
        }
        Joint::new(qs)
    }
}
