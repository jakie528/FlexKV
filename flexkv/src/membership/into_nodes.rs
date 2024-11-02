use std::collections::BTreeMap;
use std::collections::BTreeSet;

use maplit::btreemap;

use crate::Node;
use crate::NodeId;

pub trait IntoNodes<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn has_nodes(&self) -> bool {
        unimplemented!("has_nodes is deprecated")
    }

    fn node_ids(&self) -> Vec<NID> {
        unimplemented!("node_ids is deprecated")
    }

    fn into_nodes(self) -> BTreeMap<NID, N>;
}

impl<NID, N> IntoNodes<NID, N> for ()
where
    N: Node,
    NID: NodeId,
{
    fn into_nodes(self) -> BTreeMap<NID, N> {
        btreemap! {}
    }
}

impl<NID, N> IntoNodes<NID, N> for BTreeSet<NID>
where
    N: Node,
    NID: NodeId,
{
    fn into_nodes(self) -> BTreeMap<NID, N> {
        self.into_iter().map(|node_id| (node_id, N::default())).collect()
    }
}

impl<NID, N> IntoNodes<NID, N> for Option<BTreeSet<NID>>
where
    N: Node,
    NID: NodeId,
{
    fn into_nodes(self) -> BTreeMap<NID, N> {
        match self {
            None => BTreeMap::new(),
            Some(s) => s.into_iter().map(|node_id| (node_id, N::default())).collect(),
        }
    }
}

impl<NID, N> IntoNodes<NID, N> for BTreeMap<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn into_nodes(self) -> BTreeMap<NID, N> {
        self
    }
}
