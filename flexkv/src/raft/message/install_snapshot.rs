use std::fmt;

use crate::MessageSummary;
use crate::NodeId;
use crate::RaftTypeConfig;
use crate::SnapshotMeta;
use crate::Vote;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct InstallSnapshotRequest<C: RaftTypeConfig> {
    pub vote: Vote<C::NodeId>,

    pub meta: SnapshotMeta<C::NodeId, C::Node>,

    pub offset: u64,
    pub data: Vec<u8>,

    pub done: bool,
}

impl<C: RaftTypeConfig> fmt::Display for InstallSnapshotRequest<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InstallSnapshotRequest {{ vote:{}, meta:{}, offset:{}, len:{}, done:{} }}",
            self.vote,
            self.meta,
            self.offset,
            self.data.len(),
            self.done
        )
    }
}

impl<C: RaftTypeConfig> MessageSummary<InstallSnapshotRequest<C>> for InstallSnapshotRequest<C> {
    fn summary(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug)]
#[derive(PartialEq, Eq)]
#[derive(derive_more::Display)]
#[display("{{vote:{}}}", vote)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct InstallSnapshotResponse<NID: NodeId> {
    pub vote: Vote<NID>,
}

#[derive(Debug)]
#[derive(PartialEq, Eq)]
#[derive(derive_more::Display)]
#[display("SnapshotResponse{{vote:{}}}", vote)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct SnapshotResponse<NID: NodeId> {
    pub vote: Vote<NID>,
}

impl<NID: NodeId> SnapshotResponse<NID> {
    pub fn new(vote: Vote<NID>) -> Self {
        Self { vote }
    }
}

impl<NID: NodeId> From<SnapshotResponse<NID>> for InstallSnapshotResponse<NID> {
    fn from(snap_resp: SnapshotResponse<NID>) -> Self {
        Self { vote: snap_resp.vote }
    }
}
