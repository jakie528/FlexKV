use crate::LogId;
use crate::NodeId;
use crate::SnapshotId;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct SnapshotSignature<NID: NodeId> {
    pub last_log_id: Option<LogId<NID>>,

    pub last_membership_log_id: Option<LogId<NID>>,

    pub snapshot_id: SnapshotId,
}
