use core::fmt;

use crate::error::StreamingError;
use crate::raft::SnapshotResponse;
use crate::type_config::alias::InstantOf;
use crate::RaftTypeConfig;
use crate::SnapshotMeta;

#[derive(Debug)]
pub(crate) struct SnapshotCallback<C: RaftTypeConfig> {
    pub(crate) start_time: InstantOf<C>,

    pub(crate) snapshot_meta: SnapshotMeta<C::NodeId, C::Node>,

    pub(crate) result: Result<SnapshotResponse<C::NodeId>, StreamingError<C>>,
}

impl<C: RaftTypeConfig> SnapshotCallback<C> {
    pub(in crate::replication) fn new(
        start_time: InstantOf<C>,
        snapshot_meta: SnapshotMeta<C::NodeId, C::Node>,
        result: Result<SnapshotResponse<C::NodeId>, StreamingError<C>>,
    ) -> Self {
        Self {
            start_time,
            snapshot_meta,
            result,
        }
    }
}

impl<C: RaftTypeConfig> fmt::Display for SnapshotCallback<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SnapshotCallback {{ start_time: {:?}, snapshot_meta: {}, result:",
            self.start_time, self.snapshot_meta
        )?;

        match &self.result {
            Ok(resp) => write!(f, " Ok({})", resp)?,
            Err(e) => write!(f, " Err({})", e)?,
        };

        write!(f, " }}",)
    }
}
