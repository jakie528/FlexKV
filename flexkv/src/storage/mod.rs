
mod callback;
mod helper;
mod log_store_ext;
mod snapshot_signature;
mod v2;

use std::fmt;
use std::fmt::Debug;
use std::ops::RangeBounds;

pub use helper::StorageHelper;
pub use log_store_ext::RaftLogReaderExt;
use flex_macros::add_async_trait;
pub use snapshot_signature::SnapshotSignature;
pub use v2::RaftLogStorage;
pub use v2::RaftLogStorageExt;
pub use v2::RaftStateMachine;

use crate::display_ext::DisplayOption;
use crate::node::Node;
use crate::raft_types::SnapshotId;
pub use crate::storage::callback::LogApplied;
pub use crate::storage::callback::LogFlushed;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StoredMembership;
use crate::Vote;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct SnapshotMeta<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub last_log_id: Option<LogId<NID>>,

    pub last_membership: StoredMembership<NID, N>,

    pub snapshot_id: SnapshotId,
}

impl<NID, N> fmt::Display for SnapshotMeta<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{snapshot_id: {}, last_log:{}, last_membership: {}}}",
            self.snapshot_id,
            DisplayOption(&self.last_log_id),
            self.last_membership
        )
    }
}

impl<NID, N> MessageSummary<SnapshotMeta<NID, N>> for SnapshotMeta<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID, N> SnapshotMeta<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub fn signature(&self) -> SnapshotSignature<NID> {
        SnapshotSignature {
            last_log_id: self.last_log_id.clone(),
            last_membership_log_id: self.last_membership.log_id().clone(),
            snapshot_id: self.snapshot_id.clone(),
        }
    }

    pub fn last_log_id(&self) -> Option<&LogId<NID>> {
        self.last_log_id.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot<C>
where C: RaftTypeConfig
{
    pub meta: SnapshotMeta<C::NodeId, C::Node>,

    pub snapshot: Box<C::SnapshotData>,
}

impl<C> Snapshot<C>
where C: RaftTypeConfig
{
    #[allow(dead_code)]
    pub(crate) fn new(meta: SnapshotMeta<C::NodeId, C::Node>, snapshot: Box<C::SnapshotData>) -> Self {
        Self { meta, snapshot }
    }
}

impl<C> fmt::Display for Snapshot<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Snapshot{{meta: {}}}", self.meta)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogState<C: RaftTypeConfig> {
    pub last_purged_log_id: Option<LogId<C::NodeId>>,

    pub last_log_id: Option<LogId<C::NodeId>>,
}

#[add_async_trait]
pub trait RaftLogReader<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>;

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        self.try_get_log_entries(start..end).await
    }
}

#[add_async_trait]
pub trait RaftSnapshotBuilder<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C::NodeId>>;

}

#[add_async_trait]
pub trait RaftStorage<C>: RaftLogReader<C> + OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    type LogReader: RaftLogReader<C>;

    type SnapshotBuilder: RaftSnapshotBuilder<C>;


    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>;

    async fn save_committed(&mut self, _committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(None)
    }


    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>;

    async fn get_log_reader(&mut self) -> Self::LogReader;

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<C::NodeId>>
    where I: IntoIterator<Item = C::Entry> + OptionalSend;

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    async fn purge_logs_upto(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;


    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>>;

    async fn apply_to_state_machine(&mut self, entries: &[C::Entry]) -> Result<Vec<C::R>, StorageError<C::NodeId>>;


    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>;

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>>;

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>;
}
