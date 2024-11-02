
mod raft_log_storage_ext;

use flex_macros::add_async_trait;
pub use raft_log_storage_ext::RaftLogStorageExt;

use crate::storage::callback::LogFlushed;
use crate::storage::v2::sealed::Sealed;
use crate::LogId;
use crate::LogState;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftLogReader;
use crate::RaftSnapshotBuilder;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::SnapshotMeta;
use crate::StorageError;
use crate::StoredMembership;
use crate::Vote;

pub(crate) mod sealed {
    pub trait Sealed {}

    impl<T> Sealed for T {}
}

#[add_async_trait]
pub trait RaftLogStorage<C>: Sealed + RaftLogReader<C> + OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    type LogReader: RaftLogReader<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>;

    async fn get_log_reader(&mut self) -> Self::LogReader;

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>;

    async fn save_committed(&mut self, _committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(None)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend;

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>>;
}

#[add_async_trait]
pub trait RaftStateMachine<C>: Sealed + OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    type SnapshotBuilder: RaftSnapshotBuilder<C>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>>;

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend;

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>;

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>>;

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>;
}
