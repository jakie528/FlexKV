use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::ops::RangeBounds;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

use crate::storage::v2::sealed::Sealed;
use crate::storage::LogFlushed;
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::LogId;
use crate::LogState;
use crate::OptionalSend;
use crate::RaftLogReader;
use crate::RaftStorage;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::SnapshotMeta;
use crate::StorageError;
use crate::StoredMembership;
use crate::Vote;

#[derive(Debug, Clone)]
pub struct Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    storage: Arc<RwLock<S>>,
    _phantom: PhantomData<C>,
}

impl<C, S> Default for Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C> + Default,
{
    fn default() -> Self {
        Self::create(Arc::new(RwLock::new(S::default())))
    }
}

impl<C, S> Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    pub fn new(store: S) -> (Self, Self) {
        let s = Arc::new(RwLock::new(store));

        let log_store = Adaptor::create(s.clone());
        let state_machine = Adaptor::create(s);

        (log_store, state_machine)
    }

    fn create(storage: Arc<RwLock<S>>) -> Self {
        Self {
            storage,
            _phantom: PhantomData,
        }
    }

    pub async fn storage_mut(&self) -> RwLockWriteGuard<S> {
        self.storage.write().await
    }

    pub async fn storage(&self) -> RwLockReadGuard<S> {
        self.storage.read().await
    }
}

impl<C, S> RaftLogReader<C> for Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        S::try_get_log_entries(self.storage_mut().await.deref_mut(), range).await
    }
}

impl<C, S> Sealed for Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
}

impl<C, S> RaftLogStorage<C> for Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    type LogReader = S::LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        S::get_log_state(self.storage_mut().await.deref_mut()).await
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        S::save_vote(self.storage_mut().await.deref_mut(), vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        S::read_vote(self.storage_mut().await.deref_mut()).await
    }

    async fn save_committed(&mut self, committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C::NodeId>> {
        S::save_committed(self.storage_mut().await.deref_mut(), committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        S::read_committed(self.storage_mut().await.deref_mut()).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        S::get_log_reader(self.storage_mut().await.deref_mut()).await
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
    where I: IntoIterator<Item = C::Entry> + OptionalSend {

        S::append_to_log(self.storage_mut().await.deref_mut(), entries).await?;
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        S::delete_conflict_logs_since(self.storage_mut().await.deref_mut(), log_id).await
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        S::purge_logs_upto(self.storage_mut().await.deref_mut(), log_id).await
    }
}

impl<C, S> RaftStateMachine<C> for Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    type SnapshotBuilder = S::SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>> {
        S::last_applied_state(self.storage_mut().await.deref_mut()).await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where I: IntoIterator<Item = C::Entry> + OptionalSend {
        let entries = entries.into_iter().collect::<Vec<_>>();
        S::apply_to_state_machine(self.storage_mut().await.deref_mut(), &entries).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        S::get_snapshot_builder(self.storage_mut().await.deref_mut()).await
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>> {
        S::begin_receiving_snapshot(self.storage_mut().await.deref_mut()).await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>> {
        S::install_snapshot(self.storage_mut().await.deref_mut(), meta, snapshot).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>> {
        S::get_current_snapshot(self.storage_mut().await.deref_mut()).await
    }
}