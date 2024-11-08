use anyerror::AnyError;
use flex_macros::add_async_trait;

use crate::raft_state::LogIOId;
use crate::storage::LogFlushed;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StorageIOError;
use crate::Vote;

#[add_async_trait]
pub trait RaftLogStorageExt<C>: RaftLogStorage<C>
where C: RaftTypeConfig
{
    async fn blocking_append<I>(&mut self, entries: I) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (tx, rx) = C::oneshot();

        let log_io_id = LogIOId::<C::NodeId>::new(Vote::<C::NodeId>::default(), None);

        let callback = LogFlushed::<C>::new(log_io_id, tx);
        self.append(entries, callback).await?;
        rx.await
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?;

        Ok(())
    }
}

impl<C, T> RaftLogStorageExt<C> for T
where
    T: RaftLogStorage<C>,
    C: RaftTypeConfig,
{
}
