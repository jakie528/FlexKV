use flex_macros::add_async_trait;

use crate::engine::Command;
use crate::RaftTypeConfig;
use crate::StorageError;

#[add_async_trait]
pub(crate) trait RaftRuntime<C: RaftTypeConfig> {
    async fn run_command<'e>(&mut self, cmd: Command<C>) -> Result<Option<Command<C>>, StorageError<C::NodeId>>;
}
