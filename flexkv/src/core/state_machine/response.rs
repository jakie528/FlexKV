use crate::core::state_machine::command::CommandSeq;
use crate::core::ApplyResult;
use crate::RaftTypeConfig;
use crate::SnapshotMeta;
use crate::StorageError;

#[derive(Debug)]
pub(crate) enum Response<C>
where C: RaftTypeConfig
{
    BuildSnapshot(SnapshotMeta<C::NodeId, C::Node>),

    InstallSnapshot(Option<SnapshotMeta<C::NodeId, C::Node>>),

    Apply(ApplyResult<C>),
}

#[derive(Debug)]
pub(crate) struct CommandResult<C>
where C: RaftTypeConfig
{
    #[allow(dead_code)]
    pub(crate) command_seq: CommandSeq,
    pub(crate) result: Result<Response<C>, StorageError<C::NodeId>>,
}

impl<C> CommandResult<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(command_seq: CommandSeq, result: Result<Response<C>, StorageError<C::NodeId>>) -> Self {
        Self { command_seq, result }
    }
}
