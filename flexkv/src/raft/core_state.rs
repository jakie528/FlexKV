use crate::error::Fatal;
use crate::error::Infallible;
use crate::AsyncRuntime;
use crate::NodeId;

pub(in crate::raft) enum CoreState<NID, A>
where
    NID: NodeId,
    A: AsyncRuntime,
{
    Running(A::JoinHandle<Result<Infallible, Fatal<NID>>>),

    Done(Result<Infallible, Fatal<NID>>),
}
