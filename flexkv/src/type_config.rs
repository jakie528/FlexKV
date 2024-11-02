
pub(crate) mod util;

use std::fmt::Debug;

pub use util::TypeConfigExt;

use crate::entry::FromAppData;
use crate::entry::RaftEntry;
use crate::raft::responder::Responder;
use crate::AppData;
use crate::AppDataResponse;
use crate::AsyncRuntime;
use crate::Node;
use crate::NodeId;
use crate::OptionalSend;
use crate::OptionalSync;

pub trait RaftTypeConfig:
    Sized + OptionalSend + OptionalSync + Debug + Clone + Copy + Default + Eq + PartialEq + Ord + PartialOrd + 'static
{
    type D: AppData;

    type R: AppDataResponse;

    type NodeId: NodeId;

    type Node: Node;

    type Entry: RaftEntry<Self::NodeId, Self::Node> + FromAppData<Self::D>;

    type SnapshotData: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + tokio::io::AsyncSeek
        + OptionalSend
        + Unpin
        + 'static;

    type AsyncRuntime: AsyncRuntime;

    type Responder: Responder<Self>;
}

#[allow(dead_code)]
pub(crate) mod alias {
    use crate::raft::responder::Responder;
    use crate::RaftTypeConfig;

    pub type DOf<C> = <C as RaftTypeConfig>::D;
    pub type ROf<C> = <C as RaftTypeConfig>::R;
    pub type NodeIdOf<C> = <C as RaftTypeConfig>::NodeId;
    pub type NodeOf<C> = <C as RaftTypeConfig>::Node;
    pub type EntryOf<C> = <C as RaftTypeConfig>::Entry;
    pub type SnapshotDataOf<C> = <C as RaftTypeConfig>::SnapshotData;
    pub type AsyncRuntimeOf<C> = <C as RaftTypeConfig>::AsyncRuntime;
    pub type ResponderOf<C> = <C as RaftTypeConfig>::Responder;
    pub type ResponderReceiverOf<C> = <ResponderOf<C> as Responder<C>>::Receiver;

    pub type JoinErrorOf<C> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::JoinError;
    pub type JoinHandleOf<C, T> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::JoinHandle<T>;
    pub type SleepOf<C> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::Sleep;
    pub type InstantOf<C> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::Instant;
    pub type TimeoutErrorOf<C> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::TimeoutError;
    pub type TimeoutOf<C, R, F> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::Timeout<R, F>;
    pub type OneshotSenderOf<C, T> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::OneshotSender<T>;
    pub type OneshotReceiverErrorOf<C> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::OneshotReceiverError;
    pub type OneshotReceiverOf<C, T> = <AsyncRuntimeOf<C> as crate::AsyncRuntime>::OneshotReceiver<T>;

    pub type LogIdOf<C> = crate::LogId<NodeIdOf<C>>;
    pub type VoteOf<C> = crate::Vote<NodeIdOf<C>>;
    pub type LeaderIdOf<C> = crate::LeaderId<NodeIdOf<C>>;
    pub type CommittedLeaderIdOf<C> = crate::CommittedLeaderId<NodeIdOf<C>>;
}
