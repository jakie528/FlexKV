
pub mod decompose;
pub mod into_ok;
mod replication_closed;
mod streaming_error;

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;

use anyerror::AnyError;

pub use self::replication_closed::ReplicationClosed;
pub use self::streaming_error::StreamingError;
use crate::network::RPCTypes;
use crate::node::Node;
use crate::raft::AppendEntriesResponse;
use crate::raft_types::SnapshotSegmentId;
use crate::try_as_ref::TryAsRef;
use crate::LogId;
use crate::Membership;
use crate::NodeId;
use crate::StorageError;
use crate::Vote;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum RaftError<NID, E = Infallible>
where NID: NodeId
{
    #[error(transparent)]
    APIError(E),

    #[cfg_attr(feature = "serde", serde(bound = ""))]
    #[error(transparent)]
    Fatal(#[from] Fatal<NID>),
}

impl<NID, E> RaftError<NID, E>
where
    NID: NodeId,
    E: Debug,
{
    pub fn api_error(&self) -> Option<&E> {
        match self {
            RaftError::APIError(e) => Some(e),
            RaftError::Fatal(_) => None,
        }
    }

    pub fn into_api_error(self) -> Option<E> {
        match self {
            RaftError::APIError(e) => Some(e),
            RaftError::Fatal(_) => None,
        }
    }

    pub fn fatal(&self) -> Option<&Fatal<NID>> {
        match self {
            RaftError::APIError(_) => None,
            RaftError::Fatal(f) => Some(f),
        }
    }

    pub fn into_fatal(self) -> Option<Fatal<NID>> {
        match self {
            RaftError::APIError(_) => None,
            RaftError::Fatal(f) => Some(f),
        }
    }

    pub fn forward_to_leader<N>(&self) -> Option<&ForwardToLeader<NID, N>>
    where
        N: Node,
        E: TryAsRef<ForwardToLeader<NID, N>>,
    {
        match self {
            RaftError::APIError(api_err) => api_err.try_as_ref(),
            RaftError::Fatal(_) => None,
        }
    }

    pub fn into_forward_to_leader<N>(self) -> Option<ForwardToLeader<NID, N>>
    where
        N: Node,
        E: TryInto<ForwardToLeader<NID, N>>,
    {
        match self {
            RaftError::APIError(api_err) => api_err.try_into().ok(),
            RaftError::Fatal(_) => None,
        }
    }
}

impl<NID, N, E> TryAsRef<ForwardToLeader<NID, N>> for RaftError<NID, E>
where
    NID: NodeId,
    N: Node,
    E: Debug + TryAsRef<ForwardToLeader<NID, N>>,
{
    fn try_as_ref(&self) -> Option<&ForwardToLeader<NID, N>> {
        self.forward_to_leader()
    }
}

impl<NID, E> From<StorageError<NID>> for RaftError<NID, E>
where NID: NodeId
{
    fn from(se: StorageError<NID>) -> Self {
        RaftError::Fatal(Fatal::from(se))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum Fatal<NID>
where NID: NodeId
{
    #[error(transparent)]
    StorageError(#[from] StorageError<NID>),

    #[error("panicked")]
    Panicked,

    #[error("raft stopped")]
    Stopped,
}

#[derive(Debug, Clone, thiserror::Error, derive_more::TryInto)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum InstallSnapshotError {
    #[error(transparent)]
    SnapshotMismatch(#[from] SnapshotMismatch),
}

#[derive(Debug, Clone, thiserror::Error, derive_more::TryInto)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum CheckIsLeaderError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),

    #[error(transparent)]
    QuorumNotEnough(#[from] QuorumNotEnough<NID>),
}

impl<NID, N> TryAsRef<ForwardToLeader<NID, N>> for CheckIsLeaderError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn try_as_ref(&self) -> Option<&ForwardToLeader<NID, N>> {
        match self {
            Self::ForwardToLeader(f) => Some(f),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, derive_more::TryInto)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum ClientWriteError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<NID, N>),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError<NID>),
}

impl<NID, N> TryAsRef<ForwardToLeader<NID, N>> for ClientWriteError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn try_as_ref(&self) -> Option<&ForwardToLeader<NID, N>> {
        match self {
            Self::ForwardToLeader(f) => Some(f),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum ChangeMembershipError<NID: NodeId> {
    #[error(transparent)]
    InProgress(#[from] InProgress<NID>),

    #[error(transparent)]
    EmptyMembership(#[from] EmptyMembership),

    #[error(transparent)]
    LearnerNotFound(#[from] LearnerNotFound<NID>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, derive_more::TryInto)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum InitializeError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    #[error(transparent)]
    NotAllowed(#[from] NotAllowed<NID>),

    #[error(transparent)]
    NotInMembers(#[from] NotInMembers<NID, N>),
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ReplicationError<NID, N>
where
    NID: NodeId,
    N: Node,
{
    #[error(transparent)]
    HigherVote(#[from] HigherVote<NID>),

    #[error(transparent)]
    Closed(#[from] ReplicationClosed),

    #[error(transparent)]
    StorageError(#[from] StorageError<NID>),

    #[error(transparent)]
    RPCError(#[from] RPCError<NID, N>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(serialize = "E: serde::Serialize")),
    serde(bound(deserialize = "E: for <'d> serde::Deserialize<'d>"))
)]
pub enum RPCError<NID: NodeId, N: Node, E: Error = Infallible> {
    #[error(transparent)]
    Timeout(#[from] Timeout<NID>),

    #[error(transparent)]
    Unreachable(#[from] Unreachable),

    #[error(transparent)]
    PayloadTooLarge(#[from] PayloadTooLarge),

    #[error(transparent)]
    Network(#[from] NetworkError),

    #[error(transparent)]
    RemoteError(#[from] RemoteError<NID, N, E>),
}

impl<NID, N, E> RPCError<NID, N, RaftError<NID, E>>
where
    NID: NodeId,
    N: Node,
    E: Error,
{
    pub fn forward_to_leader(&self) -> Option<&ForwardToLeader<NID, N>>
    where E: TryAsRef<ForwardToLeader<NID, N>> {
        match self {
            RPCError::Timeout(_) => None,
            RPCError::Unreachable(_) => None,
            RPCError::PayloadTooLarge(_) => None,
            RPCError::Network(_) => None,
            RPCError::RemoteError(remote_err) => remote_err.source.forward_to_leader(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("error occur on remote peer {target}: {source}")]
pub struct RemoteError<NID: NodeId, N: Node, T: Error> {
    #[cfg_attr(feature = "serde", serde(bound = ""))]
    pub target: NID,
    #[cfg_attr(feature = "serde", serde(bound = ""))]
    pub target_node: Option<N>,
    pub source: T,
}

impl<NID: NodeId, N: Node, T: Error> RemoteError<NID, N, T> {
    pub fn new(target: NID, e: T) -> Self {
        Self {
            target,
            target_node: None,
            source: e,
        }
    }
    pub fn new_with_node(target: NID, node: N, e: T) -> Self {
        Self {
            target,
            target_node: Some(node),
            source: e,
        }
    }
}

impl<NID, N, E> From<RemoteError<NID, N, Fatal<NID>>> for RemoteError<NID, N, RaftError<NID, E>>
where
    NID: NodeId,
    N: Node,
    E: Error,
{
    fn from(e: RemoteError<NID, N, Fatal<NID>>) -> Self {
        RemoteError {
            target: e.target,
            target_node: e.target_node,
            source: RaftError::Fatal(e.source),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("seen a higher vote: {higher} GT mine: {sender_vote}")]
pub(crate) struct HigherVote<NID: NodeId> {
    pub(crate) higher: Vote<NID>,
    pub(crate) sender_vote: Vote<NID>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("NetworkError: {source}")]
pub struct NetworkError {
    #[from]
    source: AnyError,
}

impl NetworkError {
    pub fn new<E: Error + 'static>(e: &E) -> Self {
        Self {
            source: AnyError::new(e),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("Unreachable node: {source}")]
pub struct Unreachable {
    #[from]
    source: AnyError,
}

impl Unreachable {
    pub fn new<E: Error + 'static>(e: &E) -> Self {
        Self {
            source: AnyError::new(e),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct PayloadTooLarge {
    action: RPCTypes,

    entries_hint: u64,

    bytes_hint: u64,

    #[source]
    source: Option<AnyError>,
}

impl fmt::Display for PayloadTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RPC",)?;
        write!(f, "({})", self.action)?;
        write!(f, " payload too large:",)?;

        write!(f, " hint:(")?;
        match self.action {
            RPCTypes::Vote => {
                unreachable!("vote rpc should not have payload")
            }
            RPCTypes::AppendEntries => {
                write!(f, "entries:{}", self.entries_hint)?;
            }
            RPCTypes::InstallSnapshot => {
                write!(f, "bytes:{}", self.bytes_hint)?;
            }
        }
        write!(f, ")")?;

        if let Some(s) = &self.source {
            write!(f, ", source: {}", s)?;
        }

        Ok(())
    }
}

impl PayloadTooLarge {
    pub fn new_entries_hint(entries_hint: u64) -> Self {

        Self {
            action: RPCTypes::AppendEntries,
            entries_hint,
            bytes_hint: u64::MAX,
            source: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_bytes_hint(bytes_hint: u64) -> Self {

        Self {
            action: RPCTypes::InstallSnapshot,
            entries_hint: u64::MAX,
            bytes_hint,
            source: None,
        }
    }

    pub fn with_source_error(mut self, e: &(impl Error + 'static)) -> Self {
        self.source = Some(AnyError::new(e));
        self
    }

    pub fn action(&self) -> RPCTypes {
        self.action
    }

    pub fn entries_hint(&self) -> u64 {
        self.entries_hint
    }

    #[allow(dead_code)]
    pub(crate) fn bytes_hint(&self) -> u64 {
        self.bytes_hint
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("timeout after {timeout:?} when {action} {id}->{target}")]
pub struct Timeout<NID: NodeId> {
    pub action: RPCTypes,
    pub id: NID,
    pub target: NID,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("store has no log at: {index:?}, last purged: {last_purged_log_id:?}")]
pub struct LackEntry<NID: NodeId> {
    pub index: Option<u64>,
    pub last_purged_log_id: Option<LogId<NID>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("has to forward request to: {leader_id:?}, {leader_node:?}")]
pub struct ForwardToLeader<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub leader_id: Option<NID>,
    pub leader_node: Option<N>,
}

impl<NID, N> ForwardToLeader<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub const fn empty() -> Self {
        Self {
            leader_id: None,
            leader_node: None,
        }
    }

    pub fn new(leader_id: NID, node: N) -> Self {
        Self {
            leader_id: Some(leader_id),
            leader_node: Some(node),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("snapshot segment id mismatch, expect: {expect}, got: {got}")]
pub struct SnapshotMismatch {
    pub expect: SnapshotSegmentId,
    pub got: SnapshotSegmentId,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("not enough for a quorum, cluster: {cluster}, got: {got:?}")]
pub struct QuorumNotEnough<NID: NodeId> {
    pub cluster: String,
    pub got: BTreeSet<NID>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("the cluster is already undergoing a configuration change at log {membership_log_id:?}, last committed membership log id: {committed:?}")]
pub struct InProgress<NID: NodeId> {
    pub committed: Option<LogId<NID>>,
    pub membership_log_id: Option<LogId<NID>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("Learner {node_id} not found: add it as learner before adding it as a voter")]
pub struct LearnerNotFound<NID: NodeId> {
    pub node_id: NID,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("not allowed to initialize due to current raft state: last_log_id: {last_log_id:?} vote: {vote}")]
pub struct NotAllowed<NID: NodeId> {
    pub last_log_id: Option<LogId<NID>>,
    pub vote: Vote<NID>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
#[error("node {node_id} has to be a member. membership:{membership:?}")]
pub struct NotInMembers<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub node_id: NID,
    pub membership: Membership<NID, N>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("new membership can not be empty")]
pub struct EmptyMembership {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("infallible")]
pub enum Infallible {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("no-forward")]
pub enum NoForward {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub(crate) enum RejectVoteRequest<NID: NodeId> {
    #[error("reject vote request by a greater vote: {0}")]
    ByVote(Vote<NID>),

    #[allow(dead_code)]
    #[error("reject vote request by a greater last-log-id: {0:?}")]
    ByLastLogId(Option<LogId<NID>>),
}

impl<NID: NodeId> From<RejectVoteRequest<NID>> for AppendEntriesResponse<NID> {
    fn from(r: RejectVoteRequest<NID>) -> Self {
        match r {
            RejectVoteRequest::ByVote(v) => AppendEntriesResponse::HigherVote(v),
            RejectVoteRequest::ByLastLogId(_) => {
                unreachable!("the leader should always has a greater last log id")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RejectAppendEntries<NID: NodeId> {
    #[error("reject AppendEntries by a greater vote: {0}")]
    ByVote(Vote<NID>),

    #[error("reject AppendEntries because of conflicting log-id: {local:?}; expect to be: {expect:?}")]
    ByConflictingLogId {
        expect: LogId<NID>,
        local: Option<LogId<NID>>,
    },
}

impl<NID: NodeId> From<RejectVoteRequest<NID>> for RejectAppendEntries<NID> {
    fn from(r: RejectVoteRequest<NID>) -> Self {
        match r {
            RejectVoteRequest::ByVote(v) => RejectAppendEntries::ByVote(v),
            RejectVoteRequest::ByLastLogId(_) => {
                unreachable!("the leader should always has a greater last log id")
            }
        }
    }
}

impl<NID: NodeId> From<Result<(), RejectAppendEntries<NID>>> for AppendEntriesResponse<NID> {
    fn from(r: Result<(), RejectAppendEntries<NID>>) -> Self {
        match r {
            Ok(_) => AppendEntriesResponse::Success,
            Err(e) => match e {
                RejectAppendEntries::ByVote(v) => AppendEntriesResponse::HigherVote(v),
                RejectAppendEntries::ByConflictingLogId { expect: _, local: _ } => AppendEntriesResponse::Conflict,
            },
        }
    }
}
