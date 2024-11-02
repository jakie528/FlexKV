use std::collections::BTreeMap;

use crate::core::raft_msg::external_command::ExternalCommand;
use crate::error::CheckIsLeaderError;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::raft::BoxCoreFn;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::NodeIdOf;
use crate::type_config::alias::NodeOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::ResponderOf;
use crate::type_config::alias::SnapshotDataOf;
use crate::ChangeMembers;
use crate::MessageSummary;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::Vote;

pub(crate) mod external_command;

pub(crate) type ResultSender<C, T, E = Infallible> = OneshotSenderOf<C, Result<T, E>>;

pub(crate) type VoteTx<C> = ResultSender<C, VoteResponse<NodeIdOf<C>>>;

pub(crate) type AppendEntriesTx<C> = ResultSender<C, AppendEntriesResponse<NodeIdOf<C>>>;

pub(crate) type ClientReadTx<C> =
    ResultSender<C, (Option<LogIdOf<C>>, Option<LogIdOf<C>>), CheckIsLeaderError<NodeIdOf<C>, NodeOf<C>>>;

pub(crate) enum RaftMsg<C>
where C: RaftTypeConfig
{
    AppendEntries {
        rpc: AppendEntriesRequest<C>,
        tx: AppendEntriesTx<C>,
    },

    RequestVote {
        rpc: VoteRequest<C::NodeId>,
        tx: VoteTx<C>,
    },

    InstallFullSnapshot {
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        tx: ResultSender<C, SnapshotResponse<C::NodeId>>,
    },

    BeginReceivingSnapshot {
        tx: ResultSender<C, Box<SnapshotDataOf<C>>, Infallible>,
    },

    ClientWriteRequest {
        app_data: C::D,
        tx: ResponderOf<C>,
    },

    CheckIsLeaderRequest {
        tx: ClientReadTx<C>,
    },

    Initialize {
        members: BTreeMap<C::NodeId, C::Node>,
        tx: ResultSender<C, (), InitializeError<C::NodeId, C::Node>>,
    },

    ChangeMembership {
        changes: ChangeMembers<C::NodeId, C::Node>,

        retain: bool,

        tx: ResponderOf<C>,
    },

    ExternalCoreRequest {
        req: BoxCoreFn<C>,
    },

    ExternalCommand {
        cmd: ExternalCommand<C>,
    },
}

impl<C> MessageSummary<RaftMsg<C>> for RaftMsg<C>
where C: RaftTypeConfig
{
    fn summary(&self) -> String {
        match self {
            RaftMsg::AppendEntries { rpc, .. } => {
                format!("AppendEntries: {}", rpc.summary())
            }
            RaftMsg::RequestVote { rpc, .. } => {
                format!("RequestVote: {}", rpc.summary())
            }
            RaftMsg::BeginReceivingSnapshot { .. } => "BeginReceivingSnapshot".to_string(),
            RaftMsg::InstallFullSnapshot { vote, snapshot, .. } => {
                format!("InstallFullSnapshot: vote: {}, snapshot: {}", vote, snapshot)
            }
            RaftMsg::ClientWriteRequest { .. } => "ClientWriteRequest".to_string(),
            RaftMsg::CheckIsLeaderRequest { .. } => "CheckIsLeaderRequest".to_string(),
            RaftMsg::Initialize { members, .. } => {
                format!("Initialize: {:?}", members)
            }
            RaftMsg::ChangeMembership {
                changes: members,
                retain,
                ..
            } => {
                format!("ChangeMembership: members: {:?}, retain: {}", members, retain,)
            }
            RaftMsg::ExternalCoreRequest { .. } => "External Request".to_string(),
            RaftMsg::ExternalCommand { cmd } => {
                format!("ExternalCommand: {:?}", cmd)
            }
        }
    }
}
