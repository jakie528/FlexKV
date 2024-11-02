use std::fmt::Debug;

use crate::async_runtime::AsyncOneshotSendExt;
use crate::core::state_machine;
use crate::engine::CommandKind;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::error::InstallSnapshotError;
use crate::progress::entry::ProgressEntry;
use crate::progress::Inflight;
use crate::raft::AppendEntriesResponse;
use crate::raft::InstallSnapshotResponse;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::type_config::alias::OneshotSenderOf;
use crate::LeaderId;
use crate::LogId;
use crate::NodeId;
use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::Vote;

#[derive(Debug)]
pub(crate) enum Command<C>
where C: RaftTypeConfig
{
    BecomeLeader,

    QuitLeader,

    AppendInputEntries {
        vote: Vote<C::NodeId>,

        entries: Vec<C::Entry>,
    },

    ReplicateCommitted { committed: Option<LogId<C::NodeId>> },

    Commit {
        seq: state_machine::CommandSeq,
        already_committed: Option<LogId<C::NodeId>>,
        upto: LogId<C::NodeId>,
    },

    Replicate {
        target: C::NodeId,
        req: Inflight<C::NodeId>,
    },

    RebuildReplicationStreams {
        targets: Vec<(C::NodeId, ProgressEntry<C::NodeId>)>,
    },

    SaveVote { vote: Vote<C::NodeId> },

    SendVote { vote_req: VoteRequest<C::NodeId> },

    PurgeLog { upto: LogId<C::NodeId> },

    DeleteConflictLog { since: LogId<C::NodeId> },

    StateMachine { command: state_machine::Command<C> },

    Respond {
        when: Option<Condition<C::NodeId>>,
        resp: Respond<C>,
    },
}

impl<C> From<state_machine::Command<C>> for Command<C>
where C: RaftTypeConfig
{
    fn from(cmd: state_machine::Command<C>) -> Self {
        Self::StateMachine { command: cmd }
    }
}

impl<C> PartialEq for Command<C>
where
    C: RaftTypeConfig,
    C::Entry: PartialEq,
{
    #[rustfmt::skip]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Command::BecomeLeader,                            Command::BecomeLeader)                                                          => true,
            (Command::QuitLeader,                              Command::QuitLeader)                                                            => true,
            (Command::AppendInputEntries { vote, entries },    Command::AppendInputEntries { vote: vb, entries: b }, )                         => vote == vb && entries == b,
            (Command::ReplicateCommitted { committed },        Command::ReplicateCommitted { committed: b }, )                                 => committed == b,
            (Command::Commit { seq, already_committed, upto, }, Command::Commit { seq: b_seq, already_committed: b_committed, upto: b_upto, }, ) => seq == b_seq && already_committed == b_committed && upto == b_upto,
            (Command::Replicate { target, req },               Command::Replicate { target: b_target, req: other_req, }, )                     => target == b_target && req == other_req,
            (Command::RebuildReplicationStreams { targets },   Command::RebuildReplicationStreams { targets: b }, )                            => targets == b,
            (Command::SaveVote { vote },                       Command::SaveVote { vote: b })                                                  => vote == b,
            (Command::SendVote { vote_req },                   Command::SendVote { vote_req: b }, )                                            => vote_req == b,
            (Command::PurgeLog { upto },                       Command::PurgeLog { upto: b })                                                  => upto == b,
            (Command::DeleteConflictLog { since },             Command::DeleteConflictLog { since: b }, )                                      => since == b,
            (Command::Respond { when, resp: send },            Command::Respond { when: b_when, resp: b })                                     => send == b && when == b_when,
            (Command::StateMachine { command },                Command::StateMachine { command: b })                                           => command == b,
            _ => false,
        }
    }
}

impl<C> Command<C>
where C: RaftTypeConfig
{
    #[allow(dead_code)]
    #[rustfmt::skip]
    pub(crate) fn kind(&self) -> CommandKind {
        match self {
            Command::BecomeLeader                     => CommandKind::Main,
            Command::QuitLeader                       => CommandKind::Main,
            Command::RebuildReplicationStreams { .. } => CommandKind::Main,
            Command::Respond { .. }                   => CommandKind::Main,

            Command::AppendInputEntries { .. }        => CommandKind::Log,
            Command::SaveVote { .. }                  => CommandKind::Log,
            Command::PurgeLog { .. }                  => CommandKind::Log,
            Command::DeleteConflictLog { .. }         => CommandKind::Log,

            Command::ReplicateCommitted { .. }        => CommandKind::Network,
            Command::Replicate { .. }                 => CommandKind::Network,
            Command::SendVote { .. }                  => CommandKind::Network,

            Command::StateMachine { .. }              => CommandKind::StateMachine,
            Command::Commit { .. }                     => CommandKind::Main,
        }
    }

    #[allow(dead_code)]
    #[rustfmt::skip]
    pub(crate) fn condition(&self) -> Option<&Condition<C::NodeId>> {
        match self {
            Command::BecomeLeader                     => None,
            Command::QuitLeader                       => None,
            Command::AppendInputEntries { .. }        => None,
            Command::ReplicateCommitted { .. }        => None,
            Command::Commit { .. }                     => None,
            Command::Replicate { .. }                 => None,
            Command::RebuildReplicationStreams { .. } => None,
            Command::SaveVote { .. }                  => None,
            Command::SendVote { .. }                  => None,
            Command::PurgeLog { .. }                  => None,
            Command::DeleteConflictLog { .. }         => None,
            Command::Respond { when, .. }             => when.as_ref(),
            Command::StateMachine { .. }              => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[derive(PartialEq, Eq)]
pub(crate) enum Condition<NID>
where NID: NodeId
{
    #[allow(dead_code)]
    LogFlushed {
        leader: LeaderId<NID>,
        log_id: Option<LogId<NID>>,
    },

    #[allow(dead_code)]
    Applied { log_id: Option<LogId<NID>> },

    #[allow(dead_code)]
    StateMachineCommand { command_seq: state_machine::CommandSeq },
}

#[derive(Debug, PartialEq, Eq)]
#[derive(derive_more::From)]
pub(crate) enum Respond<C>
where C: RaftTypeConfig
{
    Vote(ValueSender<C, Result<VoteResponse<C::NodeId>, Infallible>>),
    AppendEntries(ValueSender<C, Result<AppendEntriesResponse<C::NodeId>, Infallible>>),
    ReceiveSnapshotChunk(ValueSender<C, Result<(), InstallSnapshotError>>),
    InstallSnapshot(ValueSender<C, Result<InstallSnapshotResponse<C::NodeId>, InstallSnapshotError>>),
    InstallFullSnapshot(ValueSender<C, Result<SnapshotResponse<C::NodeId>, Infallible>>),
    Initialize(ValueSender<C, Result<(), InitializeError<C::NodeId, C::Node>>>),
}

impl<C> Respond<C>
where C: RaftTypeConfig
{
    pub(crate) fn new<T>(res: T, tx: OneshotSenderOf<C, T>) -> Self
    where
        T: Debug + PartialEq + Eq + OptionalSend,
        Self: From<ValueSender<C, T>>,
    {
        Respond::from(ValueSender::new(res, tx))
    }

    pub(crate) fn send(self) {
        match self {
            Respond::Vote(x) => x.send(),
            Respond::AppendEntries(x) => x.send(),
            Respond::ReceiveSnapshotChunk(x) => x.send(),
            Respond::InstallSnapshot(x) => x.send(),
            Respond::InstallFullSnapshot(x) => x.send(),
            Respond::Initialize(x) => x.send(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ValueSender<C, T>
where
    T: Debug + PartialEq + Eq + OptionalSend,
    C: RaftTypeConfig,
{
    value: T,
    tx: OneshotSenderOf<C, T>,
}

impl<C, T> PartialEq for ValueSender<C, T>
where
    T: Debug + PartialEq + Eq + OptionalSend,
    C: RaftTypeConfig,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<C, T> Eq for ValueSender<C, T>
where
    T: Debug + PartialEq + Eq + OptionalSend,
    C: RaftTypeConfig,
{
}

impl<C, T> ValueSender<C, T>
where
    T: Debug + PartialEq + Eq + OptionalSend,
    C: RaftTypeConfig,
{
    pub(crate) fn new(res: T, tx: OneshotSenderOf<C, T>) -> Self {
        Self { value: res, tx }
    }

    pub(crate) fn send(self) {
        let _ = self.tx.send(self.value);
    }
}
