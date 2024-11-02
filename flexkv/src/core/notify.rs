use crate::core::state_machine;
use crate::raft::VoteResponse;
use crate::replication;
use crate::MessageSummary;
use crate::RaftTypeConfig;
use crate::Vote;

pub(crate) enum Notify<C>
where C: RaftTypeConfig
{
    VoteResponse {
        target: C::NodeId,
        resp: VoteResponse<C::NodeId>,

        sender_vote: Vote<C::NodeId>,
    },

    HigherVote {
        target: C::NodeId,

        higher: Vote<C::NodeId>,

        sender_vote: Vote<C::NodeId>,
    },

    Network { response: replication::Response<C> },

    StateMachine { command_result: state_machine::CommandResult<C> },

    Tick {
        i: u64,
    },
}

impl<C> Notify<C>
where C: RaftTypeConfig
{
    pub(crate) fn sm(command_result: state_machine::CommandResult<C>) -> Self {
        Self::StateMachine { command_result }
    }
}

impl<C> MessageSummary<Notify<C>> for Notify<C>
where C: RaftTypeConfig
{
    fn summary(&self) -> String {
        match self {
            Self::VoteResponse {
                target,
                resp,
                sender_vote: vote,
            } => {
                format!("VoteResponse: from: {}: {}, res-vote: {}", target, resp.summary(), vote)
            }
            Self::HigherVote {
                ref target,
                higher: ref new_vote,
                sender_vote: ref vote,
            } => {
                format!(
                    "Seen a higher vote: target: {}, vote: {}, server_state_vote: {}",
                    target, new_vote, vote
                )
            }
            Self::Network { response } => {
                format!("Replication command done: {}", response.summary())
            }
            Self::StateMachine { command_result } => {
                format!("StateMachine command done: {:?}", command_result)
            }
            Self::Tick { i } => {
                format!("Tick {}", i)
            }
        }
    }
}
