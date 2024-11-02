use std::fmt::Debug;

use crate::core::raft_msg::ResultSender;
use crate::engine::operator::leader_operator::LeaderOperator;
use crate::engine::operator::replication_operator::ReplicationOperator;
use crate::engine::operator::server_state_operator::ServerStateOperator;
use crate::engine::Command;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::engine::Respond;
use crate::engine::ValueSender;
use crate::entry::RaftEntry;
use crate::error::RejectVoteRequest;
use crate::proposer::CandidateState;
use crate::proposer::LeaderState;
use crate::type_config::alias::InstantOf;
use crate::type_config::TypeConfigExt;
use crate::AsyncRuntime;
use crate::LogId;
use crate::OptionalSend;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::Vote;

pub(crate) struct VoteOperator<'st, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'st mut EngineConfig<C::NodeId>,
    pub(crate) state: &'st mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'st mut EngineOutput<C>,
    pub(crate) leader: &'st mut LeaderState<C>,
    pub(crate) candidate: &'st mut CandidateState<C>,
}

impl<'st, C> VoteOperator<'st, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn accept_vote<T, E, F>(
        &mut self,
        vote: &Vote<C::NodeId>,
        tx: ResultSender<C, T, E>,
        f: F,
    ) -> Option<ResultSender<C, T, E>>
    where
        T: Debug + Eq + OptionalSend,
        E: Debug + Eq + OptionalSend,
        Respond<C>: From<ValueSender<C, Result<T, E>>>,
        F: Fn(&RaftState<C::NodeId, C::Node, InstantOf<C>>, RejectVoteRequest<C::NodeId>) -> Result<T, E>,
    {
        let vote_res = self.update_vote(vote);

        if let Err(e) = vote_res {
            let res = f(self.state, e);

            self.output.push_command(Command::Respond {
                when: None,
                resp: Respond::new(res, tx),
            });

            return None;
        }
        Some(tx)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), RejectVoteRequest<C::NodeId>> {
        if vote >= self.state.vote_ref() {
        } else {
            tracing::info!("vote {} is rejected by local vote: {}", vote, self.state.vote_ref());
            return Err(RejectVoteRequest::ByVote(self.state.vote_ref().clone()));
        }


        if vote > self.state.vote_ref() {
            tracing::info!("vote is changing from {} to {}", self.state.vote_ref(), vote);

            self.state.vote.update(C::now(), vote.clone());
            self.output.push_command(Command::SaveVote { vote: vote.clone() });
        } else {
            self.state.vote.touch(C::now());
        }

        self.update_internal_server_state();

        Ok(())
    }

    pub(crate) fn update_internal_server_state(&mut self) {
        if self.state.is_leader(&self.config.id) {
            self.become_leader();
        } else if self.state.is_leading(&self.config.id) {
        } else {
            self.become_following();
        }
    }

    pub(crate) fn become_leader(&mut self) {

        if let Some(l) = self.leader.as_mut() {

            if l.vote.leader_id() == self.state.vote_ref().leader_id() {
                l.vote = self.state.vote_ref().clone();
                self.server_state_operator().update_server_state_if_changed();
                return;
            }
        }


        let leader = self.state.new_leader();
        *self.leader = Some(Box::new(leader));

        self.server_state_operator().update_server_state_if_changed();

        self.replication_operator().rebuild_replication_streams();

        let leader = self.leader.as_ref().unwrap();

        if leader.last_log_id() < leader.noop_log_id() {
            self.leader_operator()
                .leader_append_entries(vec![C::Entry::new_blank(LogId::<C::NodeId>::default())]);
        }
    }

    pub(crate) fn become_following(&mut self) {

        *self.leader = None;
        *self.candidate = None;

        self.server_state_operator().update_server_state_if_changed();
    }

    pub(crate) fn server_state_operator(&mut self) -> ServerStateOperator<C> {
        ServerStateOperator {
            config: self.config,
            state: self.state,
            output: self.output,
        }
    }

    pub(crate) fn replication_operator(&mut self) -> ReplicationOperator<C> {
        let leader = self.leader.as_mut().unwrap();

        ReplicationOperator {
            config: self.config,
            leader,
            state: self.state,
            output: self.output,
        }
    }

    pub(crate) fn leader_operator(&mut self) -> LeaderOperator<C> {
        let leader = self.leader.as_mut().unwrap();

        LeaderOperator {
            config: self.config,
            leader,
            state: self.state,
            output: self.output,
        }
    }
}
