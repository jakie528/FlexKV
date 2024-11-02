use std::time::Duration;

use validit::Valid;

use crate::core::raft_msg::AppendEntriesTx;
use crate::core::raft_msg::ResultSender;
use crate::core::state_machine;
use crate::core::ServerState;
use crate::display_ext::DisplayInstantExt;
use crate::display_ext::DisplayOptionExt;
use crate::engine::engine_config::EngineConfig;
use crate::engine::operator::establish_operator::EstablishOperator;
use crate::engine::operator::following_operator::FollowingOperator;
use crate::engine::operator::leader_operator::LeaderOperator;
use crate::engine::operator::log_operator::LogOperator;
use crate::engine::operator::replication_operator::ReplicationOperator;
use crate::engine::operator::replication_operator::SendNone;
use crate::engine::operator::server_state_operator::ServerStateOperator;
use crate::engine::operator::snapshot_operator::SnapshotOperator;
use crate::engine::operator::vote_operator::VoteOperator;
use crate::engine::Command;
use crate::engine::EngineOutput;
use crate::engine::Respond;
use crate::entry::RaftEntry;
use crate::entry::RaftPayload;
use crate::error::ForwardToLeader;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::error::NotAllowed;
use crate::error::NotInMembers;
use crate::error::RejectAppendEntries;
use crate::proposer::leader_state::CandidateState;
use crate::proposer::Candidate;
use crate::proposer::LeaderQuorumSet;
use crate::proposer::LeaderState;
use crate::raft::responder::Responder;
use crate::raft::AppendEntriesResponse;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::LogStateReader;
use crate::raft_state::RaftState;
use crate::summary::MessageSummary;
use crate::type_config::alias::ResponderOf;
use crate::type_config::alias::SnapshotDataOf;
use crate::type_config::TypeConfigExt;
use crate::AsyncRuntime;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::Membership;
use crate::RaftLogId;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::SnapshotMeta;
use crate::Vote;

#[derive(Debug, Default)]
pub(crate) struct Engine<C>
where C: RaftTypeConfig
{
    pub(crate) config: EngineConfig<C::NodeId>,

    pub(crate) state: Valid<RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>>,

    pub(crate) seen_greater_log: bool,

    pub(crate) leader: LeaderState<C>,

    pub(crate) candidate: CandidateState<C>,

    pub(crate) output: EngineOutput<C>,
}

impl<C> Engine<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(
        init_state: RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
        config: EngineConfig<C::NodeId>,
    ) -> Self {
        Self {
            config,
            state: Valid::new(init_state),
            seen_greater_log: false,
            leader: None,
            candidate: None,
            output: EngineOutput::new(4096),
        }
    }

    pub(crate) fn new_candidate(&mut self, vote: Vote<C::NodeId>) -> &mut Candidate<C, LeaderQuorumSet<C::NodeId>> {
        let now = C::now();
        let last_log_id = self.state.last_log_id().cloned();

        let membership = self.state.membership_state.effective().membership();

        self.candidate = Some(Candidate::new(
            now,
            vote,
            last_log_id,
            membership.to_quorum_set(),
            membership.learner_ids(),
        ));

        self.candidate.as_mut().unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn testing_default(id: C::NodeId) -> Self {
        let config = EngineConfig {
            id,
            ..Default::default()
        };
        let state = RaftState::default();
        Self::new(state, config)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn startup(&mut self) {

        tracing::info!(
            "startup begin: state: {:?}, is_leader: {}, is_voter: {}",
            self.state,
            self.state.is_leader(&self.config.id),
            self.state.membership_state.effective().is_voter(&self.config.id)
        );

        if self.state.is_leader(&self.config.id) {
            self.vote_operator().update_internal_server_state();

            let mut rh = self.replication_operator();

            rh.update_local_progress(rh.state.last_log_id().cloned());

            rh.initiate_replication(SendNone::False);

            return;
        }

        let server_state = if self.state.membership_state.effective().is_voter(&self.config.id) {
            ServerState::Follower
        } else {
            ServerState::Learner
        };

        self.state.server_state = server_state;

        tracing::info!(
            "startup done: id={} target_state: {:?}",
            self.config.id,
            self.state.server_state
        );
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn initialize(&mut self, mut entry: C::Entry) -> Result<(), InitializeError<C::NodeId, C::Node>> {
        self.check_initialize()?;

        entry.set_log_id(&LogId::default());

        let m = entry.get_membership().expect("the only log entry for initializing has to be membership log");
        self.check_members_contain_me(m)?;

        self.following_operator().do_append_entries(vec![entry], 0);

        self.elect();

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn elect(&mut self) {
        let new_term = self.state.vote.leader_id().term + 1;
        let new_vote = Vote::new(new_term, self.config.id.clone());

        let candidate = self.new_candidate(new_vote.clone());

        tracing::info!("{}, new candidate: {}", func_name!(), candidate);

        let last_log_id = candidate.last_log_id().cloned();

        self.vote_operator().update_vote(&new_vote).unwrap();

        self.output.push_command(Command::SendVote {
            vote_req: VoteRequest::new(new_vote, last_log_id),
        });

        self.server_state_operator().update_server_state_if_changed();
    }

    pub(crate) fn candidate_ref(&self) -> Option<&Candidate<C, LeaderQuorumSet<C::NodeId>>> {
        self.candidate.as_ref()
    }

    pub(crate) fn candidate_mut(&mut self) -> Option<&mut Candidate<C, LeaderQuorumSet<C::NodeId>>> {
        self.candidate.as_mut()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn get_leader_operator_or_reject(
        &mut self,
        tx: Option<ResponderOf<C>>,
    ) -> Option<(LeaderOperator<C>, Option<ResponderOf<C>>)> {
        let res = self.leader_operator();
        let forward_err = match res {
            Ok(lh) => {
                return Some((lh, tx));
            }
            Err(forward_err) => forward_err,
        };

        if let Some(tx) = tx {
            tx.send(Err(forward_err.into()));
        }

        None
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_vote_req(&mut self, req: VoteRequest<C::NodeId>) -> VoteResponse<C::NodeId> {
        let now = C::now();
        let lease = self.config.timer_config.leader_lease;
        let vote = self.state.vote_ref();

        let vote_utime = self.state.vote_last_modified().unwrap_or_else(|| now - lease - Duration::from_millis(1));

        tracing::info!(req = display(req.summary()), "Engine::handle_vote_req");
        tracing::info!(
            my_vote = display(self.state.vote_ref().summary()),
            my_last_log_id = display(self.state.last_log_id().summary()),
            "Engine::handle_vote_req"
        );
        tracing::info!(
            "now; {}, vote is updated at: {}, vote is updated before {:?}, leader lease({:?}) will expire after {:?}",
            now.display(),
            vote_utime.display(),
            now - vote_utime,
            lease,
            vote_utime + lease - now
        );

        if vote.is_committed() {
            if now <= vote_utime + lease {
                tracing::info!(
                    "reject vote-request: leader lease has not yet expire; now; {:?}, vote is updatd at: {:?}, leader lease({:?}) will expire after {:?}",
                    now,
                    vote_utime,
                    lease,
                    vote_utime + lease - now
                );

                return VoteResponse::new(self.state.vote_ref(), self.state.last_log_id().cloned(), false);
            }
        }


        if req.last_log_id.as_ref() >= self.state.last_log_id() {
        } else {
            tracing::info!(
                "reject vote-request: by last_log_id: !(req.last_log_id({}) >= my_last_log_id({})",
                req.last_log_id.summary(),
                self.state.last_log_id().summary(),
            );

            return VoteResponse::new(self.state.vote_ref(), self.state.last_log_id().cloned(), false);
        }


        let res = self.vote_operator().update_vote(&req.vote);

        tracing::info!(
            req = display(req.summary()),
            result = debug(&res),
            "handle vote request result"
        );

        VoteResponse::new(self.state.vote_ref(), self.state.last_log_id().cloned(), res.is_ok())
    }

    #[tracing::instrument(level = "debug", skip(self, resp))]
    pub(crate) fn handle_vote_resp(&mut self, target: C::NodeId, resp: VoteResponse<C::NodeId>) {
        tracing::info!(
            resp = display(resp.summary()),
            target = display(&target),
            my_vote = display(self.state.vote_ref()),
            my_last_log_id = display(self.state.last_log_id().summary()),
            "{}",
            func_name!()
        );

        let Some(candidate) = self.candidate_mut() else {
            return;
        };

        if resp.vote_granted && &resp.vote == candidate.vote_ref() {
            let quorum_granted = candidate.grant_by(&target);
            if quorum_granted {
                tracing::info!("a quorum granted my vote");
                self.establish_leader();
            }
            return;
        }



        if resp.last_log_id.as_ref() > self.state.last_log_id() {
            tracing::info!(
                greater_log_id = display(resp.last_log_id.summary()),
                "seen a greater log id when {}",
                func_name!()
            );
            self.set_greater_log();
        }

        let _ = self.vote_operator().update_vote(&resp.vote);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_append_entries(
        &mut self,
        vote: &Vote<C::NodeId>,
        prev_log_id: Option<LogId<C::NodeId>>,
        entries: Vec<C::Entry>,
        tx: Option<AppendEntriesTx<C>>,
    ) -> bool {
        let res = self.append_entries(vote, prev_log_id, entries);
        let is_ok = res.is_ok();

        if let Some(tx) = tx {
            let resp: AppendEntriesResponse<C::NodeId> = res.into();
            self.output.push_command(Command::Respond {
                when: None,
                resp: Respond::new(Ok(resp), tx),
            });
        }
        is_ok
    }

    pub(crate) fn append_entries(
        &mut self,
        vote: &Vote<C::NodeId>,
        prev_log_id: Option<LogId<C::NodeId>>,
        entries: Vec<C::Entry>,
    ) -> Result<(), RejectAppendEntries<C::NodeId>> {
        self.vote_operator().update_vote(vote)?;


        let mut fh = self.following_operator();
        fh.ensure_log_consecutive(prev_log_id.clone())?;
        fh.append_entries(prev_log_id, entries);

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_commit_entries(&mut self, leader_committed: Option<LogId<C::NodeId>>) {
        let mut fh = self.following_operator();
        fh.commit_entries(leader_committed);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_install_full_snapshot(
        &mut self,
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        tx: ResultSender<C, SnapshotResponse<C::NodeId>>,
    ) {
        tracing::info!(vote = display(&vote), snapshot = display(&snapshot), "{}", func_name!());

        let vote_res = self.vote_operator().accept_vote(&vote, tx, |state, _rejected| {
            Ok(SnapshotResponse::new(state.vote_ref().clone()))
        });

        let Some(tx) = vote_res else {
            return;
        };

        let mut fh = self.following_operator();

        let cond = fh.install_full_snapshot(snapshot);
        let res = Ok(SnapshotResponse {
            vote: self.state.vote_ref().clone(),
        });

        self.output.push_command(Command::Respond {
            when: cond,
            resp: Respond::new(res, tx),
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_begin_receiving_snapshot(&mut self, tx: ResultSender<C, Box<SnapshotDataOf<C>>, Infallible>) {
        tracing::info!("{}", func_name!());
        self.output.push_command(Command::from(state_machine::Command::begin_receiving_snapshot(tx)));
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn leader_step_down(&mut self) {
        let em = &self.state.membership_state.effective();

        #[allow(clippy::collapsible_if)]
        if em.log_id().as_ref() <= self.state.committed() {
            self.vote_operator().update_internal_server_state();
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn finish_building_snapshot(&mut self, meta: SnapshotMeta<C::NodeId, C::Node>) {
        tracing::info!(snapshot_meta = display(&meta), "{}", func_name!());

        self.state.io_state_mut().set_building_snapshot(false);

        let mut h = self.snapshot_operator();

        let updated = h.update_snapshot(meta);
        if !updated {
            return;
        }

        self.log_operator().schedule_policy_based_purge();
        self.try_purge_log();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn try_purge_log(&mut self) {

        if self.leader.is_some() {
            self.replication_operator().try_purge_log();
        } else {
            self.log_operator().purge_log();
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn trigger_purge_log(&mut self, mut index: u64) {
        tracing::info!(index = display(index), "{}", func_name!());

        let snapshot_last_log_id = self.state.snapshot_last_log_id();
        let snapshot_last_log_id = if let Some(x) = snapshot_last_log_id {
            x.clone()
        } else {
            tracing::info!("no snapshot, can not purge");
            return;
        };

        let scheduled = self.state.purge_upto();

        if index < scheduled.next_index() {
            tracing::info!(
                "no update, already scheduled: {}; index: {}",
                scheduled.display(),
                index,
            );
            return;
        }

        if index > snapshot_last_log_id.index {
            tracing::info!(
                "can not purge logs not in a snapshot; index: {}, last in snapshot log id: {}",
                index,
                snapshot_last_log_id
            );
            index = snapshot_last_log_id.index;
        }

        let log_id = self.state.get_log_id(index).unwrap();

        tracing::info!(purge_upto = display(&log_id), "{}", func_name!());

        self.log_operator().update_purge_upto(log_id);
        self.try_purge_log();
    }
}

impl<C> Engine<C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    fn establish_leader(&mut self) {
        tracing::info!("{}", func_name!());

        let candidate = self.candidate.take().unwrap();
        let leader = self.establish_operator().establish(candidate);

        let Some(leader) = leader else { return };

        let vote = leader.vote_ref().clone();

        self.replication_operator().rebuild_replication_streams();

        let _res = self.vote_operator().update_vote(&vote);

        self.leader_operator()
            .unwrap()
            .leader_append_entries(vec![C::Entry::new_blank(LogId::<C::NodeId>::default())]);
    }

    fn check_initialize(&self) -> Result<(), NotAllowed<C::NodeId>> {
        if !self.state.is_initialized() {
            return Ok(());
        }

        tracing::error!(
            last_log_id = display(self.state.last_log_id().summary()),
            vote = display(self.state.vote_ref()),
            "Can not initialize"
        );

        Err(NotAllowed {
            last_log_id: self.state.last_log_id().cloned(),
            vote: self.state.vote_ref().clone(),
        })
    }

    fn check_members_contain_me(
        &self,
        m: &Membership<C::NodeId, C::Node>,
    ) -> Result<(), NotInMembers<C::NodeId, C::Node>> {
        if !m.is_voter(&self.config.id) {
            let e = NotInMembers {
                node_id: self.config.id.clone(),
                membership: m.clone(),
            };
            Err(e)
        } else {
            Ok(())
        }
    }

    pub(crate) fn is_there_greater_log(&self) -> bool {
        self.seen_greater_log
    }

    pub(crate) fn set_greater_log(&mut self) {
        self.seen_greater_log = true;
    }

    pub(crate) fn reset_greater_log(&mut self) {
        self.seen_greater_log = false;
    }

    #[allow(dead_code)]
    pub(crate) fn calc_server_state(&self) -> ServerState {
        self.state.calc_server_state(&self.config.id)
    }


    pub(crate) fn vote_operator(&mut self) -> VoteOperator<C> {
        VoteOperator {
            config: &mut self.config,
            state: &mut self.state,
            output: &mut self.output,
            leader: &mut self.leader,
            candidate: &mut self.candidate,
        }
    }

    pub(crate) fn log_operator(&mut self) -> LogOperator<C> {
        LogOperator {
            config: &mut self.config,
            state: &mut self.state,
            output: &mut self.output,
        }
    }

    pub(crate) fn snapshot_operator(&mut self) -> SnapshotOperator<C> {
        SnapshotOperator {
            state: &mut self.state,
            output: &mut self.output,
        }
    }

    pub(crate) fn leader_operator(&mut self) -> Result<LeaderOperator<C>, ForwardToLeader<C::NodeId, C::Node>> {
        let leader = match self.leader.as_mut() {
            None => {
                return Err(self.state.forward_to_leader());
            }
            Some(x) => x,
        };

        if !leader.vote.is_committed() {
            return Err(self.state.forward_to_leader());
        }

        Ok(LeaderOperator {
            config: &mut self.config,
            leader,
            state: &mut self.state,
            output: &mut self.output,
        })
    }

    pub(crate) fn replication_operator(&mut self) -> ReplicationOperator<C> {
        let leader = match self.leader.as_mut() {
            None => {
                unreachable!("There is no leader, can not handle replication");
            }
            Some(x) => x,
        };

        ReplicationOperator {
            config: &mut self.config,
            leader,
            state: &mut self.state,
            output: &mut self.output,
        }
    }

    pub(crate) fn following_operator(&mut self) -> FollowingOperator<C> {
        FollowingOperator {
            config: &mut self.config,
            state: &mut self.state,
            output: &mut self.output,
        }
    }

    pub(crate) fn server_state_operator(&mut self) -> ServerStateOperator<C> {
        ServerStateOperator {
            config: &self.config,
            state: &mut self.state,
            output: &mut self.output,
        }
    }
    pub(crate) fn establish_operator(&mut self) -> EstablishOperator<C> {
        EstablishOperator {
            config: &mut self.config,
            leader: &mut self.leader,
        }
    }
}
