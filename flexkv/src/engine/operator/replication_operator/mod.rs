use std::ops::Deref;

use crate::engine::operator::log_operator::LogOperator;
use crate::engine::operator::snapshot_operator::SnapshotOperator;
use crate::engine::Command;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::progress::entry::ProgressEntry;
use crate::progress::Inflight;
use crate::progress::Progress;
use crate::proposer::Leader;
use crate::proposer::LeaderQuorumSet;
use crate::raft_state::LogStateReader;
use crate::replication::request_id::RequestId;
use crate::replication::response::ReplicationResult;
use crate::type_config::alias::InstantOf;
use crate::AsyncRuntime;
use crate::EffectiveMembership;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::Membership;
use crate::RaftState;
use crate::RaftTypeConfig;

pub(crate) struct ReplicationOperator<'x, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) leader: &'x mut Leader<C, LeaderQuorumSet<C::NodeId>>,
    pub(crate) state: &'x mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'x mut EngineOutput<C>,
}

#[derive(Debug)]
#[derive(PartialEq, Eq)]
pub(crate) enum SendNone {
    False,
    True,
}

impl<'x, C> ReplicationOperator<'x, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn append_membership(&mut self, log_id: &LogId<C::NodeId>, m: &Membership<C::NodeId, C::Node>) {
        self.state.membership_state.append(EffectiveMembership::new_arc(Some(log_id.clone()), m.clone()));



        self.rebuild_progresses();
        self.rebuild_replication_streams();
        self.initiate_replication(SendNone::False);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn rebuild_progresses(&mut self) {
        let em = self.state.membership_state.effective();

        let learner_ids = em.learner_ids().collect::<Vec<_>>();

        {
            let end = self.state.last_log_id().next_index();
            let default_v = ProgressEntry::empty(end);

            let old_progress = self.leader.progress.clone();

            self.leader.progress =
                old_progress.upgrade_quorum_set(em.membership().to_quorum_set(), &learner_ids, default_v);
        }

        {
            let old_progress = self.leader.clock_progress.clone();

            self.leader.clock_progress =
                old_progress.upgrade_quorum_set(em.membership().to_quorum_set(), &learner_ids, None);
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_success_progress(
        &mut self,
        target: C::NodeId,
        request_id: RequestId,
        result: ReplicationResult<C>,
    ) {
        self.update_leader_clock(target.clone(), result.sending_time);

        let id = request_id.request_id();
        let Some(id) = id else {
            return;
        };

        match result.result {
            Ok(matching) => {
                self.update_matching(target.clone(), id, matching);
            }
            Err(conflict) => {
                self.update_conflicting(target, id, conflict);
            }
        }
    }

    pub(crate) fn update_leader_clock(&mut self, node_id: C::NodeId, t: InstantOf<C>) {
        let _ = *self.leader
            .clock_progress
            .increase_to(&node_id, Some(t))
            .expect("it should always update existing progress");

    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_matching(&mut self, node_id: C::NodeId, inflight_id: u64, log_id: Option<LogId<C::NodeId>>) {

        let quorum_accepted = self
            .leader
            .progress
            .update_with(&node_id, |prog_entry| {
                let res = prog_entry.update_matching(inflight_id, log_id);
                if let Err(e) = &res {
                    tracing::error!(error = display(e), "update_matching");
                    panic!("update_matching error: {}", e);
                }
            })
            .expect("it should always update existing progress")
            .clone();

        self.try_commit_quorum_accepted(quorum_accepted);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn try_commit_quorum_accepted(&mut self, granted: Option<LogId<C::NodeId>>) {
        if let Some(c) = granted.clone() {
            if !self.state.vote_ref().is_same_leader(c.committed_leader_id()) {
                return;
            }
        }

        if let Some(prev_committed) = self.state.update_committed(&granted) {
            self.output.push_command(Command::ReplicateCommitted {
                committed: self.state.committed().cloned(),
            });

            let seq = self.output.next_sm_seq();
            self.output.push_command(Command::Commit {
                seq,
                already_committed: prev_committed,
                upto: self.state.committed().cloned().unwrap(),
            });

            if self.config.snapshot_policy.should_snapshot(&self.state) {
                self.snapshot_operator().trigger_snapshot();
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_conflicting(&mut self, target: C::NodeId, inflight_id: u64, conflict: LogId<C::NodeId>) {

        let prog_entry = self.leader.progress.get_mut(&target).unwrap();

        prog_entry.update_conflicting(inflight_id, conflict.index).unwrap();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_progress(
        &mut self,
        target: C::NodeId,
        request_id: RequestId,
        repl_res: Result<ReplicationResult<C>, String>,
    ) {
        match repl_res {
            Ok(p) => {
                self.update_success_progress(target.clone(), request_id, p);
            }
            Err(err_str) => {
                tracing::warn!(
                    request_id = display(request_id),
                    result = display(&err_str),
                    "update progress error"
                );

                if request_id == RequestId::HeartBeat {
                    tracing::warn!("heartbeat error: {}, no update to inflight data", err_str);
                } else {
                    let p = self.leader.progress.get_mut(&target).unwrap();

                    p.inflight = Inflight::None;
                }
            }
        };

        self.try_purge_log();


        {
            let p = self.leader.progress.get_mut(&target).unwrap();

            let r = p.next_send(self.state.deref(), self.config.max_payload_entries);

            if let Ok(inflight) = r {
                Self::send_to_target(self.output, &target, inflight);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn rebuild_replication_streams(&mut self) {
        let mut targets = vec![];

        for (target, prog_entry) in self.leader.progress.iter_mut() {
            if target != &self.config.id {
                prog_entry.inflight = Inflight::None;

                targets.push((target.clone(), prog_entry.clone()));
            }
        }
        self.output.push_command(Command::RebuildReplicationStreams { targets });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn initiate_replication(&mut self, send_none: SendNone) {

        for (id, prog_entry) in self.leader.progress.iter_mut() {
            if id == &self.config.id {
                continue;
            }

            let t = prog_entry.next_send(self.state, self.config.max_payload_entries);

            match t {
                Ok(inflight) => {
                    Self::send_to_target(self.output, id, inflight);
                }
                Err(e) => {

                    #[allow(clippy::collapsible_if)]
                    if e == &Inflight::None {
                        if send_none == SendNone::True {
                            Self::send_to_target(self.output, id, e);
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn send_to_target(output: &mut EngineOutput<C>, target: &C::NodeId, inflight: &Inflight<C::NodeId>) {
        output.push_command(Command::Replicate {
            target: target.clone(),
            req: inflight.clone(),
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn try_purge_log(&mut self) {

        if self.state.purge_upto() <= self.state.last_purged_log_id() {
            return;
        }

        let purge_upto = self.state.purge_upto().unwrap().clone();

        let mut in_use = false;
        for (_, prog_entry) in self.leader.progress.iter() {
            if prog_entry.is_log_range_inflight(&purge_upto) {
                in_use = true;
            }
        }

        if in_use {
            return;
        }

        self.log_operator().purge_log();
    }

    pub(crate) fn update_local_progress(&mut self, upto: Option<LogId<C::NodeId>>) {

        if upto.is_none() {
            return;
        }

        let id = self.config.id.clone();

        if let Some(prog_entry) = self.leader.progress.get_mut(&id) {
            if prog_entry.matching >= upto {
                return;
            }
            prog_entry.inflight = Inflight::logs(None, upto.clone());

            let inflight_id = prog_entry.inflight.get_id().unwrap();
            self.update_matching(id, inflight_id, upto);
        }
    }

    pub(crate) fn log_operator(&mut self) -> LogOperator<C> {
        LogOperator {
            config: self.config,
            state: self.state,
            output: self.output,
        }
    }

    fn snapshot_operator(&mut self) -> SnapshotOperator<C> {
        SnapshotOperator {
            state: self.state,
            output: self.output,
        }
    }
}
