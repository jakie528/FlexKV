use std::sync::Arc;

use crate::core::state_machine;
use crate::engine::operator::log_operator::LogOperator;
use crate::engine::operator::server_state_operator::ServerStateOperator;
use crate::engine::operator::snapshot_operator::SnapshotOperator;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::entry::RaftPayload;
use crate::error::RejectAppendEntries;
use crate::raft_state::LogStateReader;
use crate::AsyncRuntime;
use crate::EffectiveMembership;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::MessageSummary;
use crate::RaftLogId;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::StoredMembership;

pub(crate) struct FollowingOperator<'x, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) state: &'x mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'x mut EngineOutput<C>,
}

impl<'x, C> FollowingOperator<'x, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn append_entries(&mut self, prev_log_id: Option<LogId<C::NodeId>>, entries: Vec<C::Entry>) {

        let last_log_id = entries.last().map(|x| x.get_log_id().clone());

        self.state.update_accepted(std::cmp::max(prev_log_id, last_log_id));

        let l = entries.len();
        let since = self.state.first_conflicting_index(&entries);
        if since < l {
            self.truncate_logs(entries[since].get_log_id().index);
        }

        self.do_append_entries(entries, since);
    }

    pub(crate) fn ensure_log_consecutive(
        &mut self,
        prev_log_id: Option<LogId<C::NodeId>>,
    ) -> Result<(), RejectAppendEntries<C::NodeId>> {
        if let Some(ref prev) = prev_log_id {
            if !self.state.has_log_id(prev) {
                let local = self.state.get_log_id(prev.index);

                self.truncate_logs(prev.index);
                return Err(RejectAppendEntries::ByConflictingLogId {
                    local,
                    expect: prev.clone(),
                });
            }
        }


        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entries))]
    pub(crate) fn do_append_entries(&mut self, mut entries: Vec<C::Entry>, since: usize) {
        let l = entries.len();

        if since == l {
            return;
        }

        let entries = entries.split_off(since);

        self.state.extend_log_ids(&entries);
        self.append_membership(entries.iter());

        self.output.push_command(Command::AppendInputEntries {
            vote: self.state.vote_ref().clone(),
            entries,
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn commit_entries(&mut self, leader_committed: Option<LogId<C::NodeId>>) {
        let accepted = self.state.accepted().cloned();
        let committed = std::cmp::min(accepted.clone(), leader_committed.clone());

        if let Some(prev_committed) = self.state.update_committed(&committed) {
            let seq = self.output.next_sm_seq();
            self.output.push_command(Command::Commit {
                seq,
                already_committed: prev_committed,
                upto: committed.unwrap(),
            });

            if self.config.snapshot_policy.should_snapshot(&self.state) {
                self.snapshot_operator().trigger_snapshot();
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn truncate_logs(&mut self, since: u64) {

        let since_log_id = match self.state.get_log_id(since) {
            None => {
                return;
            }
            Some(x) => x,
        };

        self.state.log_ids.truncate(since);
        self.output.push_command(Command::DeleteConflictLog { since: since_log_id });

        let changed = self.state.membership_state.truncate(since);
        if let Some(_c) = changed {
            self.server_state_operator().update_server_state_if_changed();
        }
    }

    fn append_membership<'a>(&mut self, entries: impl DoubleEndedIterator<Item = &'a C::Entry>)
    where C::Entry: 'a {
        let memberships = Self::last_two_memberships(entries);
        if memberships.is_empty() {
            return;
        }

        for (_i, m) in memberships.into_iter().enumerate() {
            self.state.membership_state.append(Arc::new(EffectiveMembership::new_from_stored_membership(m)));
        }

        self.server_state_operator().update_server_state_if_changed();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn update_committed_membership(&mut self, membership: EffectiveMembership<C::NodeId, C::Node>) {

        let m = Arc::new(membership);

        let _effective_changed = self.state.membership_state.update_committed(m);

        self.server_state_operator().update_server_state_if_changed();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn install_full_snapshot(&mut self, snapshot: Snapshot<C>) -> Option<Condition<C::NodeId>> {
        let meta = &snapshot.meta;
        tracing::info!("install_full_snapshot: meta:{:?}", meta);

        let snap_last_log_id = meta.last_log_id.clone();

        if snap_last_log_id.as_ref() <= self.state.committed() {
            tracing::info!(
                "No need to install snapshot; snapshot last_log_id({}) <= committed({})",
                snap_last_log_id.summary(),
                self.state.committed().summary()
            );
            return None;
        }

        let snap_last_log_id = snap_last_log_id.unwrap();


        let mut snap_operator = self.snapshot_operator();
        let updated = snap_operator.update_snapshot(meta.clone());
        if !updated {
            return None;
        }

        let local = self.state.get_log_id(snap_last_log_id.index);
        if let Some(local) = local {
            if local != snap_last_log_id {
                self.truncate_logs(self.state.committed().next_index());
            }
        }

        self.state.update_accepted(Some(snap_last_log_id.clone()));
        self.state.committed = Some(snap_last_log_id.clone());
        self.update_committed_membership(EffectiveMembership::new_from_stored_membership(
            meta.last_membership.clone(),
        ));

        self.output.push_command(Command::from(state_machine::Command::install_full_snapshot(snapshot)));
        let last_sm_seq = self.output.last_sm_seq();

        self.state.purge_upto = Some(snap_last_log_id);
        self.log_operator().purge_log();

        Some(Condition::StateMachineCommand {
            command_seq: last_sm_seq,
        })
    }

    fn last_two_memberships<'a>(
        entries: impl DoubleEndedIterator<Item = &'a C::Entry>,
    ) -> Vec<StoredMembership<C::NodeId, C::Node>>
    where C::Entry: 'a {
        let mut memberships = vec![];

        for ent in entries.rev() {
            if let Some(m) = ent.get_membership() {
                memberships.insert(0, StoredMembership::new(Some(ent.get_log_id().clone()), m.clone()));
                if memberships.len() == 2 {
                    break;
                }
            }
        }

        memberships
    }

    fn log_operator(&mut self) -> LogOperator<C> {
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

    fn server_state_operator(&mut self) -> ServerStateOperator<C> {
        ServerStateOperator {
            config: self.config,
            state: self.state,
            output: self.output,
        }
    }
}
