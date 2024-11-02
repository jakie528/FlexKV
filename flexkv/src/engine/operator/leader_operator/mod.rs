use crate::engine::operator::replication_operator::ReplicationOperator;
use crate::engine::operator::replication_operator::SendNone;
use crate::engine::Command;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::entry::RaftPayload;
use crate::proposer::Leader;
use crate::proposer::LeaderQuorumSet;
use crate::raft_state::LogStateReader;
use crate::type_config::alias::LogIdOf;
use crate::AsyncRuntime;
use crate::RaftLogId;
use crate::RaftState;
use crate::RaftTypeConfig;

pub(crate) struct LeaderOperator<'x, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) leader: &'x mut Leader<C, LeaderQuorumSet<C::NodeId>>,
    pub(crate) state: &'x mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'x mut EngineOutput<C>,
}

impl<'x, C> LeaderOperator<'x, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip(self, entries))]
    pub(crate) fn leader_append_entries(&mut self, mut entries: Vec<C::Entry>) {
        let l = entries.len();
        if l == 0 {
            return;
        }

        self.leader.assign_log_ids(&mut entries);

        self.state.extend_log_ids_from_same_leader(&entries);

        let mut membership_entry = None;
        for entry in entries.iter() {
            if let Some(m) = entry.get_membership() {
                membership_entry = Some((entry.get_log_id().clone(), m.clone()));
            }
        }

        self.output.push_command(Command::AppendInputEntries {
            vote: self.leader.vote.clone(),
            entries,
        });

        let mut rh = self.replication_operator();

        if let Some((log_id, m)) = membership_entry {
            rh.append_membership(&log_id, &m);
        }

        rh.initiate_replication(SendNone::False);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn send_heartbeat(&mut self) -> () {
        let mut rh = self.replication_operator();
        rh.initiate_replication(SendNone::True);
    }

    pub(crate) fn get_read_log_id(&self) -> Option<LogIdOf<C>> {
        let committed = self.state.committed().cloned();
        std::cmp::max(self.leader.noop_log_id.clone(), committed)
    }

    pub(crate) fn replication_operator(&mut self) -> ReplicationOperator<C> {
        ReplicationOperator {
            config: self.config,
            leader: self.leader,
            state: self.state,
            output: self.output,
        }
    }
}
