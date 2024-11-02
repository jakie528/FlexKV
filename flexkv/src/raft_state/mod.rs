use std::error::Error;
use std::ops::Deref;

use validit::Validate;

use crate::engine::LogIdList;
use crate::error::ForwardToLeader;
use crate::log_id::RaftLogId;
use crate::node::Node;
use crate::utime::UTime;
use crate::Instant;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;
use crate::RaftTypeConfig;
use crate::ServerState;
use crate::SnapshotMeta;
use crate::Vote;

mod accepted;
pub(crate) mod io_state;
mod log_state_reader;
mod membership_state;
mod vote_state_reader;

#[allow(unused)]
pub(crate) use io_state::log_io_id::LogIOId;
pub(crate) use io_state::IOState;


pub(crate) use accepted::Accepted;
pub(crate) use log_state_reader::LogStateReader;
pub use membership_state::MembershipState;
pub(crate) use vote_state_reader::VoteStateReader;

use crate::proposer::Leader;
use crate::proposer::LeaderQuorumSet;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub struct RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    pub(crate) vote: UTime<Vote<NID>, I>,

    pub committed: Option<LogId<NID>>,

    pub(crate) purged_next: u64,

    pub log_ids: LogIdList<NID>,

    pub membership_state: MembershipState<NID, N>,

    pub snapshot_meta: SnapshotMeta<NID, N>,

    pub server_state: ServerState,

    pub(crate) accepted: Accepted<NID>,

    pub(crate) io_state: IOState<NID>,

    pub(crate) purge_upto: Option<LogId<NID>>,
}

impl<NID, N, I> Default for RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    fn default() -> Self {
        Self {
            vote: UTime::default(),
            committed: None,
            purged_next: 0,
            log_ids: LogIdList::default(),
            membership_state: MembershipState::default(),
            snapshot_meta: SnapshotMeta::default(),
            server_state: ServerState::default(),
            accepted: Accepted::default(),
            io_state: IOState::default(),
            purge_upto: None,
        }
    }
}

impl<NID, N, I> LogStateReader<NID> for RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    fn get_log_id(&self, index: u64) -> Option<LogId<NID>> {
        self.log_ids.get(index)
    }

    fn last_log_id(&self) -> Option<&LogId<NID>> {
        self.log_ids.last()
    }

    fn committed(&self) -> Option<&LogId<NID>> {
        self.committed.as_ref()
    }

    fn io_applied(&self) -> Option<&LogId<NID>> {
        self.io_state.applied()
    }

    fn io_snapshot_last_log_id(&self) -> Option<&LogId<NID>> {
        self.io_state.snapshot()
    }

    fn io_purged(&self) -> Option<&LogId<NID>> {
        self.io_state.purged()
    }

    fn snapshot_last_log_id(&self) -> Option<&LogId<NID>> {
        self.snapshot_meta.last_log_id.as_ref()
    }

    fn purge_upto(&self) -> Option<&LogId<NID>> {
        self.purge_upto.as_ref()
    }

    fn last_purged_log_id(&self) -> Option<&LogId<NID>> {
        if self.purged_next == 0 {
            return None;
        }
        self.log_ids.first()
    }
}

impl<NID, N, I> VoteStateReader<NID> for RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    fn vote_ref(&self) -> &Vote<NID> {
        self.vote.deref()
    }
}

impl<NID, N, I> Validate for RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.purged_next == 0 {
            validit::less_equal!(self.log_ids.first().index(), Some(0));
        } else {
            validit::equal!(self.purged_next, self.log_ids.first().next_index());
        }

        validit::less_equal!(self.last_purged_log_id(), self.purge_upto());
        if self.snapshot_last_log_id().is_none() {
            validit::less_equal!(self.purge_upto(), self.committed());
        } else {
            validit::less_equal!(self.purge_upto(), self.snapshot_last_log_id());
        }
        validit::less_equal!(self.snapshot_last_log_id(), self.committed());
        validit::less_equal!(self.committed(), self.last_log_id());

        self.membership_state.validate()?;

        Ok(())
    }
}

impl<NID, N, I> RaftState<NID, N, I>
where
    NID: NodeId,
    N: Node,
    I: Instant,
{
    pub fn vote_ref(&self) -> &Vote<NID> {
        self.vote.deref()
    }

    pub fn vote_last_modified(&self) -> Option<I> {
        self.vote.utime()
    }

    pub(crate) fn is_initialized(&self) -> bool {
        if self.last_log_id().is_some() {
            return true;
        }

        if self.vote_ref() != &Vote::default() {
            return true;
        }

        false
    }

    pub(crate) fn accepted(&self) -> Option<&LogId<NID>> {
        self.accepted.last_accepted_log_id(self.vote_ref().leader_id())
    }

    pub(crate) fn update_accepted(&mut self, accepted: Option<LogId<NID>>) {
        if accepted.as_ref() > self.accepted.last_accepted_log_id(self.vote_ref().leader_id()) {
            self.accepted = Accepted::new(self.vote_ref().leader_id().clone(), accepted);
        }
    }

    pub(crate) fn extend_log_ids_from_same_leader<'a, LID: RaftLogId<NID> + 'a>(&mut self, new_log_ids: &[LID]) {
        self.log_ids.extend_from_same_leader(new_log_ids)
    }

    pub(crate) fn extend_log_ids<'a, LID: RaftLogId<NID> + 'a>(&mut self, new_log_id: &[LID]) {
        self.log_ids.extend(new_log_id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_committed(&mut self, committed: &Option<LogId<NID>>) -> Option<Option<LogId<NID>>> {
        if committed.as_ref() > self.committed() {
            let prev = self.committed().cloned();

            self.committed = committed.clone();
            self.membership_state.commit(committed);

            Some(prev)
        } else {
            None
        }
    }

    pub(crate) fn io_state_mut(&mut self) -> &mut IOState<NID> {
        &mut self.io_state
    }

    pub(crate) fn io_state(&self) -> &IOState<NID> {
        &self.io_state
    }

    pub(crate) fn first_conflicting_index<Ent>(&self, entries: &[Ent]) -> usize
    where Ent: RaftLogId<NID> {
        let l = entries.len();

        for (i, ent) in entries.iter().enumerate() {
            let log_id = ent.get_log_id();

            if !self.has_log_id(log_id) {
                return i;
            }
        }
        l
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn purge_log(&mut self, upto: &LogId<NID>) {
        self.purged_next = upto.index + 1;
        self.log_ids.purge(upto);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn calc_server_state(&self, id: &NID) -> ServerState {

        #[allow(clippy::collapsible_else_if)]
        if self.is_leader(id) {
            ServerState::Leader
        } else if self.is_leading(id) {
            ServerState::Candidate
        } else {
            if self.is_voter(id) {
                ServerState::Follower
            } else {
                ServerState::Learner
            }
        }
    }

    pub(crate) fn is_voter(&self, id: &NID) -> bool {
        self.membership_state.is_voter(id)
    }

    pub(crate) fn is_leading(&self, id: &NID) -> bool {
        self.membership_state.contains(id) && self.vote.leader_id().voted_for().as_ref() == Some(id)
    }

    pub(crate) fn is_leader(&self, id: &NID) -> bool {
        self.is_leading(id) && self.vote.is_committed()
    }

    pub(crate) fn new_leader<C>(&mut self) -> Leader<C, LeaderQuorumSet<C::NodeId>>
    where C: RaftTypeConfig<NodeId = NID> {
        let em = &self.membership_state.effective().membership();

        let last_leader_log_ids = self.log_ids.by_last_leader();

        Leader::new(
            self.vote_ref().clone(),
            em.to_quorum_set(),
            em.learner_ids(),
            last_leader_log_ids,
        )
    }

    pub(crate) fn forward_to_leader(&self) -> ForwardToLeader<NID, N> {
        let vote = self.vote_ref();

        if vote.is_committed() {
            let id = vote.leader_id().voted_for().unwrap();

            let node = self.membership_state.effective().get_node(&id);
            if let Some(n) = node {
                return ForwardToLeader::new(id, n.clone());
            }
        };

        ForwardToLeader::empty()
    }
}
