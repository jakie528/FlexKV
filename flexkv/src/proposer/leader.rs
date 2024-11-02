use std::fmt;

use crate::progress::entry::ProgressEntry;
use crate::progress::Progress;
use crate::progress::VecProgress;
use crate::quorum::QuorumSet;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::LogIdOf;
use crate::Instant;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::RaftLogId;
use crate::RaftTypeConfig;
use crate::Vote;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct Leader<C, QS: QuorumSet<C::NodeId>>
where C: RaftTypeConfig
{
    pub(crate) vote: Vote<C::NodeId>,

    last_log_id: Option<LogId<C::NodeId>>,

    pub(crate) noop_log_id: Option<LogId<C::NodeId>>,

    pub(crate) progress: VecProgress<C::NodeId, ProgressEntry<C::NodeId>, Option<LogId<C::NodeId>>, QS>,

    pub(crate) clock_progress: VecProgress<C::NodeId, Option<InstantOf<C>>, Option<InstantOf<C>>, QS>,
}

impl<C, QS> Leader<C, QS>
where
    C: RaftTypeConfig,
    QS: QuorumSet<C::NodeId> + Clone + fmt::Debug + 'static,
{
    pub(crate) fn new(
        vote: Vote<C::NodeId>,
        quorum_set: QS,
        learner_ids: impl IntoIterator<Item = C::NodeId>,
        last_leader_log_id: &[LogIdOf<C>],
    ) -> Self {
        let learner_ids = learner_ids.into_iter().collect::<Vec<_>>();

        let vote_leader_id = vote.committed_leader_id().unwrap();
        let first = last_leader_log_id.first();

        let noop_log_id = if first.map(|x| x.committed_leader_id().clone()) == Some(vote_leader_id) {
            first.cloned()
        } else {
            Some(LogId::new(
                vote.committed_leader_id().unwrap(),
                last_leader_log_id.last().next_index(),
            ))
        };

        let last_log_id = last_leader_log_id.last().cloned();

        Self {
            vote,
            last_log_id: last_log_id.clone(),
            noop_log_id,
            progress: VecProgress::new(
                quorum_set.clone(),
                learner_ids.iter().cloned(),
                ProgressEntry::empty(last_log_id.next_index()),
            ),
            clock_progress: VecProgress::new(quorum_set, learner_ids, None),
        }
    }

    pub(crate) fn noop_log_id(&self) -> Option<&LogIdOf<C>> {
        self.noop_log_id.as_ref()
    }

    pub(crate) fn last_log_id(&self) -> Option<&LogId<C::NodeId>> {
        self.last_log_id.as_ref()
    }

    pub(crate) fn vote_ref(&self) -> &Vote<C::NodeId> {
        &self.vote
    }

    pub(crate) fn assign_log_ids<'a, LID: RaftLogId<C::NodeId> + 'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a mut LID>,
    ) {
        let committed_leader_id = self.vote.committed_leader_id().unwrap();

        let first = LogId::new(committed_leader_id, self.last_log_id().next_index());
        let mut last = first.clone();

        for entry in entries {
            entry.set_log_id(&last);
            last.index += 1;
        }

        if last.index > first.index {
            last.index -= 1;
            self.last_log_id = Some(last);
        }
    }

    pub(crate) fn last_quorum_acked_time(&mut self) -> Option<InstantOf<C>> {

        let node_id = self.vote.leader_id().voted_for().unwrap();
        let now = Instant::now();

        let granted = self.clock_progress.increase_to(&node_id, Some(now));

        match granted {
            Ok(x) => *x,
            Err(x) => *x,
        }
    }
}
