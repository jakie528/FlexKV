use crate::engine::EngineConfig;
use crate::proposer::Candidate;
use crate::proposer::Leader;
use crate::proposer::LeaderQuorumSet;
use crate::proposer::LeaderState;
use crate::RaftTypeConfig;

pub(crate) struct EstablishOperator<'x, C>
where C: RaftTypeConfig
{
    #[allow(dead_code)]
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) leader: &'x mut LeaderState<C>,
}

impl<'x, C> EstablishOperator<'x, C>
where C: RaftTypeConfig
{
    pub(crate) fn establish(
        self,
        candidate: Candidate<C, LeaderQuorumSet<C::NodeId>>,
    ) -> Option<&'x mut Leader<C, LeaderQuorumSet<C::NodeId>>> {
        let vote = candidate.vote_ref().clone();

        if let Some(l) = self.leader.as_ref() {
            #[allow(clippy::neg_cmp_op_on_partial_ord)]
            if !(&vote > l.vote_ref()) {
                tracing::warn!(
                    "vote is not greater than current existing leader vote. Do not establish new leader and quit"
                );
                return None;
            }
        }

        let leader = candidate.into_leader();
        *self.leader = Some(Box::new(leader));

        self.leader.as_mut().map(|x| x.as_mut())
    }
}
