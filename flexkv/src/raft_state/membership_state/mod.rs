use std::error::Error;
use std::sync::Arc;

use validit::Validate;

use crate::EffectiveMembership;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::MessageSummary;
use crate::Node;
use crate::NodeId;

mod change_handler;

pub(crate) use change_handler::ChangeHandler;

#[derive(Debug, Clone, Default)]
#[derive(PartialEq, Eq)]
pub struct MembershipState<NID, N>
where
    NID: NodeId,
    N: Node,
{
    committed: Arc<EffectiveMembership<NID, N>>,

    effective: Arc<EffectiveMembership<NID, N>>,
}

impl<NID, N> MessageSummary<MembershipState<NID, N>> for MembershipState<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn summary(&self) -> String {
        format!(
            "MembershipState{{committed: {}, effective: {}}}",
            self.committed().summary(),
            self.effective().summary()
        )
    }
}

impl<NID, N> MembershipState<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub(crate) fn new(
        committed: Arc<EffectiveMembership<NID, N>>,
        effective: Arc<EffectiveMembership<NID, N>>,
    ) -> Self {
        Self { committed, effective }
    }

    pub(crate) fn contains(&self, id: &NID) -> bool {
        self.effective.membership().contains(id)
    }

    pub(crate) fn is_voter(&self, id: &NID) -> bool {
        self.effective.membership().is_voter(id)
    }

    pub(crate) fn commit(&mut self, committed_log_id: &Option<LogId<NID>>) {
        if committed_log_id >= self.effective().log_id() {
            self.committed = self.effective.clone();
        }
    }

    pub(crate) fn update_committed(
        &mut self,
        c: Arc<EffectiveMembership<NID, N>>,
    ) -> Option<Arc<EffectiveMembership<NID, N>>> {
        let mut changed = false;

        if c.log_id().index() >= self.effective.log_id().index() {
            changed = c.membership() != self.effective.membership();

            self.effective = c.clone()
        }

        if c.log_id() > self.committed.log_id() {
            self.committed = c
        }

        if changed {
            Some(self.effective().clone())
        } else {
            None
        }
    }

    pub(crate) fn append(&mut self, m: Arc<EffectiveMembership<NID, N>>) {

        self.committed = self.effective.clone();
        self.effective = m;
    }

    pub(crate) fn truncate(&mut self, since: u64) -> Option<Arc<EffectiveMembership<NID, N>>> {

        if Some(since) <= self.effective().log_id().index() {

            self.effective = self.committed.clone();
            return Some(self.effective.clone());
        }
        None
    }

    pub fn committed(&self) -> &Arc<EffectiveMembership<NID, N>> {
        &self.committed
    }

    pub fn effective(&self) -> &Arc<EffectiveMembership<NID, N>> {
        &self.effective
    }

    pub(crate) fn change_handler(&self) -> ChangeHandler<NID, N> {
        ChangeHandler { state: self }
    }
}

impl<NID, N> Validate for MembershipState<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        validit::less_equal!(self.committed.log_id(), self.effective.log_id());
        validit::less_equal!(self.committed.log_id().index(), self.effective.log_id().index());
        Ok(())
    }
}
