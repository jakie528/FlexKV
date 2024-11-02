use crate::error::ChangeMembershipError;
use crate::error::InProgress;
use crate::ChangeMembers;
use crate::Membership;
use crate::MembershipState;
use crate::Node;
use crate::NodeId;

pub(crate) struct ChangeHandler<'m, NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub(crate) state: &'m MembershipState<NID, N>,
}

impl<'m, NID, N> ChangeHandler<'m, NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub(crate) fn apply(
        &self,
        change: ChangeMembers<NID, N>,
        retain: bool,
    ) -> Result<Membership<NID, N>, ChangeMembershipError<NID>> {
        self.ensure_committed()?;

        let new_membership = self.state.effective().membership().clone().change(change, retain)?;
        Ok(new_membership)
    }

    pub(crate) fn ensure_committed(&self) -> Result<(), InProgress<NID>> {
        let effective = self.state.effective();
        let committed = self.state.committed();

        if effective.log_id() == committed.log_id() {
            Ok(())
        } else {
            Err(InProgress {
                committed: committed.log_id().clone(),
                membership_log_id: effective.log_id().clone(),
            })
        }
    }
}
