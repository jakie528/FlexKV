use crate::NodeId;
use crate::Vote;

#[allow(dead_code)]
pub(crate) trait VoteStateReader<NID: NodeId> {
    fn vote_ref(&self) -> &Vote<NID>;
}
