use crate::quorum::QuorumSet;

pub(crate) trait Coherent<ID, Other>
where
    ID: PartialOrd + Ord + 'static,
    Self: QuorumSet<ID>,
    Other: QuorumSet<ID>,
{
    fn is_coherent_with(&self, other: &Other) -> bool;
}

pub(crate) trait FindCoherent<ID, Other>
where
    ID: PartialOrd + Ord + 'static,
    Self: QuorumSet<ID>,
    Other: QuorumSet<ID>,
{
    fn find_coherent(&self, other: Other) -> Self;
}
