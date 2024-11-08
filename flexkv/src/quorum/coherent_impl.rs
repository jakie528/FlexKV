use crate::quorum::coherent::FindCoherent;
use crate::quorum::Coherent;
use crate::quorum::Joint;
use crate::quorum::QuorumSet;

impl<ID, QS> Coherent<ID, Joint<ID, QS, Vec<QS>>> for Joint<ID, QS, Vec<QS>>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID> + PartialEq,
{
    fn is_coherent_with(&self, other: &Joint<ID, QS, Vec<QS>>) -> bool {
        for a in self.children() {
            for b in other.children() {
                if a == b {
                    return true;
                }
            }
        }

        false
    }
}

impl<ID, QS> Coherent<ID, Joint<ID, QS, &[QS]>> for Joint<ID, QS, &[QS]>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID> + PartialEq,
{
    fn is_coherent_with(&self, other: &Joint<ID, QS, &[QS]>) -> bool {
        for a in self.children().iter() {
            for b in other.children().iter() {
                if a == b {
                    return true;
                }
            }
        }

        false
    }
}

impl<ID, QS> Coherent<ID, QS> for Joint<ID, QS, Vec<QS>>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID> + PartialEq,
{
    fn is_coherent_with(&self, other: &QS) -> bool {
        for a in self.children().iter() {
            if a == other {
                return true;
            }
        }

        false
    }
}

impl<ID, QS> FindCoherent<ID, QS> for Joint<ID, QS, Vec<QS>>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID> + PartialEq + Clone,
{
    fn find_coherent(&self, other: QS) -> Self {
        if self.is_coherent_with(&other) {
            Joint::from(vec![other])
        } else {
            let last = self.children().last();
            if let Some(last) = last {
                Joint::from(vec![last.clone(), other])
            } else {
                Joint::from(vec![other])
            }
        }
    }
}
