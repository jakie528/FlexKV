use std::collections::BTreeSet;

use crate::quorum::quorum_set::QuorumSet;

impl<ID> QuorumSet<ID> for BTreeSet<ID>
where ID: PartialOrd + Ord + Clone + 'static
{
    type Iter = std::collections::btree_set::IntoIter<ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        let mut count = 0;
        let limit = self.len();
        for id in ids {
            if self.contains(id) {
                count += 2;
                if count > limit {
                    return true;
                }
            }
        }
        false
    }

    fn ids(&self) -> Self::Iter {
        self.clone().into_iter()
    }
}

impl<ID> QuorumSet<ID> for Vec<ID>
where ID: PartialOrd + Ord + Clone + 'static
{
    type Iter = std::collections::btree_set::IntoIter<ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        let mut count = 0;
        let limit = self.len();
        for id in ids {
            if self.contains(id) {
                count += 2;
                if count > limit {
                    return true;
                }
            }
        }
        false
    }

    fn ids(&self) -> Self::Iter {
        BTreeSet::from_iter(self.iter().cloned()).into_iter()
    }
}

impl<ID> QuorumSet<ID> for &[ID]
where ID: PartialOrd + Ord + Clone + 'static
{
    type Iter = std::collections::btree_set::IntoIter<ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        let mut count = 0;
        let limit = self.len();
        for id in ids {
            if self.contains(id) {
                count += 2;
                if count > limit {
                    return true;
                }
            }
        }
        false
    }

    fn ids(&self) -> Self::Iter {
        BTreeSet::from_iter(self.iter().cloned()).into_iter()
    }
}
