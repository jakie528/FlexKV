use std::marker::PhantomData;

use maplit::btreeset;

use crate::quorum::QuorumSet;

pub(crate) trait AsJoint<'d, ID, QS, D>
where
    ID: 'static,
    QS: QuorumSet<ID>,
{
    fn as_joint(&'d self) -> Joint<ID, QS, D>
    where D: 'd;
}

#[derive(Clone, Debug, Default)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub(crate) struct Joint<ID, QS, D>
where
    ID: 'static,
    QS: QuorumSet<ID>,
{
    data: D,
    _p: PhantomData<(ID, QS)>,
}

impl<ID, QS, D> Joint<ID, QS, D>
where
    ID: 'static,
    QS: QuorumSet<ID>,
{
    pub(crate) fn new(data: D) -> Self {
        Self { data, _p: PhantomData }
    }

    pub(crate) fn children(&self) -> &D {
        &self.data
    }
}

impl<'d, ID, QS> QuorumSet<ID> for Joint<ID, QS, &'d [QS]>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID>,
{
    type Iter = std::collections::btree_set::IntoIter<ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        for child in self.data.iter() {
            if !child.is_quorum(ids.clone()) {
                return false;
            }
        }
        true
    }

    fn ids(&self) -> Self::Iter {
        let mut ids = btreeset! {};
        for child in self.data.iter() {
            ids.extend(child.ids())
        }
        ids.into_iter()
    }
}

impl<ID, QS> QuorumSet<ID> for Joint<ID, QS, Vec<QS>>
where
    ID: PartialOrd + Ord + 'static,
    QS: QuorumSet<ID>,
{
    type Iter = std::collections::btree_set::IntoIter<ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        for child in self.data.iter() {
            if !child.is_quorum(ids.clone()) {
                return false;
            }
        }
        true
    }

    fn ids(&self) -> Self::Iter {
        let mut ids = btreeset! {};
        for child in self.data.iter() {
            ids.extend(child.ids())
        }
        ids.into_iter()
    }
}
