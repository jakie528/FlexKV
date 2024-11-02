use std::sync::Arc;

pub(crate) trait QuorumSet<ID: 'static> {
    type Iter: Iterator<Item = ID>;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool;

    fn ids(&self) -> Self::Iter;
}

impl<ID: 'static, T: QuorumSet<ID>> QuorumSet<ID> for Arc<T> {
    type Iter = T::Iter;

    fn is_quorum<'a, I: Iterator<Item = &'a ID> + Clone>(&self, ids: I) -> bool {
        self.as_ref().is_quorum(ids)
    }

    fn ids(&self) -> Self::Iter {
        self.as_ref().ids()
    }
}
