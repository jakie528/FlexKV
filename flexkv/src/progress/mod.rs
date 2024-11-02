
pub(crate) mod entry;
pub(crate) mod inflight;

use std::borrow::Borrow;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::slice::Iter;
use std::slice::IterMut;

#[allow(unused_imports)]
pub(crate) use inflight::Inflight;

use crate::quorum::QuorumSet;

pub(crate) trait Progress<ID, V, P, QS>
where
    ID: 'static,
    V: Borrow<P>,
    QS: QuorumSet<ID>,
{
    fn update_with<F>(&mut self, id: &ID, f: F) -> Result<&P, &P>
    where F: FnOnce(&mut V);

    fn update(&mut self, id: &ID, value: V) -> Result<&P, &P> {
        self.update_with(id, |x| *x = value)
    }

    fn increase_to(&mut self, id: &ID, value: V) -> Result<&P, &P>
    where V: PartialOrd {
        self.update_with(id, |x| {
            if value > *x {
                *x = value;
            }
        })
    }

    #[allow(dead_code)]
    fn try_get(&self, id: &ID) -> Option<&V>;

    fn get_mut(&mut self, id: &ID) -> Option<&mut V>;

    #[allow(dead_code)]
    fn get(&self, id: &ID) -> &V;

    #[allow(dead_code)]
    fn granted(&self) -> &P;

    #[allow(dead_code)]
    fn quorum_set(&self) -> &QS;

    fn iter(&self) -> Iter<(ID, V)>;

    fn upgrade_quorum_set(self, quorum_set: QS, learner_ids: &[ID], default_v: V) -> Self;

    fn is_voter(&self, id: &ID) -> Option<bool>;
}

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct VecProgress<ID, V, P, QS>
where
    ID: 'static,
    QS: QuorumSet<ID>,
{
    quorum_set: QS,

    granted: P,

    voter_count: usize,

    vector: Vec<(ID, V)>,

    stat: Stat,
}

impl<ID, V, P, QS> Display for VecProgress<ID, V, P, QS>
where
    ID: PartialEq + Debug + Clone + 'static,
    V: Clone + 'static,
    V: Borrow<P>,
    P: PartialOrd + Ord + Clone + 'static,
    QS: QuorumSet<ID> + 'static,
    ID: Display,
    V: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (i, (id, v)) in self.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", id, v)?
        }
        write!(f, "}}")?;

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct Stat {
    update_count: u64,
    move_count: u64,
    is_quorum_count: u64,
}

impl<ID, V, P, QS> VecProgress<ID, V, P, QS>
where
    ID: PartialEq + Clone + Debug + 'static,
    V: Clone + 'static,
    V: Borrow<P>,
    P: PartialOrd + Ord + Clone + 'static,
    QS: QuorumSet<ID>,
{
    pub(crate) fn new(quorum_set: QS, learner_ids: impl IntoIterator<Item = ID>, default_v: V) -> Self {
        let mut vector = quorum_set.ids().map(|id| (id, default_v.clone())).collect::<Vec<_>>();

        let voter_count = vector.len();

        vector.extend(learner_ids.into_iter().map(|id| (id, default_v.clone())));

        Self {
            quorum_set,
            granted: default_v.borrow().clone(),
            voter_count,
            vector,
            stat: Default::default(),
        }
    }

    #[inline(always)]
    pub(crate) fn index(&self, target: &ID) -> Option<usize> {
        for (i, elt) in self.vector.iter().enumerate() {
            if elt.0 == *target {
                return Some(i);
            }
        }

        None
    }

    #[inline(always)]
    fn move_up(&mut self, index: usize) -> usize {
        self.stat.move_count += 1;
        for i in (0..index).rev() {
            if self.vector[i].1.borrow() < self.vector[i + 1].1.borrow() {
                self.vector.swap(i, i + 1);
            } else {
                return i + 1;
            }
        }

        0
    }

    pub(crate) fn iter_mut(&mut self) -> IterMut<(ID, V)> {
        self.vector.iter_mut()
    }

    #[allow(dead_code)]
    pub(crate) fn stat(&self) -> &Stat {
        &self.stat
    }
}

impl<ID, V, P, QS> Progress<ID, V, P, QS> for VecProgress<ID, V, P, QS>
where
    ID: PartialEq + Debug + Clone + 'static,
    V: Clone + 'static,
    V: Borrow<P>,
    P: PartialOrd + Ord + Clone + 'static,
    QS: QuorumSet<ID> + 'static,
{
    fn update_with<F>(&mut self, id: &ID, f: F) -> Result<&P, &P>
    where F: FnOnce(&mut V) {
        self.stat.update_count += 1;

        let index = match self.index(id) {
            None => {
                return Err(&self.granted);
            }
            Some(x) => x,
        };

        let elt = &mut self.vector[index];

        let prev_progress = elt.1.borrow().clone();

        f(&mut elt.1);

        let new_progress = elt.1.borrow();

        let prev_le_granted = prev_progress <= self.granted;
        let new_gt_granted = new_progress > &self.granted;

        if &prev_progress == new_progress {
            return Ok(&self.granted);
        }

        if index >= self.voter_count {
            return Ok(&self.granted);
        }


        if prev_le_granted && new_gt_granted {
            let new_index = self.move_up(index);

            for i in new_index..self.voter_count {
                let prog = self.vector[i].1.borrow();

                if prog <= &self.granted {
                    break;
                }

                let it = self.vector[0..=i].iter().map(|x| &x.0);

                self.stat.is_quorum_count += 1;

                if self.quorum_set.is_quorum(it) {
                    self.granted = prog.clone();
                    break;
                }
            }
        }

        Ok(&self.granted)
    }

    #[allow(dead_code)]
    fn try_get(&self, id: &ID) -> Option<&V> {
        let index = self.index(id)?;
        Some(&self.vector[index].1)
    }

    fn get_mut(&mut self, id: &ID) -> Option<&mut V> {
        let index = self.index(id)?;
        Some(&mut self.vector[index].1)
    }

    #[allow(dead_code)]
    fn get(&self, id: &ID) -> &V {
        let index = self.index(id).unwrap();
        &self.vector[index].1
    }

    #[allow(dead_code)]
    fn granted(&self) -> &P {
        &self.granted
    }

    #[allow(dead_code)]
    fn quorum_set(&self) -> &QS {
        &self.quorum_set
    }

    fn iter(&self) -> Iter<(ID, V)> {
        self.vector.as_slice().iter()
    }

    fn upgrade_quorum_set(self, quorum_set: QS, leaner_ids: &[ID], default_v: V) -> Self {
        let mut new_prog = Self::new(quorum_set, leaner_ids.iter().cloned(), default_v);

        new_prog.stat = self.stat.clone();

        for (id, v) in self.iter() {
            let _ = new_prog.update(id, v.clone());
        }
        new_prog
    }

    fn is_voter(&self, id: &ID) -> Option<bool> {
        let index = self.index(id)?;
        Some(index < self.voter_count)
    }
}
