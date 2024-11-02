use crate::log_id::RaftLogId;
use crate::storage::RaftLogReaderExt;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;
use crate::RaftTypeConfig;
use crate::StorageError;

#[derive(Default, Debug, Clone)]
#[derive(PartialEq, Eq)]
pub struct LogIdList<NID: NodeId> {
    key_log_ids: Vec<LogId<NID>>,
}

impl<NID> LogIdList<NID>
where NID: NodeId
{
    pub(crate) async fn load_log_ids<C, LRX>(
        last_purged_log_id: Option<LogId<NID>>,
        last_log_id: Option<LogId<NID>>,
        sto: &mut LRX,
    ) -> Result<LogIdList<NID>, StorageError<NID>>
    where
        C: RaftTypeConfig<NodeId = NID>,
        LRX: RaftLogReaderExt<C>,
    {
        let mut res = vec![];

        let last = match last_log_id {
            None => return Ok(LogIdList::new(res)),
            Some(x) => x,
        };
        let first = match last_purged_log_id {
            None => sto.get_log_id(0).await?,
            Some(x) => x,
        };

        let mut stack = vec![(first, last.clone())];

        loop {
            let (first, last) = match stack.pop() {
                None => {
                    break;
                }
                Some(x) => x,
            };

            if first.leader_id == last.leader_id {
                if res.last().map(|x| &x.leader_id) < Some(&first.leader_id) {
                    res.push(first);
                }
                continue;
            }

            if first.index + 1 == last.index {
                if res.last().map(|x| &x.leader_id) < Some(&first.leader_id) {
                    res.push(first);
                }
                res.push(last);
                continue;
            }

            let mid = sto.get_log_id((first.index + last.index) / 2).await?;

            if first.leader_id == mid.leader_id {
                if res.last().map(|x| &x.leader_id) < Some(&first.leader_id) {
                    res.push(first);
                }
                stack.push((mid, last));
            } else if mid.leader_id == last.leader_id {
                stack.push((first, mid));
            } else {
                stack.push((mid.clone(), last.clone()));
                stack.push((first, mid));
            }
        }

        if res.last() != Some(&last) {
            res.push(last);
        }

        Ok(LogIdList::new(res))
    }
}

impl<NID: NodeId> LogIdList<NID> {
    pub fn new(key_log_ids: impl IntoIterator<Item = LogId<NID>>) -> Self {
        Self {
            key_log_ids: key_log_ids.into_iter().collect(),
        }
    }

    pub(crate) fn extend_from_same_leader<'a, LID: RaftLogId<NID> + 'a>(&mut self, new_ids: &[LID]) {
        if let Some(first) = new_ids.first() {
            let first_id = first.get_log_id();
            self.append(first_id.clone());

            if let Some(last) = new_ids.last() {
                let last_id = last.get_log_id();

                if last_id != first_id {
                    self.append(last_id.clone());
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn extend<'a, LID: RaftLogId<NID> + 'a>(&mut self, new_ids: &[LID]) {
        let mut prev = self.last().map(|x| x.leader_id.clone());

        for x in new_ids.iter() {
            let log_id = x.get_log_id();

            if prev.as_ref() != Some(&log_id.leader_id) {
                self.append(log_id.clone());

                prev = Some(log_id.leader_id.clone());
            }
        }

        if let Some(last) = new_ids.last() {
            let log_id = last.get_log_id();

            if self.last() != Some(log_id) {
                self.append(log_id.clone());
            }
        }
    }

    pub(crate) fn append(&mut self, new_log_id: LogId<NID>) {
        let l = self.key_log_ids.len();
        if l == 0 {
            self.key_log_ids.push(new_log_id);
            return;
        }

        if l == 1 {
            self.key_log_ids.push(new_log_id);
            return;
        }


        let last = self.key_log_ids[l - 1].clone();

        if self.key_log_ids.get(l - 2).map(|x| &x.leader_id) == Some(&last.leader_id) {
            self.key_log_ids[l - 1] = new_log_id;
            return;
        }


        self.key_log_ids.push(new_log_id);
    }

    #[allow(dead_code)]
    pub(crate) fn truncate(&mut self, at: u64) {
        let res = self.key_log_ids.binary_search_by(|log_id| log_id.index.cmp(&at));

        let i = match res {
            Ok(i) => i,
            Err(i) => {
                if i == self.key_log_ids.len() {
                    return;
                }
                i
            }
        };

        self.key_log_ids.truncate(i);

        let last = self.key_log_ids.last();
        if let Some(last) = last {
            let (last_leader_id, last_index) = (last.leader_id.clone(), last.index);
            if last_index < at - 1 {
                self.append(LogId::new(last_leader_id, at - 1));
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn purge(&mut self, upto: &LogId<NID>) {
        let last = self.last().cloned();

        if upto.index >= last.next_index() {
            self.key_log_ids = vec![upto.clone()];
            return;
        }

        if upto.index < self.key_log_ids[0].index {
            return;
        }

        let res = self.key_log_ids.binary_search_by(|log_id| log_id.index.cmp(&upto.index));

        match res {
            Ok(i) => {
                if i > 0 {
                    self.key_log_ids = self.key_log_ids.split_off(i)
                }
            }
            Err(i) => {
                self.key_log_ids = self.key_log_ids.split_off(i - 1);
                self.key_log_ids[0].index = upto.index;
            }
        }
    }

    pub(crate) fn get(&self, index: u64) -> Option<LogId<NID>> {
        let res = self.key_log_ids.binary_search_by(|log_id| log_id.index.cmp(&index));

        match res {
            Ok(i) => Some(LogId::new(self.key_log_ids[i].leader_id.clone(), index)),
            Err(i) => {
                if i == 0 || i == self.key_log_ids.len() {
                    None
                } else {
                    Some(LogId::new(self.key_log_ids[i - 1].leader_id.clone(), index))
                }
            }
        }
    }

    pub(crate) fn first(&self) -> Option<&LogId<NID>> {
        self.key_log_ids.first()
    }

    pub(crate) fn last(&self) -> Option<&LogId<NID>> {
        self.key_log_ids.last()
    }


    #[allow(dead_code)]
    pub(crate) fn by_last_leader(&self) -> &[LogId<NID>] {
        let ks = &self.key_log_ids;
        let l = ks.len();
        if l < 2 {
            return ks;
        }

        if ks[l - 1].leader_id() == ks[l - 2].leader_id() {
            &ks[l - 2..]
        } else {
            &ks[l - 1..]
        }
    }
}
