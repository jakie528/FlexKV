use std::marker::PhantomData;
use std::sync::Arc;

use anyerror::AnyError;

use crate::display_ext::DisplayOptionExt;
use crate::engine::LogIdList;
use crate::entry::RaftPayload;
use crate::log_id::RaftLogId;
use crate::raft_state::io_state::log_io_id::LogIOId;
use crate::raft_state::IOState;
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::type_config::TypeConfigExt;
use crate::utime::UTime;
use crate::AsyncRuntime;
use crate::EffectiveMembership;
use crate::LogIdOptionExt;
use crate::MembershipState;
use crate::RaftLogReader;
use crate::RaftSnapshotBuilder;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StorageIOError;
use crate::StoredMembership;

pub struct StorageHelper<'a, C, LS, SM>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    pub(crate) log_store: &'a mut LS,
    pub(crate) state_machine: &'a mut SM,
    _p: PhantomData<C>,
}

impl<'a, C, LS, SM> StorageHelper<'a, C, LS, SM>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    pub fn new(sto: &'a mut LS, sm: &'a mut SM) -> Self {
        Self {
            log_store: sto,
            state_machine: sm,
            _p: Default::default(),
        }
    }

    pub async fn get_initial_state(
        &mut self,
    ) -> Result<RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>, StorageError<C::NodeId>>
    {
        let vote = self.log_store.read_vote().await?;
        let vote = vote.unwrap_or_default();

        let mut committed = self.log_store.read_committed().await?;

        let st = self.log_store.get_log_state().await?;
        let mut last_purged_log_id = st.last_purged_log_id;
        let mut last_log_id = st.last_log_id;

        let (mut last_applied, _) = self.state_machine.applied_state().await?;

        tracing::info!(
            vote = display(&vote),
            last_purged_log_id = display(last_purged_log_id.display()),
            last_applied = display(last_applied.display()),
            committed = display(committed.display()),
            last_log_id = display(last_log_id.display()),
            "get_initial_state"
        );

        if committed < last_applied {
            committed = last_applied.clone();
        }

        if last_applied < committed {
            let start = last_applied.next_index();
            let end = committed.next_index();

            self.reapply_committed(start, end).await?;

            last_applied = committed;
        }

        let mem_state = self.get_membership().await?;

        if last_log_id < last_applied {
            tracing::info!(
                "Clean the hole between last_log_id({}) and last_applied({}) by purging logs to {}",
                last_log_id.display(),
                last_applied.display(),
                last_applied.display(),
            );

            self.log_store.purge(last_applied.clone().unwrap()).await?;
            last_log_id = last_applied.clone();
            last_purged_log_id = last_applied.clone();
        }

        tracing::info!(
            "load key log ids from ({},{}]",
            last_purged_log_id.display(),
            last_log_id.display()
        );
        let log_ids = LogIdList::load_log_ids(last_purged_log_id.clone(), last_log_id, self.log_store).await?;

        let snapshot = self.state_machine.get_current_snapshot().await?;

        let snapshot = match snapshot {
            None => {
                if last_purged_log_id.is_some() {
                    let mut b = self.state_machine.get_snapshot_builder().await;
                    let s = b.build_snapshot().await?;
                    Some(s)
                } else {
                    None
                }
            }
            s @ Some(_) => s,
        };
        let snapshot_meta = snapshot.map(|x| x.meta).unwrap_or_default();

        let io_state = IOState::new(
            vote.clone(),
            LogIOId::default(),
            last_applied.clone(),
            snapshot_meta.last_log_id.clone(),
            last_purged_log_id.clone(),
        );

        let now = C::now();

        Ok(RaftState {
            committed: last_applied,
            vote: UTime::new(now, vote),
            purged_next: last_purged_log_id.next_index(),
            log_ids,
            membership_state: mem_state,
            snapshot_meta,

            server_state: Default::default(),
            accepted: Default::default(),
            io_state,
            purge_upto: last_purged_log_id,
        })
    }

    pub(crate) async fn reapply_committed(&mut self, mut start: u64, end: u64) -> Result<(), StorageError<C::NodeId>> {
        let chunk_size = 64;

        tracing::info!(
            "re-apply log [{}..{}) in {} item chunks to state machine",
            chunk_size,
            start,
            end
        );

        let mut log_reader = self.log_store.get_log_reader().await;

        while start < end {
            let chunk_end = std::cmp::min(end, start + chunk_size);
            let entries = log_reader.try_get_log_entries(start..chunk_end).await?;

            let first = entries.first().map(|x| x.get_log_id().index);
            let last = entries.last().map(|x| x.get_log_id().index);

            let make_err = || {
                let err = AnyError::error(format!(
                    "Failed to get log entries, expected index: [{}, {}), got [{:?}, {:?})",
                    start, chunk_end, first, last
                ));

                tracing::error!("{}", err);
                err
            };

            if first != Some(start) {
                return Err(StorageIOError::read_log_at_index(start, make_err()).into());
            }
            if last != Some(chunk_end - 1) {
                return Err(StorageIOError::read_log_at_index(chunk_end - 1, make_err()).into());
            }

            tracing::info!(
                "re-apply {} log entries: [{}, {}),",
                chunk_end - start,
                start,
                chunk_end
            );
            self.state_machine.apply(entries).await?;

            start = chunk_end;
        }

        Ok(())
    }

    pub async fn get_membership(&mut self) -> Result<MembershipState<C::NodeId, C::Node>, StorageError<C::NodeId>> {
        let (last_applied, sm_mem) = self.state_machine.applied_state().await?;

        let log_mem = self.last_membership_in_log(last_applied.next_index()).await?;

        if log_mem.len() == 2 {
            return Ok(MembershipState::new(
                Arc::new(EffectiveMembership::new_from_stored_membership(log_mem[0].clone())),
                Arc::new(EffectiveMembership::new_from_stored_membership(log_mem[1].clone())),
            ));
        }

        let effective = if log_mem.is_empty() {
            EffectiveMembership::new_from_stored_membership(sm_mem.clone())
        } else {
            EffectiveMembership::new_from_stored_membership(log_mem[0].clone())
        };

        let res = MembershipState::new(
            Arc::new(EffectiveMembership::new_from_stored_membership(sm_mem)),
            Arc::new(effective),
        );

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn last_membership_in_log(
        &mut self,
        since_index: u64,
    ) -> Result<Vec<StoredMembership<C::NodeId, C::Node>>, StorageError<C::NodeId>> {
        let st = self.log_store.get_log_state().await?;

        let mut end = st.last_log_id.next_index();

        tracing::info!("load membership from log: [{}..{})", since_index, end);

        let start = std::cmp::max(st.last_purged_log_id.next_index(), since_index);
        let step = 64;

        let mut res = vec![];

        while start < end {
            let step_start = std::cmp::max(start, end.saturating_sub(step));
            let entries = self.log_store.try_get_log_entries(step_start..end).await?;

            for ent in entries.iter().rev() {
                if let Some(mem) = ent.get_membership() {
                    let em = StoredMembership::new(Some(ent.get_log_id().clone()), mem.clone());
                    res.insert(0, em);
                    if res.len() == 2 {
                        return Ok(res);
                    }
                }
            }

            end = end.saturating_sub(step);
        }

        Ok(res)
    }
}
