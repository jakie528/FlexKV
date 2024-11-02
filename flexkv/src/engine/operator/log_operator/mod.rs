use crate::engine::Command;
use crate::engine::EngineConfig;
use crate::engine::EngineOutput;
use crate::raft_state::LogStateReader;
use crate::summary::MessageSummary;
use crate::AsyncRuntime;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::RaftState;
use crate::RaftTypeConfig;


pub(crate) struct LogOperator<'x, C>
where C: RaftTypeConfig
{
    pub(crate) config: &'x mut EngineConfig<C::NodeId>,
    pub(crate) state: &'x mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'x mut EngineOutput<C>,
}

impl<'x, C> LogOperator<'x, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn purge_log(&mut self) {
        let st = &mut self.state;
        let purge_upto = st.purge_upto();

        tracing::info!(
            last_purged_log_id = display(st.last_purged_log_id().summary()),
            purge_upto = display(purge_upto.summary()),
            "purge_log"
        );

        if purge_upto <= st.last_purged_log_id() {
            return;
        }

        let upto = purge_upto.unwrap().clone();

        st.purge_log(&upto);
        self.output.push_command(Command::PurgeLog { upto });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn schedule_policy_based_purge(&mut self) {
        if let Some(purge_upto) = self.calc_purge_upto() {
            self.update_purge_upto(purge_upto);
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_purge_upto(&mut self, purge_upto: LogId<C::NodeId>) {
        self.state.purge_upto = Some(purge_upto);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn calc_purge_upto(&self) -> Option<LogId<C::NodeId>> {
        let st = &self.state;
        let max_keep = self.config.max_in_snapshot_log_to_keep;
        let batch_size = self.config.purge_batch_size;

        let purge_end = self.state.snapshot_meta.last_log_id.next_index().saturating_sub(max_keep);

        if st.last_purged_log_id().next_index() + batch_size > purge_end {
            return None;
        }

        let log_id = self.state.log_ids.get(purge_end - 1);
        log_id
    }
}
