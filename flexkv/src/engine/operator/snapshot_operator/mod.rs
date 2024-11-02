
use crate::core::state_machine;
use crate::engine::Command;
use crate::engine::EngineOutput;
use crate::raft_state::LogStateReader;
use crate::summary::MessageSummary;
use crate::AsyncRuntime;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::SnapshotMeta;

pub(crate) struct SnapshotOperator<'st, 'out, C>
where C: RaftTypeConfig
{
    pub(crate) state: &'st mut RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>,
    pub(crate) output: &'out mut EngineOutput<C>,
}

impl<'st, 'out, C> SnapshotOperator<'st, 'out, C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn trigger_snapshot(&mut self) -> bool {

        if self.state.io_state_mut().building_snapshot() {
            return false;
        }

        tracing::info!("push snapshot building command");

        self.state.io_state.set_building_snapshot(true);

        self.output.push_command(Command::from(state_machine::Command::build_snapshot()));
        true
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn update_snapshot(&mut self, meta: SnapshotMeta<C::NodeId, C::Node>) -> bool {
        tracing::info!("update_snapshot: {:?}", meta);

        if meta.last_log_id <= self.state.snapshot_last_log_id().cloned() {
            tracing::info!(
                "No need to install a smaller snapshot: current snapshot last_log_id({}), new snapshot last_log_id({})",
                self.state.snapshot_last_log_id().summary(),
                meta.last_log_id.summary()
            );
            return false;
        }

        self.state.snapshot_meta = meta;

        true
    }
}
