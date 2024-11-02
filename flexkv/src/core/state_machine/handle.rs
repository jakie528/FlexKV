
use tokio::sync::mpsc;

use crate::core::state_machine;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::TypeConfigExt;
use crate::RaftTypeConfig;
use crate::Snapshot;

pub(crate) struct Handle<C>
where C: RaftTypeConfig
{
    pub(in crate::core::state_machine) cmd_tx: mpsc::UnboundedSender<state_machine::Command<C>>,

    #[allow(dead_code)]
    pub(in crate::core::state_machine) join_handle: JoinHandleOf<C, ()>,
}

impl<C> Handle<C>
where C: RaftTypeConfig
{
    pub(crate) fn send(&mut self, cmd: state_machine::Command<C>) -> Result<(), mpsc::error::SendError<state_machine::Command<C>>> {
        self.cmd_tx.send(cmd)
    }

    pub(crate) fn new_snapshot_reader(&self) -> SnapshotReader<C> {
        SnapshotReader {
            cmd_tx: self.cmd_tx.downgrade(),
        }
    }
}

pub(crate) struct SnapshotReader<C>
where C: RaftTypeConfig
{
    cmd_tx: mpsc::WeakUnboundedSender<state_machine::Command<C>>,
}

impl<C> SnapshotReader<C>
where C: RaftTypeConfig
{
    pub(crate) async fn get_snapshot(&self) -> Result<Option<Snapshot<C>>, &'static str> {
        let (tx, rx) = C::oneshot();

        let cmd = state_machine::Command::get_snapshot(tx);

        let Some(cmd_tx) = self.cmd_tx.upgrade() else {
            tracing::info!("failed to upgrade cmd_tx, state_machine::Worker may have shutdown");
            return Err("failed to upgrade cmd_tx, state_machine::Worker may have shutdown");
        };

        let _ = cmd_tx.send(cmd);

        let got = match rx.await {
            Ok(x) => x,
            Err(_e) => {
                tracing::error!("failed to receive snapshot, state_machine::Worker may have shutdown");
                return Err("failed to receive snapshot, state_machine::Worker may have shutdown");
            }
        };

        let snapshot = got.unwrap();

        Ok(snapshot)
    }
}
