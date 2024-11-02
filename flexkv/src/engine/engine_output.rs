use std::collections::VecDeque;

use crate::core::state_machine::CommandSeq;
use crate::engine::Command;
use crate::RaftTypeConfig;

#[derive(Debug, Default)]
pub(crate) struct EngineOutput<C>
where C: RaftTypeConfig
{
    pub(crate) seq: CommandSeq,

    pub(crate) commands: VecDeque<Command<C>>,
}

impl<C> EngineOutput<C>
where C: RaftTypeConfig
{
    pub(crate) fn next_sm_seq(&mut self) -> CommandSeq {
        self.seq += 1;
        self.seq
    }

    pub(crate) fn last_sm_seq(&self) -> CommandSeq {
        self.seq
    }

    pub(crate) fn new(command_buffer_size: usize) -> Self {
        Self {
            seq: 0,
            commands: VecDeque::with_capacity(command_buffer_size),
        }
    }

    pub(crate) fn push_command(&mut self, mut cmd: Command<C>) {

        match &mut cmd {
            Command::StateMachine { command } => {
                let seq = self.next_sm_seq();
                command.set_seq(seq);
            }
            Command::BecomeLeader => {}
            Command::QuitLeader => {}
            Command::AppendInputEntries { .. } => {}
            Command::ReplicateCommitted { .. } => {}
            Command::Commit { .. } => {}
            Command::Replicate { .. } => {}
            Command::RebuildReplicationStreams { .. } => {}
            Command::SaveVote { .. } => {}
            Command::SendVote { .. } => {}
            Command::PurgeLog { .. } => {}
            Command::DeleteConflictLog { .. } => {}
            Command::Respond { .. } => {}
        }
        self.commands.push_back(cmd)
    }

    pub(crate) fn postpone_command(&mut self, cmd: Command<C>) {
        self.commands.push_front(cmd)
    }

    pub(crate) fn pop_command(&mut self) -> Option<Command<C>> {
        self.commands.pop_front()
    }

    /*
    pub(crate) fn iter_commands(&self) -> impl Iterator<Item = &Command<C>> {
        self.commands.iter()
    }
    */
}
