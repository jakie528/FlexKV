
use std::fmt;

use crate::core::raft_msg::ResultSender;
use crate::RaftTypeConfig;
use crate::Snapshot;

pub(crate) enum ExternalCommand<C: RaftTypeConfig> {
    Elect,

    Heartbeat,

    Snapshot,

    GetSnapshot { tx: ResultSender<C, Option<Snapshot<C>>> },

    PurgeLog { upto: u64 },
}

impl<C> fmt::Debug for ExternalCommand<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<C> fmt::Display for ExternalCommand<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExternalCommand::Elect => {
                write!(f, "Elect")
            }
            ExternalCommand::Heartbeat => {
                write!(f, "Heartbeat")
            }
            ExternalCommand::Snapshot => {
                write!(f, "Snapshot")
            }
            ExternalCommand::GetSnapshot { .. } => {
                write!(f, "GetSnapshot")
            }
            ExternalCommand::PurgeLog { upto } => {
                write!(f, "PurgeLog[..={}]", upto)
            }
        }
    }
}
