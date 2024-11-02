use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::replication::request_id::RequestId;
use crate::replication::ReplicationSessionId;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::LogIdOf;
use crate::MessageSummary;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::Vote;

#[derive(Debug)]
pub(crate) enum Response<C>
where C: RaftTypeConfig
{
    Progress {
        target: C::NodeId,

        request_id: RequestId,

        result: Result<ReplicationResult<C>, String>,

        session_id: ReplicationSessionId<C::NodeId>,
    },

    StorageError { error: StorageError<C::NodeId> },

    HigherVote {
        target: C::NodeId,

        higher: Vote<C::NodeId>,

        sender_vote: Vote<C::NodeId>,
    },
}

impl<C> MessageSummary<Response<C>> for Response<C>
where C: RaftTypeConfig
{
    fn summary(&self) -> String {
        match self {
            Self::Progress {
                ref target,
                request_id: ref id,
                ref result,
                ref session_id,
            } => {
                format!(
                    "UpdateReplicationProgress: target: {}, id: {}, result: {:?}, session_id: {}",
                    target, id, result, session_id,
                )
            }

            Self::StorageError { error } => format!("ReplicationStorageError: {}", error),

            Self::HigherVote {
                target,
                higher,
                sender_vote,
            } => {
                format!(
                    "Seen a higher vote: target: {}, vote: {}, server_state_vote: {}",
                    target, higher, sender_vote
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicationResult<C: RaftTypeConfig> {
    pub(crate) sending_time: InstantOf<C>,

    pub(crate) result: Result<Option<LogIdOf<C>>, LogIdOf<C>>,
}

impl<C> fmt::Display for ReplicationResult<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{time:{:?}, result:", self.sending_time)?;

        match &self.result {
            Ok(matching) => write!(f, "Match:{}", matching.display())?,
            Err(conflict) => write!(f, "Conflict:{}", conflict)?,
        }

        write!(f, "}}")
    }
}

impl<C> ReplicationResult<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(sending_time: InstantOf<C>, result: Result<Option<LogIdOf<C>>, LogIdOf<C>>) -> Self {
        Self { sending_time, result }
    }
}
