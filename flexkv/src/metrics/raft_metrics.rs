use std::fmt;
use std::sync::Arc;

use crate::core::ServerState;
use crate::display_ext::DisplayOption;
use crate::display_ext::DisplayOptionExt;
use crate::error::Fatal;
use crate::metrics::ReplicationMetrics;
use crate::node::Node;
use crate::summary::MessageSummary;
use crate::LogId;
use crate::NodeId;
use crate::StoredMembership;
use crate::Vote;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct RaftMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub running_state: Result<(), Fatal<NID>>,

    pub id: NID,

    pub current_term: u64,

    pub vote: Vote<NID>,

    pub last_log_index: Option<u64>,

    pub last_applied: Option<LogId<NID>>,

    pub snapshot: Option<LogId<NID>>,

    pub purged: Option<LogId<NID>>,

    pub state: ServerState,

    pub current_leader: Option<NID>,

    pub millis_since_quorum_ack: Option<u64>,

    pub membership_config: Arc<StoredMembership<NID, N>>,

    pub replication: Option<ReplicationMetrics<NID>>,
}

impl<NID, N> fmt::Display for RaftMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metrics{{")?;

        write!(
            f,
            "id:{}, {:?}, term:{}, vote:{}, last_log:{}, last_applied:{}, leader:{}(since_last_ack:{} ms)",
            self.id,
            self.state,
            self.current_term,
            self.vote,
            DisplayOption(&self.last_log_index),
            DisplayOption(&self.last_applied),
            DisplayOption(&self.current_leader),
            DisplayOption(&self.millis_since_quorum_ack),
        )?;

        write!(f, ", ")?;
        write!(
            f,
            "membership:{}, snapshot:{}, purged:{}, replication:{{{}}}",
            self.membership_config.summary(),
            DisplayOption(&self.snapshot),
            DisplayOption(&self.purged),
            self.replication
                .as_ref()
                .map(|x| { x.iter().map(|(k, v)| format!("{}:{}", k, DisplayOption(v))).collect::<Vec<_>>().join(",") })
                .unwrap_or_default(),
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}
impl<NID, N> MessageSummary<RaftMetrics<NID, N>> for RaftMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID, N> RaftMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub fn new_initial(id: NID) -> Self {
        Self {
            running_state: Ok(()),
            id,

            current_term: 0,
            vote: Vote::default(),
            last_log_index: None,
            last_applied: None,
            snapshot: None,
            purged: None,

            state: ServerState::Follower,
            current_leader: None,
            millis_since_quorum_ack: None,
            membership_config: Arc::new(StoredMembership::default()),
            replication: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct RaftDataMetrics<NID>
where NID: NodeId
{
    pub last_log: Option<LogId<NID>>,
    pub last_applied: Option<LogId<NID>>,
    pub snapshot: Option<LogId<NID>>,
    pub purged: Option<LogId<NID>>,

    pub millis_since_quorum_ack: Option<u64>,

    pub replication: Option<ReplicationMetrics<NID>>,
}

impl<NID> fmt::Display for RaftDataMetrics<NID>
where NID: NodeId
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataMetrics{{")?;

        write!(
            f,
            "last_log:{}, last_applied:{}, snapshot:{}, purged:{}, quorum_acked(leader):{} ms before, replication:{{{}}}",
            DisplayOption(&self.last_log),
            DisplayOption(&self.last_applied),
            DisplayOption(&self.snapshot),
            DisplayOption(&self.purged),
            self.millis_since_quorum_ack.display(),
            self.replication
                .as_ref()
                .map(|x| { x.iter().map(|(k, v)| format!("{}:{}", k, DisplayOption(v))).collect::<Vec<_>>().join(",") })
                .unwrap_or_default(),
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}

impl<NID> MessageSummary<RaftDataMetrics<NID>> for RaftDataMetrics<NID>
where NID: NodeId
{
    fn summary(&self) -> String {
        self.to_string()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct RaftServerMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    pub id: NID,
    pub vote: Vote<NID>,
    pub state: ServerState,
    pub current_leader: Option<NID>,
    pub membership_config: Arc<StoredMembership<NID, N>>,
}

impl<NID, N> fmt::Display for RaftServerMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServerMetrics{{")?;

        write!(
            f,
            "id:{}, {:?}, vote:{}, leader:{}, membership:{}",
            self.id,
            self.state,
            self.vote,
            DisplayOption(&self.current_leader),
            self.membership_config.summary(),
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}

impl<NID, N> MessageSummary<RaftServerMetrics<NID, N>> for RaftServerMetrics<NID, N>
where
    NID: NodeId,
    N: Node,
{
    fn summary(&self) -> String {
        self.to_string()
    }
}
