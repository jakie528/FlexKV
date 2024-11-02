use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::display_ext::DisplaySlice;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::RaftTypeConfig;
use crate::Vote;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct AppendEntriesRequest<C: RaftTypeConfig> {
    pub vote: Vote<C::NodeId>,

    pub prev_log_id: Option<LogId<C::NodeId>>,

    pub entries: Vec<C::Entry>,

    pub leader_commit: Option<LogId<C::NodeId>>,
}

impl<C: RaftTypeConfig> fmt::Debug for AppendEntriesRequest<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendEntriesRequest")
            .field("vote", &self.vote)
            .field("prev_log_id", &self.prev_log_id)
            .field("entries", &self.entries)
            .field("leader_commit", &self.leader_commit)
            .finish()
    }
}

impl<C: RaftTypeConfig> MessageSummary<AppendEntriesRequest<C>> for AppendEntriesRequest<C> {
    fn summary(&self) -> String {
        format!(
            "vote={}, prev_log_id={}, leader_commit={}, entries={}",
            self.vote,
            self.prev_log_id.summary(),
            self.leader_commit.summary(),
            DisplaySlice::<_>(self.entries.as_slice())
        )
    }
}

#[derive(Debug)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum AppendEntriesResponse<NID: NodeId> {
    Success,

    PartialSuccess(Option<LogId<NID>>),

    Conflict,

    HigherVote(Vote<NID>),
}

impl<NID: NodeId> AppendEntriesResponse<NID> {
    pub fn is_success(&self) -> bool {
        matches!(*self, AppendEntriesResponse::Success)
    }

    pub fn is_conflict(&self) -> bool {
        matches!(*self, AppendEntriesResponse::Conflict)
    }
}

impl<NID: NodeId> fmt::Display for AppendEntriesResponse<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendEntriesResponse::Success => write!(f, "Success"),
            AppendEntriesResponse::PartialSuccess(m) => {
                write!(f, "PartialSuccess({})", m.display())
            }
            AppendEntriesResponse::HigherVote(vote) => write!(f, "Higher vote, {}", vote),
            AppendEntriesResponse::Conflict => write!(f, "Conflict"),
        }
    }
}
