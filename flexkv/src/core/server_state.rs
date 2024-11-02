#[derive(Debug, Clone, Copy, Default)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum ServerState {
    #[default]
    Learner,
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

impl ServerState {
    pub fn is_learner(&self) -> bool {
        matches!(self, Self::Learner)
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, Self::Follower)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, Self::Candidate)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader)
    }
}
