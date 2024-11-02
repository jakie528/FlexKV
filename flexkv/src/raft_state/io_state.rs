use log_io_id::LogIOId;

use crate::LogId;
use crate::NodeId;
use crate::Vote;

pub(crate) mod log_io_id;

#[derive(Debug, Clone, Copy)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct IOState<NID: NodeId> {
    building_snapshot: bool,

    pub(crate) vote: Vote<NID>,

    pub(crate) flushed: LogIOId<NID>,

    pub(crate) applied: Option<LogId<NID>>,

    pub(crate) snapshot: Option<LogId<NID>>,

    pub(crate) purged: Option<LogId<NID>>,
}

impl<NID: NodeId> IOState<NID> {
    pub(crate) fn new(
        vote: Vote<NID>,
        flushed: LogIOId<NID>,
        applied: Option<LogId<NID>>,
        snapshot: Option<LogId<NID>>,
        purged: Option<LogId<NID>>,
    ) -> Self {
        Self {
            building_snapshot: false,
            vote,
            flushed,
            applied,
            snapshot,
            purged,
        }
    }

    pub(crate) fn update_vote(&mut self, vote: Vote<NID>) {
        self.vote = vote;
    }

    pub(crate) fn vote(&self) -> &Vote<NID> {
        &self.vote
    }

    pub(crate) fn update_applied(&mut self, log_id: Option<LogId<NID>>) {

        self.applied = log_id;
    }

    pub(crate) fn applied(&self) -> Option<&LogId<NID>> {
        self.applied.as_ref()
    }

    pub(crate) fn update_snapshot(&mut self, log_id: Option<LogId<NID>>) {
        self.snapshot = log_id;
    }

    pub(crate) fn snapshot(&self) -> Option<&LogId<NID>> {
        self.snapshot.as_ref()
    }

    pub(crate) fn set_building_snapshot(&mut self, building: bool) {
        self.building_snapshot = building;
    }

    pub(crate) fn building_snapshot(&self) -> bool {
        self.building_snapshot
    }

    pub(crate) fn update_purged(&mut self, log_id: Option<LogId<NID>>) {
        self.purged = log_id;
    }

    pub(crate) fn purged(&self) -> Option<&LogId<NID>> {
        self.purged.as_ref()
    }
}
