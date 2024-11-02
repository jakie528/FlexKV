use std::borrow::Borrow;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use validit::Validate;

use crate::display_ext::DisplayOptionExt;
use crate::progress::inflight::Inflight;
use crate::progress::inflight::InflightError;
use crate::raft_state::LogStateReader;
use crate::summary::MessageSummary;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;

#[derive(Clone, Copy, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct ProgressEntry<NID: NodeId> {
    pub(crate) matching: Option<LogId<NID>>,

    pub(crate) curr_inflight_id: u64,

    pub(crate) inflight: Inflight<NID>,

    pub(crate) searching_end: u64,
}

impl<NID: NodeId> ProgressEntry<NID> {
    #[allow(dead_code)]
    pub(crate) fn new(matching: Option<LogId<NID>>) -> Self {
        Self {
            matching: matching.clone(),
            curr_inflight_id: 0,
            inflight: Inflight::None,
            searching_end: matching.next_index(),
        }
    }

    pub(crate) fn empty(end: u64) -> Self {
        Self {
            matching: None,
            curr_inflight_id: 0,
            inflight: Inflight::None,
            searching_end: end,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_curr_inflight_id(mut self, v: u64) -> Self {
        self.curr_inflight_id = v;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_inflight(mut self, inflight: Inflight<NID>) -> Self {
        self.inflight = inflight;
        self
    }

    pub(crate) fn is_log_range_inflight(&self, upto: &LogId<NID>) -> bool {
        match &self.inflight {
            Inflight::None => false,
            Inflight::Logs { log_id_range, .. } => {
                let lid = Some(upto.clone());
                lid > log_id_range.prev
            }
            Inflight::Snapshot { last_log_id: _, .. } => false,
        }
    }

    pub(crate) fn update_matching(
        &mut self,
        request_id: u64,
        matching: Option<LogId<NID>>,
    ) -> Result<(), InflightError> {

        self.inflight.ack(request_id, matching.clone())?;

        self.matching = matching;

        let matching_next = self.matching.next_index();
        self.searching_end = std::cmp::max(self.searching_end, matching_next);

        Ok(())
    }

    pub(crate) fn update_conflicting(&mut self, request_id: u64, conflict: u64) -> Result<(), InflightError> {
        self.inflight.conflict(request_id, conflict)?;

        self.searching_end = conflict;

        {
            if conflict < self.matching.next_index() {
                tracing::warn!(
                    "conflict {} < last matching {}: follower log is reverted; with 'loosen-follower-log-revert' enabled, this is allowed.",
                    conflict,
                    self.matching.display(),
                );

                self.matching = None;
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn next_send(
        &mut self,
        log_state: &impl LogStateReader<NID>,
        max_entries: u64,
    ) -> Result<&Inflight<NID>, &Inflight<NID>> {
        if !self.inflight.is_none() {
            return Err(&self.inflight);
        }

        let last_next = log_state.last_log_id().next_index();

        let purge_upto_next = {
            let purge_upto = log_state.purge_upto();
            purge_upto.next_index()
        };


        if self.searching_end < purge_upto_next {
            self.curr_inflight_id += 1;
            let snapshot_last = log_state.snapshot_last_log_id();
            self.inflight = Inflight::snapshot(snapshot_last.cloned()).with_id(self.curr_inflight_id);
            return Ok(&self.inflight);
        }

        let mut start = Self::calc_mid(self.matching.next_index(), self.searching_end);
        if start < purge_upto_next {
            start = purge_upto_next;
        }

        let end = std::cmp::min(start + max_entries, last_next);

        if start == end {
            self.inflight = Inflight::None;
            return Err(&self.inflight);
        }

        let prev = log_state.prev_log_id(start);
        let last = log_state.prev_log_id(end);

        self.curr_inflight_id += 1;
        self.inflight = Inflight::logs(prev, last).with_id(self.curr_inflight_id);

        Ok(&self.inflight)
    }

    #[allow(dead_code)]
    pub(crate) fn sending_start(&self) -> (u64, u64) {
        let mid = Self::calc_mid(self.matching.next_index(), self.searching_end);
        (mid, self.searching_end)
    }

    fn calc_mid(matching_next: u64, end: u64) -> u64 {
        let d = end - matching_next;
        let offset = d / 16 * 8;
        matching_next + offset
    }
}

impl<NID: NodeId> Borrow<Option<LogId<NID>>> for ProgressEntry<NID> {
    fn borrow(&self) -> &Option<LogId<NID>> {
        &self.matching
    }
}

impl<NID: NodeId> Display for ProgressEntry<NID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{[{}, {}), inflight:{}}}",
            self.matching.summary(),
            self.searching_end,
            self.inflight
        )
    }
}

impl<NID: NodeId> Validate for ProgressEntry<NID> {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        validit::less_equal!(self.matching.next_index(), self.searching_end);

        self.inflight.validate()?;

        match &self.inflight {
            Inflight::None => {}
            Inflight::Logs { log_id_range, .. } => {
                validit::less_equal!(&self.matching, &log_id_range.prev);
                validit::less_equal!(log_id_range.prev.next_index(), self.searching_end);
            }
            Inflight::Snapshot { last_log_id, .. } => {
                validit::less!(&self.matching, &last_log_id);
            }
        }
        Ok(())
    }
}
