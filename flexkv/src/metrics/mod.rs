
mod metric;
mod raft_metrics;
mod wait;

mod metric_display;
mod wait_condition;

use std::collections::BTreeMap;

pub use metric::Metric;
pub use raft_metrics::RaftDataMetrics;
pub use raft_metrics::RaftMetrics;
pub use raft_metrics::RaftServerMetrics;
pub use wait::Wait;
pub use wait::WaitError;
pub(crate) use wait_condition::Condition;

use crate::LogId;

pub(crate) type ReplicationMetrics<NID> = BTreeMap<NID, Option<LogId<NID>>>;
