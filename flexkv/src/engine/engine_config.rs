use std::time::Duration;

use crate::engine::time_state;
use crate::AsyncRuntime;
use crate::Config;
use crate::NodeId;
use crate::SnapshotPolicy;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct EngineConfig<NID: NodeId> {
    pub(crate) id: NID,

    pub(crate) snapshot_policy: SnapshotPolicy,

    pub(crate) max_in_snapshot_log_to_keep: u64,

    pub(crate) purge_batch_size: u64,

    pub(crate) max_payload_entries: u64,

    pub(crate) timer_config: time_state::Config,
}

impl<NID: NodeId> Default for EngineConfig<NID> {
    fn default() -> Self {
        Self {
            id: NID::default(),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
            max_in_snapshot_log_to_keep: 1000,
            purge_batch_size: 256,
            max_payload_entries: 300,
            timer_config: time_state::Config::default(),
        }
    }
}

impl<NID: NodeId> EngineConfig<NID> {
    pub(crate) fn new<RT: AsyncRuntime>(id: NID, config: &Config) -> Self {
        let election_timeout = Duration::from_millis(config.new_rand_election_timeout::<RT>());
        Self {
            id,
            snapshot_policy: config.snapshot_policy.clone(),
            max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
            purge_batch_size: config.purge_batch_size,
            max_payload_entries: config.max_payload_entries,
            timer_config: time_state::Config {
                election_timeout,
                smaller_log_timeout: Duration::from_millis(config.election_timeout_max * 2),
                leader_lease: Duration::from_millis(config.election_timeout_max),
            },
        }
    }
}
