

use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyerror::AnyError;
use clap::Parser;
use rand::Rng;

use crate::config::error::ConfigError;
use crate::raft_state::LogStateReader;
use crate::AsyncRuntime;
use crate::LogIdOptionExt;
use crate::NodeId;


#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum SnapshotPolicy {


    LogsSinceLast(u64),

    Never,
}

impl SnapshotPolicy {
    pub(crate) fn should_snapshot<NID>(&self, state: &impl Deref<Target = impl LogStateReader<NID>>) -> bool
    where NID: NodeId {
        match self {
            SnapshotPolicy::LogsSinceLast(threshold) => {
                state.committed().next_index() >= state.snapshot_last_log_id().next_index() + threshold
            }
            SnapshotPolicy::Never => false,
        }
    }
}

fn parse_bytes_with_unit(src: &str) -> Result<u64, ConfigError> {
    let res = byte_unit::Byte::from_str(src).map_err(|e| ConfigError::InvalidNumber {
        invalid: src.to_string(),
        reason: e.to_string(),
    })?;

    Ok(res.as_u64())
}

fn parse_snapshot_policy(src: &str) -> Result<SnapshotPolicy, ConfigError> {
    if src == "never" {
        return Ok(SnapshotPolicy::Never);
    }

    let elts = src.split(':').collect::<Vec<_>>();
    if elts.len() != 2 {
        return Err(ConfigError::InvalidSnapshotPolicy {
            syntax: "never|since_last:<num>".to_string(),
            invalid: src.to_string(),
        });
    }

    if elts[0] != "since_last" {
        return Err(ConfigError::InvalidSnapshotPolicy {
            syntax: "never|since_last:<num>".to_string(),
            invalid: src.to_string(),
        });
    }

    let n_logs = elts[1].parse::<u64>().map_err(|e| ConfigError::InvalidNumber {
        invalid: src.to_string(),
        reason: e.to_string(),
    })?;
    Ok(SnapshotPolicy::LogsSinceLast(n_logs))
}

#[derive(Clone, Debug, Parser)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Config {

    #[clap(long, default_value = "foo")]
    pub cluster_name: String,


    #[clap(long, default_value = "150")]
    pub election_timeout_min: u64,


    #[clap(long, default_value = "300")]
    pub election_timeout_max: u64,


    #[clap(long, default_value = "50")]
    pub heartbeat_interval: u64,




    #[clap(long, default_value = "200")]
    pub install_snapshot_timeout: u64,





    #[clap(long, default_value = "0")]
    pub send_snapshot_timeout: u64,





    #[clap(long, default_value = "300")]
    pub max_payload_entries: u64,








    #[clap(long, default_value = "5000")]
    pub replication_lag_threshold: u64,


    #[clap(
        long,
        default_value = "since_last:5000",
        value_parser=parse_snapshot_policy
    )]
    pub snapshot_policy: SnapshotPolicy,


    #[clap(long, default_value = "3MiB", value_parser=parse_bytes_with_unit)]
    pub snapshot_max_chunk_size: u64,




    #[clap(long, default_value = "1000")]
    pub max_in_snapshot_log_to_keep: u64,


    #[clap(long, default_value = "1")]
    pub purge_batch_size: u64,















    #[clap(long,
           default_value_t = true,
           action = clap::ArgAction::Set,
           num_args = 0..=1,
           default_missing_value = "true"
    )]
    pub enable_tick: bool,




    #[clap(long,
           default_value_t = true,
           action = clap::ArgAction::Set,
           num_args = 0..=1,
           default_missing_value = "true"
    )]
    pub enable_heartbeat: bool,





    #[clap(long,
           default_value_t = true,
           action = clap::ArgAction::Set,
           num_args = 0..=1,
           default_missing_value = "true"
    )]
    pub enable_elect: bool,
}


pub(crate) struct RuntimeConfig {
    pub(crate) enable_heartbeat: AtomicBool,
    pub(crate) enable_elect: AtomicBool,
}

impl RuntimeConfig {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            enable_heartbeat: AtomicBool::from(config.enable_heartbeat),
            enable_elect: AtomicBool::from(config.enable_elect),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        <Self as Parser>::parse_from(Vec::<&'static str>::new())
    }
}

impl Config {

    pub fn new_rand_election_timeout<RT: AsyncRuntime>(&self) -> u64 {
        RT::thread_rng().gen_range(self.election_timeout_min..self.election_timeout_max)
    }


    pub fn install_snapshot_timeout(&self) -> Duration {
        Duration::from_millis(self.install_snapshot_timeout)
    }


    pub fn send_snapshot_timeout(&self) -> Duration {
        #[allow(deprecated)]
        if self.send_snapshot_timeout > 0 {
            Duration::from_millis(self.send_snapshot_timeout)
        } else {
            self.install_snapshot_timeout()
        }
    }




    pub fn build(args: &[&str]) -> Result<Config, ConfigError> {
        let config = <Self as Parser>::try_parse_from(args).map_err(|e| ConfigError::ParseError {
            source: AnyError::from(&e),
            args: args.iter().map(|x| x.to_string()).collect(),
        })?;
        config.validate()
    }


    pub fn validate(self) -> Result<Config, ConfigError> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ConfigError::ElectionTimeout {
                min: self.election_timeout_min,
                max: self.election_timeout_max,
            });
        }

        if self.election_timeout_min <= self.heartbeat_interval {
            return Err(ConfigError::ElectionTimeoutLTHeartBeat {
                election_timeout_min: self.election_timeout_min,
                heartbeat_interval: self.heartbeat_interval,
            });
        }

        if self.max_payload_entries == 0 {
            return Err(ConfigError::MaxPayloadIs0);
        }

        Ok(self)
    }
}
