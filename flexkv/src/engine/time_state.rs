use std::time::Duration;

#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) election_timeout: Duration,

    pub(crate) smaller_log_timeout: Duration,

    pub(crate) leader_lease: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150),
            smaller_log_timeout: Duration::from_millis(200),
            leader_lease: Duration::from_millis(150),
        }
    }
}
