use crate::log_id::LogIndexOptionExt;

pub(crate) fn replication_lag(matched_log_index: &Option<u64>, last_log_index: &Option<u64>) -> u64 {
    last_log_index.next_index().saturating_sub(matched_log_index.next_index())
}
