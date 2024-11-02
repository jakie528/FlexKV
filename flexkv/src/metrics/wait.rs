use core::time::Duration;
use std::collections::BTreeSet;

use tokio::sync::watch;

use crate::core::ServerState;
use crate::metrics::Condition;
use crate::metrics::Metric;
use crate::metrics::RaftMetrics;
use crate::node::Node;
use crate::AsyncRuntime;
use crate::Instant;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::OptionalSend;
use crate::Vote;

#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("timeout after {0:?} when {1}")]
    Timeout(Duration, String),

    #[error("raft is shutting down")]
    ShuttingDown,
}

pub struct Wait<NID, N, A>
where
    NID: NodeId,
    N: Node,
    A: AsyncRuntime,
{
    pub timeout: Duration,
    pub rx: watch::Receiver<RaftMetrics<NID, N>>,
    pub(crate) _phantom: std::marker::PhantomData<A>,
}

impl<NID, N, A> Wait<NID, N, A>
where
    NID: NodeId,
    N: Node,
    A: AsyncRuntime,
{
    #[tracing::instrument(level = "trace", skip(self, func), fields(msg=%msg.to_string()))]
    pub async fn metrics<T>(&self, func: T, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError>
    where T: Fn(&RaftMetrics<NID, N>) -> bool + OptionalSend {
        let timeout_at = A::Instant::now() + self.timeout;

        let mut rx = self.rx.clone();
        loop {
            let latest = rx.borrow().clone();

            if func(&latest) {
                return Ok(latest);
            }

            let now = A::Instant::now();
            if now >= timeout_at {
                return Err(WaitError::Timeout(
                    self.timeout,
                    format!("{} latest: {:?}", msg.to_string(), latest),
                ));
            }

            let sleep_time = timeout_at - now;
            let delay = A::sleep(sleep_time);

            tokio::select! {
                _ = delay => {
                    return Err(WaitError::Timeout(self.timeout, format!("{} latest: {}", msg.to_string(), latest.summary())));
                }
                changed = rx.changed() => {
                    match changed {
                        Ok(_) => {
                        },
                        Err(_) => {
                            return Err(WaitError::ShuttingDown);
                        }
                    }
                }
            };
        }
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn vote(&self, want: Vote<NID>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::Vote(want), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn current_leader(&self, leader_id: NID, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.metrics(
            |m| m.current_leader.as_ref() == Some(&leader_id),
            &format!("{} .current_leader == {}", msg.to_string(), leader_id),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn log(&self, want_log_index: Option<u64>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::LastLogIndex(want_log_index), msg.to_string()).await?;
        self.eq(Metric::AppliedIndex(want_log_index), msg.to_string()).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn log_at_least(
        &self,
        want_log: Option<u64>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.ge(Metric::LastLogIndex(want_log), msg.to_string()).await?;
        self.ge(Metric::AppliedIndex(want_log), msg.to_string()).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn log_index(&self, index: Option<u64>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::LastLogIndex(index), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn log_index_at_least(
        &self,
        index: Option<u64>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.ge(Metric::LastLogIndex(index), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn applied_index(
        &self,
        index: Option<u64>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::AppliedIndex(index), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn applied_index_at_least(
        &self,
        index: Option<u64>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.ge(Metric::AppliedIndex(index), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn state(&self, want_state: ServerState, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.metrics(
            |m| m.state == want_state,
            &format!("{} .state == {:?}", msg.to_string(), want_state),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn members(
        &self,
        want_members: BTreeSet<NID>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.metrics(
            |m| {
                let got = m.membership_config.membership().voter_ids().collect::<BTreeSet<_>>();
                want_members == got
            },
            &format!("{} .members -> {:?}", msg.to_string(), want_members),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(msg=msg.to_string().as_str()))]
    pub async fn voter_ids(
        &self,
        voter_ids: impl IntoIterator<Item = NID>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        let want = voter_ids.into_iter().collect::<BTreeSet<_>>();

        self.metrics(
            |m| {
                let got = m.membership_config.membership().voter_ids().collect();
                want == got
            },
            &format!("{} .members == {:?}", msg.to_string(), want),
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn snapshot(
        &self,
        snapshot_last_log_id: LogId<NID>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::Snapshot(Some(snapshot_last_log_id)), msg).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(msg=msg.to_string().as_str()))]
    pub async fn purged(&self, want: Option<LogId<NID>>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.eq(Metric::Purged(want), msg).await
    }

    pub async fn ge(&self, metric: Metric<NID>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.until(Condition::ge(metric), msg).await
    }

    pub async fn eq(&self, metric: Metric<NID>, msg: impl ToString) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.until(Condition::eq(metric), msg).await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(cond=cond.to_string(), msg=msg.to_string().as_str()))]
    pub(crate) async fn until(
        &self,
        cond: Condition<NID>,
        msg: impl ToString,
    ) -> Result<RaftMetrics<NID, N>, WaitError> {
        self.metrics(
            |raft_metrics| match &cond {
                Condition::GE(expect) => raft_metrics >= expect,
                Condition::EQ(expect) => raft_metrics == expect,
            },
            &format!("{} .{}", msg.to_string(), cond),
        )
        .await
    }
}
