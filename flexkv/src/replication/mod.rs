
pub(crate) mod callbacks;
pub(crate) mod hint;
mod replication_session_id;
pub(crate) mod request;
pub(crate) mod request_id;
pub(crate) mod response;

use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use futures::future::FutureExt;
pub(crate) use replication_session_id::ReplicationSessionId;
use request::Data;
use request::DataWithId;
use request::Replicate;
use response::ReplicationResult;
pub(crate) use response::Response;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tracing_futures::Instrument;

use crate::config::Config;
use crate::core::notify::Notify;
use crate::core::state_machine::handle::SnapshotReader;
use crate::display_ext::DisplayOptionExt;
use crate::error::decompose::DecomposeResult;
use crate::error::HigherVote;
use crate::error::PayloadTooLarge;
use crate::error::RPCError;
use crate::error::ReplicationClosed;
use crate::error::ReplicationError;
use crate::error::Timeout;
use crate::log_id::LogIdOptionExt;
use crate::log_id_range::LogIdRange;
use crate::network::Backoff;
use crate::network::RPCOption;
use crate::network::RPCTypes;
use crate::network::RaftNetwork;
use crate::network::RaftNetworkFactory;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::replication::callbacks::SnapshotCallback;
use crate::replication::hint::ReplicationHint;
use crate::replication::request_id::RequestId;
use crate::storage::RaftLogReader;
use crate::storage::RaftLogStorage;
use crate::storage::Snapshot;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::TypeConfigExt;
use crate::AsyncRuntime;
use crate::LogId;
use crate::MessageSummary;
use crate::RaftLogId;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StorageIOError;
use crate::Vote;

pub(crate) struct ReplicationHandle<C>
where C: RaftTypeConfig
{
    pub(crate) join_handle: JoinHandleOf<C, Result<(), ReplicationClosed>>,

    pub(crate) tx_repl: mpsc::UnboundedSender<Replicate<C>>,
}

pub(crate) struct ReplicationCore<C, N, LS>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
{
    target: C::NodeId,

    session_id: ReplicationSessionId<C::NodeId>,

    #[allow(clippy::type_complexity)]
    tx_raft_core: mpsc::UnboundedSender<Notify<C>>,

    rx_event: mpsc::UnboundedReceiver<Replicate<C>>,

    weak_tx_event: mpsc::WeakUnboundedSender<Replicate<C>>,

    network: N::Network,

    snapshot_network: Arc<Mutex<N::Network>>,

    snapshot_state: Option<(oneshot::Sender<()>, JoinHandleOf<C, ()>)>,

    backoff: Option<Backoff>,

    log_reader: LS::LogReader,

    snapshot_reader: SnapshotReader<C>,

    config: Arc<Config>,

    committed: Option<LogId<C::NodeId>>,

    matching: Option<LogId<C::NodeId>>,

    next_action: Option<Data<C>>,

    entries_hint: ReplicationHint,
}

impl<C, N, LS> ReplicationCore<C, N, LS>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
{
    #[tracing::instrument(level = "trace", skip_all,fields(target=display(&target), session_id=display(&session_id)))]
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        target: C::NodeId,
        session_id: ReplicationSessionId<C::NodeId>,
        config: Arc<Config>,
        committed: Option<LogId<C::NodeId>>,
        matching: Option<LogId<C::NodeId>>,
        network: N::Network,
        snapshot_network: N::Network,
        log_reader: LS::LogReader,
        snapshot_reader: SnapshotReader<C>,
        tx_raft_core: mpsc::UnboundedSender<Notify<C>>,
        span: tracing::Span,
    ) -> ReplicationHandle<C> {

        let (tx_event, rx_event) = mpsc::unbounded_channel();

        let this = Self {
            target,
            session_id,
            network,
            snapshot_network: Arc::new(Mutex::new(snapshot_network)),
            snapshot_state: None,
            backoff: None,
            log_reader,
            snapshot_reader,
            config,
            committed,
            matching,
            tx_raft_core,
            rx_event,
            weak_tx_event: tx_event.downgrade(),
            next_action: None,
            entries_hint: Default::default(),
        };

        let join_handle = C::AsyncRuntime::spawn(this.main().instrument(span));

        ReplicationHandle {
            join_handle,
            tx_repl: tx_event,
        }
    }

    #[tracing::instrument(level="debug", skip(self), fields(session=%self.session_id, target=display(&self.target), cluster=%self.config.cluster_name))]
    async fn main(mut self) -> Result<(), ReplicationClosed> {
        loop {
            let action = self.next_action.take();

            let Some(d) = action else {
                self.drain_events_with_backoff().await?;
                continue;
            };

            let mut log_data = None;

            let request_id = d.request_id();

            let res = match d {
                Data::Heartbeat => {
                    let m = &self.matching;
                    let d = DataWithId::new(RequestId::new_heartbeat(), LogIdRange::new(m.clone(), m.clone()));

                    log_data = Some(d.clone());
                    self.send_log_entries(d).await
                }
                Data::Logs(log) => {
                    log_data = Some(log.clone());
                    self.send_log_entries(log).await
                }
                Data::Snapshot(snap) => self.stream_snapshot(snap).await,
                Data::SnapshotCallback(resp) => self.handle_snapshot_callback(resp),
            };

            match res {
                Ok(next) => {
                    self.backoff = None;

                    if let Some(next) = next {
                        self.next_action = Some(next);
                    }
                }
                Err(err) => {
                    tracing::warn!(error=%err, "error replication to target={}", self.target);

                    match err {
                        ReplicationError::Closed(closed) => {
                            return Err(closed);
                        }
                        ReplicationError::HigherVote(h) => {
                            let _ = self.tx_raft_core.send(Notify::Network {
                                response: Response::HigherVote {
                                    target: self.target,
                                    higher: h.higher,
                                    sender_vote: self.session_id.vote_ref().clone(),
                                },
                            });
                            return Ok(());
                        }
                        ReplicationError::StorageError(error) => {
                            tracing::error!(error=%error, "error replication to target={}", self.target);

                            let _ = self.tx_raft_core.send(Notify::Network {
                                response: Response::StorageError { error },
                            });
                            return Ok(());
                        }
                        ReplicationError::RPCError(err) => {
                            tracing::error!(err = display(&err), "RPCError");

                            let retry = match &err {
                                RPCError::Timeout(_) => false,
                                RPCError::Unreachable(_unreachable) => {
                                    if self.backoff.is_none() {
                                        self.backoff = Some(self.network.backoff());
                                    }
                                    false
                                }
                                RPCError::PayloadTooLarge(too_large) => {
                                    self.update_hint(too_large);

                                    self.next_action = Some(Data::Logs(log_data.unwrap()));
                                    true
                                }
                                RPCError::Network(_) => false,
                                RPCError::RemoteError(_) => false,
                            };

                            if !retry {
                                self.send_progress_error(request_id, err);
                            }
                        }
                    };
                }
            };

            self.drain_events_with_backoff().await?;
        }
    }

    async fn drain_events_with_backoff(&mut self) -> Result<(), ReplicationClosed> {
        if let Some(b) = &mut self.backoff {
            let duration = b.next().unwrap_or_else(|| {
                tracing::warn!("backoff exhausted, using default");
                Duration::from_millis(500)
            });

            self.backoff_drain_events(C::now() + duration).await?;
        }

        self.drain_events().await?;
        Ok(())
    }

    fn update_hint(&mut self, too_large: &PayloadTooLarge) {
        const DEFAULT_ENTRIES_HINT_TTL: u64 = 10;

        match too_large.action() {
            RPCTypes::Vote => {
                unreachable!("Vote RPC should not be too large")
            }
            RPCTypes::AppendEntries => {
                self.entries_hint = ReplicationHint::new(too_large.entries_hint(), DEFAULT_ENTRIES_HINT_TTL);
            }
            RPCTypes::InstallSnapshot => {
                tracing::error!("InstallSnapshot RPC is too large, but it is not supported yet");
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_log_entries(
        &mut self,
        log_ids: DataWithId<LogIdRange<C::NodeId>>,
    ) -> Result<Option<Data<C>>, ReplicationError<C::NodeId, C::Node>> {
        let request_id = log_ids.request_id();

        let (logs, sending_range) = {
            let rng = log_ids.data();

            let (start, end) = {
                let start = rng.prev.next_index();
                let end = rng.last.next_index();

                if let Some(hint) = self.entries_hint.get() {
                    let hint_end = start + hint;
                    (start, std::cmp::min(end, hint_end))
                } else {
                    (start, end)
                }
            };

            if start == end {
                let r = LogIdRange::new(rng.prev.clone(), rng.prev.clone());
                (vec![], r)
            } else {
                let logs = self.log_reader.limited_get_log_entries(start, end).await?;

                let _first = logs.first().map(|x| x.get_log_id().clone()).unwrap();
                let last = logs.last().map(|x| x.get_log_id().clone()).unwrap();

                let r = LogIdRange::new(rng.prev.clone(), Some(last));
                (logs, r)
            }
        };

        let leader_time = C::now();

        let payload = AppendEntriesRequest {
            vote: self.session_id.vote_ref().clone(),
            prev_log_id: sending_range.prev.clone(),
            leader_commit: self.committed.clone(),
            entries: logs,
        };

        let the_timeout = Duration::from_millis(self.config.heartbeat_interval);
        let option = RPCOption::new(the_timeout);
        let res = C::timeout(the_timeout, self.network.append_entries(payload, option)).await;

        let append_res = res.map_err(|_e| {
            let to = Timeout {
                action: RPCTypes::AppendEntries,
                id: self.session_id.vote_ref().leader_id().voted_for().unwrap(),
                target: self.target.clone(),
                timeout: the_timeout,
            };
            RPCError::Timeout(to)
        })?; // return Timeout error

        let append_resp = DecomposeResult::<C, _, _>::decompose_infallible(append_res)?;
        match append_resp {
            AppendEntriesResponse::Success => {
                let matching = sending_range.last;
                let next = self.finish_success_append(matching, leader_time, log_ids);
                Ok(next)
            }
            AppendEntriesResponse::PartialSuccess(matching) => {
                let next = self.finish_success_append(matching, leader_time, log_ids);
                Ok(next)
            }
            AppendEntriesResponse::HigherVote(vote) => {

                Err(ReplicationError::HigherVote(HigherVote {
                    higher: vote,
                    sender_vote: self.session_id.vote_ref().clone(),
                }))
            }
            AppendEntriesResponse::Conflict => {
                let conflict = sending_range.prev.clone();

                let conflict = conflict.unwrap();
                self.send_progress(request_id, ReplicationResult::new(leader_time, Err(conflict)));

                Ok(None)
            }
        }
    }

    fn send_progress_error(&mut self, request_id: RequestId, err: RPCError<C::NodeId, C::Node>) {
        let _ = self.tx_raft_core.send(Notify::Network {
            response: Response::Progress {
                target: self.target.clone(),
                request_id,
                result: Err(err.to_string()),
                session_id: self.session_id.clone(),
            },
        });
    }

    fn send_progress(&mut self, request_id: RequestId, replication_result: ReplicationResult<C>) {

        match &replication_result.result {
            Ok(matching) => {
                self.validate_matching(matching.clone());
                self.matching = matching.clone();
            }
            Err(_conflict) => {
            }
        }

        let _ = self.tx_raft_core.send({
            Notify::Network {
                response: Response::Progress {
                    session_id: self.session_id.clone(),
                    request_id,
                    target: self.target.clone(),
                    result: Ok(replication_result),
                },
            }
        });
    }

    fn validate_matching(&self, matching: Option<LogId<C::NodeId>>) {
            if self.matching > matching {
                tracing::warn!(
                    "follower log is reverted from {} to {}; with 'loosen-follower-log-revert' enabled, this is allowed",
                    self.matching.display(),
                    matching.display(),
                );
            }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn backoff_drain_events(&mut self, until: InstantOf<C>) -> Result<(), ReplicationClosed> {
        let d = until - C::now();
        tracing::warn!(
            interval = debug(d),
            "{} backoff mode: drain events without processing them",
            func_name!()
        );

        loop {
            let sleep_duration = until - C::now();
            let sleep = C::sleep(sleep_duration);

            let recv = self.rx_event.recv();

            select! {
                _ = sleep => {
                    return Ok(());
                }
                recv_res = recv => {
                    let event = recv_res.ok_or(ReplicationClosed::new("RaftCore closed replication"))?;
                    self.process_event(event);
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn drain_events(&mut self) -> Result<(), ReplicationClosed> {

        if self.next_action.is_none() {
            let event =
                self.rx_event.recv().await.ok_or(ReplicationClosed::new("rx_repl is closed in drain_event()"))?;
            self.process_event(event);
        }


        self.try_drain_events().await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn try_drain_events(&mut self) -> Result<(), ReplicationClosed> {

        loop {
            let maybe_res = self.rx_event.recv().now_or_never();

            let Some(recv_res) = maybe_res else {
                return Ok(());
            };

            let event = recv_res.ok_or(ReplicationClosed::new("rx_repl is closed in try_drain_event"))?;

            self.process_event(event);
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub fn process_event(&mut self, event: Replicate<C>) {

        match event {
            Replicate::Committed(c) => {
                self.committed = c;

                if self.next_action.is_none() {
                    self.next_action = Some(Data::new_heartbeat());
                }
            }
            Replicate::Heartbeat => {
                if self.next_action.is_none() {
                    self.next_action = Some(Data::new_heartbeat());
                }
            }
            Replicate::Data(d) => {


                self.next_action = Some(d);
            }
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn stream_snapshot(
        &mut self,
        snapshot_req: DataWithId<Option<LogIdOf<C>>>,
    ) -> Result<Option<Data<C>>, ReplicationError<C::NodeId, C::Node>> {
        let request_id = snapshot_req.request_id();

        tracing::info!(request_id = display(request_id), "{}", func_name!());

        let snapshot = self.snapshot_reader.get_snapshot().await.map_err(|reason| {
            tracing::warn!(error = display(&reason), "failed to get snapshot from state machine");
            ReplicationClosed::new(reason)
        })?;

        tracing::info!(
            "received snapshot: request_id={}; meta:{}",
            request_id,
            snapshot.as_ref().map(|x| &x.meta).summary()
        );

        let snapshot = match snapshot {
            None => {
                let io_err = StorageIOError::read_snapshot(None, AnyError::error("snapshot not found"));
                let sto_err = StorageError::IO { source: io_err };
                return Err(ReplicationError::StorageError(sto_err));
            }
            Some(x) => x,
        };

        let mut option = RPCOption::new(self.config.install_snapshot_timeout());
        option.snapshot_chunk_size = Some(self.config.snapshot_max_chunk_size as usize);

        let (tx_cancel, rx_cancel) = oneshot::channel();

        let jh = C::spawn(Self::send_snapshot(
            request_id,
            self.snapshot_network.clone(),
            self.session_id.vote_ref().clone(),
            snapshot,
            option,
            rx_cancel,
            self.weak_tx_event.clone(),
        ));

        self.snapshot_state = Some((tx_cancel, jh));
        Ok(None)
    }

    async fn send_snapshot(
        request_id: RequestId,
        network: Arc<Mutex<N::Network>>,
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        option: RPCOption,
        cancel: oneshot::Receiver<()>,
        weak_tx: mpsc::WeakUnboundedSender<Replicate<C>>,
    ) {
        let meta = snapshot.meta.clone();

        let mut net = network.lock().await;

        let start_time = C::now();

        let cancel = async move {
            let _ = cancel.await;
            ReplicationClosed::new("ReplicationCore is dropped")
        };

        let res = net.full_snapshot(vote, snapshot, cancel, option).await;
        if let Err(e) = &res {
            tracing::warn!(error = display(e), "failed to send snapshot");
        }

        let res = res.decompose_infallible();

        if let Some(tx_noty) = weak_tx.upgrade() {
            let data = Data::new_snapshot_callback(request_id, start_time, meta, res);
            let send_res = tx_noty.send(Replicate::new_data(data));
            if send_res.is_err() {
                tracing::warn!("weak_tx failed to send snapshot result to ReplicationCore");
            }
        } else {
            tracing::warn!("weak_tx is dropped, no response is sent to ReplicationCore");
        }
    }

    fn handle_snapshot_callback(
        &mut self,
        callback: DataWithId<SnapshotCallback<C>>,
    ) -> Result<Option<Data<C>>, ReplicationError<C::NodeId, C::Node>> {
        self.snapshot_state = None;

        let request_id = callback.request_id();
        let SnapshotCallback {
            start_time,
            result,
            snapshot_meta,
        } = callback.into_data();

        let resp = result?;

        let sender_vote = self.session_id.vote_ref().clone();
        if resp.vote > sender_vote {
            return Err(ReplicationError::HigherVote(HigherVote {
                higher: resp.vote,
                sender_vote,
            }));
        }

        self.send_progress(
            request_id,
            ReplicationResult::new(start_time, Ok(snapshot_meta.last_log_id)),
        );

        Ok(None)
    }

    fn finish_success_append(
        &mut self,
        matching: Option<LogId<C::NodeId>>,
        leader_time: InstantOf<C>,
        log_ids: DataWithId<LogIdRange<C::NodeId>>,
    ) -> Option<Data<C>> {
        self.send_progress(
            log_ids.request_id(),
            ReplicationResult::new(leader_time, Ok(matching.clone())),
        );

        if matching < log_ids.data().last {
            Some(Data::new_logs(
                log_ids.request_id(),
                LogIdRange::new(matching, log_ids.data().last.clone()),
            ))
        } else {
            None
        }
    }
}
