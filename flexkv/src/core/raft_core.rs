use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryFutureExt;
use maplit::btreeset;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::async_runtime::AsyncOneshotSendExt;
use crate::config::Config;
use crate::config::RuntimeConfig;
use crate::core::balancer::Balancer;
use crate::core::command_state::CommandState;
use crate::core::notify::Notify;
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::AppendEntriesTx;
use crate::core::raft_msg::ClientReadTx;
use crate::core::raft_msg::RaftMsg;
use crate::core::raft_msg::ResultSender;
use crate::core::raft_msg::VoteTx;
use crate::core::state_machine;
use crate::core::state_machine::handle;
use crate::core::state_machine::CommandSeq;
use crate::core::ServerState;
use crate::display_ext::DisplayInstantExt;
use crate::display_ext::DisplayOptionExt;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::Respond;
use crate::entry::FromAppData;
use crate::entry::RaftEntry;
use crate::error::ClientWriteError;
use crate::error::Fatal;
use crate::error::ForwardToLeader;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::error::QuorumNotEnough;
use crate::error::RPCError;
use crate::error::Timeout;
use crate::log_id::LogIdOptionExt;
use crate::log_id::RaftLogId;
use crate::metrics::RaftDataMetrics;
use crate::metrics::RaftMetrics;
use crate::metrics::RaftServerMetrics;
use crate::metrics::ReplicationMetrics;
use crate::network::RPCOption;
use crate::network::RPCTypes;
use crate::network::RaftNetwork;
use crate::network::RaftNetworkFactory;
use crate::progress::entry::ProgressEntry;
use crate::progress::Inflight;
use crate::progress::Progress;
use crate::quorum::QuorumSet;
use crate::raft::responder::Responder;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::raft::ClientWriteResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::LogIOId;
use crate::raft_state::LogStateReader;
use crate::replication;
use crate::replication::request::Replicate;
use crate::replication::request_id::RequestId;
use crate::replication::response::ReplicationResult;
use crate::replication::ReplicationCore;
use crate::replication::ReplicationHandle;
use crate::replication::ReplicationSessionId;
use crate::runtime::RaftRuntime;
use crate::storage::LogFlushed;
use crate::storage::RaftLogReaderExt;
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::ResponderOf;
use crate::type_config::TypeConfigExt;
use crate::AsyncRuntime;
use crate::ChangeMembers;
use crate::Instant;
use crate::LogId;
use crate::Membership;
use crate::MessageSummary;
use crate::Node;
use crate::NodeId;
use crate::OptionalSend;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::StorageIOError;
use crate::Vote;

#[derive(Debug)]
pub(crate) struct ApplyingEntry<NID: NodeId, N: Node> {
    log_id: LogId<NID>,
    membership: Option<Membership<NID, N>>,
}

impl<NID: NodeId, N: Node> ApplyingEntry<NID, N> {
    pub(crate) fn new(log_id: LogId<NID>, membership: Option<Membership<NID, N>>) -> Self {
        Self { log_id, membership }
    }
}

pub(crate) struct ApplyResult<C: RaftTypeConfig> {
    pub(crate) since: u64,
    pub(crate) end: u64,
    pub(crate) last_applied: LogId<C::NodeId>,
    pub(crate) applying_entries: Vec<ApplyingEntry<C::NodeId, C::Node>>,
    pub(crate) apply_results: Vec<C::R>,
}

impl<C: RaftTypeConfig> Debug for ApplyResult<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplyResult")
            .field("since", &self.since)
            .field("end", &self.end)
            .field("last_applied", &self.last_applied)
            .finish()
    }
}

pub(crate) struct LeaderData<C: RaftTypeConfig> {
    pub(crate) next_heartbeat: <C::AsyncRuntime as AsyncRuntime>::Instant,
}

impl<C: RaftTypeConfig> LeaderData<C> {
    pub(crate) fn new() -> Self {
        Self {
            next_heartbeat: C::now(),
        }
    }
}

pub struct RaftCore<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    pub(crate) id: C::NodeId,

    pub(crate) config: Arc<Config>,

    pub(crate) runtime_config: Arc<RuntimeConfig>,

    pub(crate) network: N,

    pub(crate) log_store: LS,

    pub(crate) sm_handle: handle::Handle<C>,

    pub(crate) engine: Engine<C>,

    pub(crate) client_resp_channels: BTreeMap<u64, ResponderOf<C>>,

    pub(crate) replications: BTreeMap<C::NodeId, ReplicationHandle<C>>,

    pub(crate) leader_data: Option<LeaderData<C>>,

    #[allow(dead_code)]
    pub(crate) tx_api: mpsc::UnboundedSender<RaftMsg<C>>,
    pub(crate) rx_api: mpsc::UnboundedReceiver<RaftMsg<C>>,

    pub(crate) tx_notify: mpsc::UnboundedSender<Notify<C>>,

    pub(crate) rx_notify: mpsc::UnboundedReceiver<Notify<C>>,

    pub(crate) tx_metrics: watch::Sender<RaftMetrics<C::NodeId, C::Node>>,
    pub(crate) tx_data_metrics: watch::Sender<RaftDataMetrics<C::NodeId>>,
    pub(crate) tx_server_metrics: watch::Sender<RaftServerMetrics<C::NodeId, C::Node>>,

    pub(crate) command_state: CommandState,

    pub(crate) span: Span,

    pub(crate) _p: PhantomData<SM>,
}

impl<C, N, LS, SM> RaftCore<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    pub(crate) async fn main(
        mut self,
        rx_shutdown: <C::AsyncRuntime as AsyncRuntime>::OneshotReceiver<()>,
    ) -> Result<Infallible, Fatal<C::NodeId>> {
        let span = tracing::span!(parent: &self.span, Level::DEBUG, "main");
        let res = self.do_main(rx_shutdown).instrument(span).await;

        self.report_metrics(None);

        let err = res.unwrap_err();
        match err {
            Fatal::Stopped => { /* Normal quit */ }
            _ => {
                tracing::error!(error = display(&err), "quit RaftCore::main on error");
            }
        }

        {
            let mut curr = self.tx_metrics.borrow().clone();
            curr.state = ServerState::Shutdown;
            curr.running_state = Err(err.clone());

            let _ = self.tx_metrics.send(curr);
        }

        Err(err)
    }

    #[tracing::instrument(level="trace", skip_all, fields(id=display(&self.id), cluster=%self.config.cluster_name))]
    async fn do_main(
        &mut self,
        rx_shutdown: <C::AsyncRuntime as AsyncRuntime>::OneshotReceiver<()>,
    ) -> Result<Infallible, Fatal<C::NodeId>> {

        self.engine.startup();
        self.run_engine_commands().await?;

        self.report_metrics(None);

        self.runtime_loop(rx_shutdown).await
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) async fn handle_check_is_leader_request(&mut self, tx: ClientReadTx<C>) {

        let resp = {
            let l = self.engine.leader_operator();
            let lh = match l {
                Ok(leading_operator) => leading_operator,
                Err(forward) => {
                    let _ = tx.send(Err(forward.into()));
                    return;
                }
            };

            let read_log_id = lh.get_read_log_id();

            let applied = self.engine.state.io_applied().cloned();

            (read_log_id, applied)
        };

        let my_id = self.id.clone();
        let my_vote = self.engine.state.vote_ref().clone();
        let ttl = Duration::from_millis(self.config.heartbeat_interval);
        let eff_mem = self.engine.state.membership_state.effective().clone();
        let core_tx = self.tx_notify.clone();

        let mut granted = btreeset! {my_id.clone()};

        if eff_mem.is_quorum(granted.iter()) {
            let _ = tx.send(Ok(resp));
            return;
        }

        let mut pending = FuturesUnordered::new();

        let voter_progresses = {
            let l = &self.engine.leader.as_ref().unwrap();
            l.progress.iter().filter(|(id, _v)| l.progress.is_voter(id) == Some(true))
        };

        for (target, progress) in voter_progresses {
            let target = target.clone();

            if target == my_id {
                continue;
            }

            let rpc = AppendEntriesRequest {
                vote: my_vote.clone(),
                prev_log_id: progress.matching.clone(),
                entries: vec![],
                leader_commit: self.engine.state.committed().cloned(),
            };

            let target_node = eff_mem.get_node(&target).unwrap().clone();
            let mut client = self.network.new_client(target.clone(), &target_node).await;

            let option = RPCOption::new(ttl);

            let fu = {
                let my_id = my_id.clone();
                let target = target.clone();
                async move {
                    let outer_res = C::AsyncRuntime::timeout(ttl, client.append_entries(rpc, option)).await;
                    match outer_res {
                        Ok(append_res) => match append_res {
                            Ok(x) => Ok((target.clone(), x)),
                            Err(err) => Err((target.clone(), err)),
                        },
                        Err(_timeout) => {
                            let timeout_err = Timeout {
                                action: RPCTypes::AppendEntries,
                                id: my_id,
                                target: target.clone(),
                                timeout: ttl,
                            };

                            Err((target, RPCError::Timeout(timeout_err)))
                        }
                    }
                }
            };

            let fu = fu.instrument(tracing::debug_span!("spawn_is_leader", target = target.to_string()));
            let task = C::AsyncRuntime::spawn(fu).map_err(move |err| (target, err));

            pending.push(task);
        }

        let waiting_fu = async move {
            while let Some(res) = pending.next().await {
                let (target, append_res) = match res {
                    Ok(Ok(res)) => res,
                    Ok(Err((target, err))) => {
                        tracing::error!(target=display(&target), error=%err, "timeout while confirming leadership for read request");
                        continue;
                    }
                    Err((target, err)) => {
                        tracing::error!(target = display(&target), "fail to join task: {}", err);
                        continue;
                    }
                };

                if let AppendEntriesResponse::HigherVote(vote) = append_res {

                    let send_res = core_tx.send(Notify::HigherVote {
                        target,
                        higher: vote,
                        sender_vote: my_vote,
                    });

                    if let Err(_e) = send_res {
                        tracing::error!("fail to send HigherVote to RaftCore");
                    }

                    let err = ForwardToLeader::empty();
                    let _ = tx.send(Err(err.into()));
                    return;
                }

                granted.insert(target);

                if eff_mem.is_quorum(granted.iter()) {
                    let _ = tx.send(Ok(resp));
                    return;
                }
            }


            let _ = tx.send(Err(QuorumNotEnough {
                cluster: eff_mem.membership().summary(),
                got: granted,
            }
            .into()));
        };


        #[allow(clippy::let_underscore_future)]
        let _ = C::AsyncRuntime::spawn(waiting_fu.instrument(tracing::debug_span!("spawn_is_leader_waiting")));
    }

    #[tracing::instrument(level = "debug", skip(self, tx))]
    pub(super) fn change_membership(
        &mut self,
        changes: ChangeMembers<C::NodeId, C::Node>,
        retain: bool,
        tx: ResponderOf<C>,
    ) {
        let res = self.engine.state.membership_state.change_handler().apply(changes, retain);
        let new_membership = match res {
            Ok(x) => x,
            Err(e) => {
                tx.send(Err(ClientWriteError::ChangeMembershipError(e)));
                return;
            }
        };

        let ent = C::Entry::new_membership(LogId::default(), new_membership);
        self.write_entry(ent, Some(tx));
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id = display(&self.id)))]
    pub fn write_entry(&mut self, entry: C::Entry, resp_tx: Option<ResponderOf<C>>) -> bool {

        let (mut lh, tx) = if let Some((lh, tx)) = self.engine.get_leader_operator_or_reject(resp_tx) {
            (lh, tx)
        } else {
            return false;
        };

        let entries = vec![entry];
        lh.leader_append_entries(entries);
        let index = lh.state.last_log_id().unwrap().index;

        if let Some(tx) = tx {
            self.client_resp_channels.insert(index, tx);
        }

        true
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id = display(&self.id)))]
    pub fn send_heartbeat(&mut self, _emitter: impl Display) -> bool {

        let mut lh = if let Some((lh, _)) = self.engine.get_leader_operator_or_reject(None) {
            lh
        } else {
            return false;
        };

        lh.send_heartbeat();

        true
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn flush_metrics(&mut self) {
        let leader_metrics = if let Some(leader) = self.engine.leader.as_ref() {
            let prog = &leader.progress;
            Some(
                prog.iter()
                    .map(|(id, p)| {
                        (
                            id.clone(),
                            <ProgressEntry<<C as RaftTypeConfig>::NodeId> as Borrow<Option<LogId<C::NodeId>>>>::borrow(
                                p,
                            )
                            .clone(),
                        )
                    })
                    .collect(),
            )
        } else {
            None
        };
        self.report_metrics(leader_metrics);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn report_metrics(&mut self, replication: Option<ReplicationMetrics<C::NodeId>>) {
        let last_quorum_acked = self.last_quorum_acked_time();
        let millis_since_quorum_ack = last_quorum_acked.map(|t| t.elapsed().as_millis() as u64);

        let st = &self.engine.state;

        let membership_config = st.membership_state.effective().stored_membership().clone();
        let current_leader = self.current_leader();

        let m = RaftMetrics {
            running_state: Ok(()),
            id: self.id.clone(),

            current_term: st.vote_ref().leader_id().get_term(),
            vote: st.io_state().vote().clone(),
            last_log_index: st.last_log_id().index(),
            last_applied: st.io_applied().cloned(),
            snapshot: st.io_snapshot_last_log_id().cloned(),
            purged: st.io_purged().cloned(),

            state: st.server_state,
            current_leader: current_leader.clone(),
            millis_since_quorum_ack,
            membership_config: membership_config.clone(),

            replication: replication.clone(),
        };

        let data_metrics = RaftDataMetrics {
            last_log: st.last_log_id().cloned(),
            last_applied: st.io_applied().cloned(),
            snapshot: st.io_snapshot_last_log_id().cloned(),
            purged: st.io_purged().cloned(),
            millis_since_quorum_ack,
            replication,
        };

        let server_metrics = RaftServerMetrics {
            id: self.id.clone(),
            vote: st.io_state().vote().clone(),
            state: st.server_state,
            current_leader,
            membership_config,
        };


        self.tx_data_metrics.send_if_modified(|metrix| {
            if data_metrics.ne(metrix) {
                *metrix = data_metrics.clone();
                return true;
            }
            false
        });

        self.tx_server_metrics.send_if_modified(|metrix| {
            if server_metrics.ne(metrix) {
                *metrix = server_metrics.clone();
                return true;
            }
            false
        });

        let res = self.tx_metrics.send(m);

        if let Err(err) = res {
            tracing::error!(error=%err, id=display(&self.id), "error reporting metrics");
        }
    }

    #[tracing::instrument(level = "debug", skip(self, tx))]
    pub(crate) fn handle_initialize(
        &mut self,
        member_nodes: BTreeMap<C::NodeId, C::Node>,
        tx: ResultSender<C, (), InitializeError<C::NodeId, C::Node>>,
    ) {

        let membership = Membership::from(member_nodes);

        let entry = C::Entry::new_membership(LogId::default(), membership);
        let res = self.engine.initialize(entry);
        self.engine.output.push_command(Command::Respond {
            when: None,
            resp: Respond::new(res, tx),
        });
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn trigger_snapshot(&mut self) {
        self.engine.snapshot_operator().trigger_snapshot();
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(crate) fn reject_with_forward_to_leader<T: OptionalSend, E>(&self, tx: ResultSender<C, T, E>)
    where E: From<ForwardToLeader<C::NodeId, C::Node>> + OptionalSend {
        let mut leader_id = self.current_leader();
        let leader_node = self.get_leader_node(leader_id.clone());

        if leader_node.is_none() {
            leader_id = None;
        }

        let err = ForwardToLeader { leader_id, leader_node };

        let _ = tx.send(Err(err.into()));
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn current_leader(&self) -> Option<C::NodeId> {
        let vote = self.engine.state.vote_ref();
        if !vote.is_committed() {
            return None;
        }

        let id = vote.leader_id().voted_for().unwrap();

        if self.engine.state.membership_state.effective().is_voter(&id) {
            Some(id)
        } else {
            None
        }
    }

    fn last_quorum_acked_time(&mut self) -> Option<InstantOf<C>> {
        let leading = self.engine.leader.as_mut();
        leading.and_then(|l| l.last_quorum_acked_time())
    }

    pub(crate) fn get_leader_node(&self, leader_id: Option<C::NodeId>) -> Option<C::Node> {
        let leader_id = match leader_id {
            None => return None,
            Some(x) => x,
        };

        self.engine.state.membership_state.effective().get_node(&leader_id).cloned()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn append_to_log<I>(
        &mut self,
        entries: I,
        vote: Vote<C::NodeId>,
        last_log_id: LogId<C::NodeId>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (tx, rx) = C::AsyncRuntime::oneshot();
        let log_io_id = LogIOId::new(vote, Some(last_log_id));

        let callback = LogFlushed::new(log_io_id, tx);

        self.log_store.append(entries, callback).await?;
        rx.await
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?
            .map_err(|e| StorageIOError::write_logs(AnyError::error(e)))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn apply_to_state_machine(
        &mut self,
        seq: CommandSeq,
        since: u64,
        upto_index: u64,
    ) -> Result<(), StorageError<C::NodeId>> {
        let end = upto_index + 1;

        if since == end {
            return Ok(());
        }

        let entries = self.log_store.get_log_entries(since..end).await?;

        let last_applied = entries[entries.len() - 1].get_log_id().clone();

        let cmd = state_machine::Command::apply(entries).with_seq(seq);
        self.sm_handle.send(cmd).map_err(|e| StorageIOError::apply(last_applied, AnyError::error(e)))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn handle_apply_result(&mut self, res: ApplyResult<C>) {

        let mut results = res.apply_results.into_iter();
        let mut applying_entries = res.applying_entries.into_iter();

        for log_index in res.since..res.end {
            let ent = applying_entries.next().unwrap();
            let apply_res = results.next().unwrap();
            let tx = self.client_resp_channels.remove(&log_index);

            Self::send_response(ent, apply_res, tx);
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn send_response(entry: ApplyingEntry<C::NodeId, C::Node>, resp: C::R, tx: Option<ResponderOf<C>>) {
        let tx = match tx {
            None => return,
            Some(x) => x,
        };

        let membership = entry.membership;

        let res = Ok(ClientWriteResponse {
            log_id: entry.log_id,
            data: resp,
            membership,
        });

        tx.send(res);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    #[allow(clippy::type_complexity)]
    pub(crate) async fn spawn_replication_stream(
        &mut self,
        target: C::NodeId,
        progress_entry: ProgressEntry<C::NodeId>,
    ) -> ReplicationHandle<C> {
        let target_node = self.engine.state.membership_state.effective().get_node(&target).unwrap();

        let membership_log_id = self.engine.state.membership_state.effective().log_id();
        let network = self.network.new_client(target.clone(), target_node).await;
        let snapshot_network = self.network.new_client(target.clone(), target_node).await;

        let leader = self.engine.leader.as_ref().unwrap();

        let session_id = ReplicationSessionId::new(leader.vote.clone(), membership_log_id.clone());

        ReplicationCore::<C, N, LS>::spawn(
            target.clone(),
            session_id,
            self.config.clone(),
            self.engine.state.committed().cloned(),
            progress_entry.matching,
            network,
            snapshot_network,
            self.log_store.get_log_reader().await,
            self.sm_handle.new_snapshot_reader(),
            self.tx_notify.clone(),
            tracing::span!(parent: &self.span, Level::DEBUG, "replication", id=display(&self.id), target=display(&target)),
        )
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_all_replication(&mut self) {
        tracing::info!("remove all replication");

        let nodes = std::mem::take(&mut self.replications);

        for (_target, s) in nodes {
            let handle = s.join_handle;

            drop(s.tx_repl);
            let _x = handle.await;
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn run_engine_commands(&mut self) -> Result<(), StorageError<C::NodeId>> {

        while let Some(cmd) = self.engine.output.pop_command() {

            let res = self.run_command(cmd).await?;

            if let Some(cmd) = res {
                self.engine.output.postpone_command(cmd);
                return Ok(());
            }
        }

        Ok(())
    }

    #[tracing::instrument(level="debug", skip_all, fields(id=display(&self.id)))]
    async fn runtime_loop(
        &mut self,
        mut rx_shutdown: <C::AsyncRuntime as AsyncRuntime>::OneshotReceiver<()>,
    ) -> Result<Infallible, Fatal<C::NodeId>> {
        let mut balancer = Balancer::new(10_000);

        loop {
            self.flush_metrics();


            select! {
                biased;

                _ = &mut rx_shutdown => {
                    tracing::info!("recv from rx_shutdown");
                    return Err(Fatal::Stopped);
                }

                notify_res = self.rx_notify.recv() => {
                    match notify_res {
                        Some(notify) => self.handle_notify(notify)?,
                        None => {
                            tracing::error!("all rx_notify senders are dropped");
                            return Err(Fatal::Stopped);
                        }
                    };
                }

                msg_res = self.rx_api.recv() => {
                    match msg_res {
                        Some(msg) => self.handle_api_msg(msg).await,
                        None => {
                            tracing::info!("all rx_api senders are dropped");
                            return Err(Fatal::Stopped);
                        }
                    };
                }
            }

            self.run_engine_commands().await?;


            let raft_msg_processed = self.process_raft_msg(balancer.raft_msg()).await?;
            let notify_processed = self.process_notify(balancer.notify()).await?;


            #[allow(clippy::collapsible_else_if)]
            if notify_processed == balancer.notify() {
                tracing::info!("there may be more Notify to process, increase Notify ratio");
                balancer.increase_notify();
            } else {
                if raft_msg_processed == balancer.raft_msg() {
                    tracing::info!("there may be more RaftMsg to process, increase RaftMsg ratio");
                    balancer.increase_raft_msg();
                }
            }
        }
    }

    async fn process_raft_msg(&mut self, at_most: u64) -> Result<u64, Fatal<C::NodeId>> {
        for i in 0..at_most {
            let res = self.rx_api.try_recv();
            let msg = match res {
                Ok(msg) => msg,
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => {
                        return Ok(i + 1);
                    }
                    mpsc::error::TryRecvError::Disconnected => {
                        return Err(Fatal::Stopped);
                    }
                },
            };

            self.handle_api_msg(msg).await;


            self.run_engine_commands().await?;
        }

        Ok(at_most)
    }

    async fn process_notify(&mut self, at_most: u64) -> Result<u64, Fatal<C::NodeId>> {
        for i in 0..at_most {
            let res = self.rx_notify.try_recv();
            let notify = match res {
                Ok(msg) => msg,
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => {
                        return Ok(i + 1);
                    }
                    mpsc::error::TryRecvError::Disconnected => {
                        return Err(Fatal::Stopped);
                    }
                },
            };

            self.handle_notify(notify)?;


            self.run_engine_commands().await?;
        }
        Ok(at_most)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(vote=vote_req.summary()))]
    async fn spawn_parallel_vote_requests(&mut self, vote_req: &VoteRequest<C::NodeId>) {
        let members = self.engine.state.membership_state.effective().voter_ids();

        let vote = vote_req.vote.clone();

        for target in members {
            if target == self.id {
                continue;
            }

            let req = vote_req.clone();

            let target_node = self.engine.state.membership_state.effective().get_node(&target).unwrap().clone();
            let mut client = self.network.new_client(target.clone(), &target_node).await;

            let tx = self.tx_notify.clone();

            let ttl = Duration::from_millis(self.config.election_timeout_min);
            let id = self.id.clone();
            let option = RPCOption::new(ttl);

            #[allow(clippy::let_underscore_future)]
            let _ = C::AsyncRuntime::spawn(
                {
                    let target = target.clone();
                    let vote = vote.clone();

                    async move {
                        let tm_res = C::AsyncRuntime::timeout(ttl, client.vote(req, option)).await;
                        let res = match tm_res {
                            Ok(res) => res,

                            Err(_timeout) => {
                                let timeout_err = Timeout {
                                    action: RPCTypes::Vote,
                                    id,
                                    target: target.clone(),
                                    timeout: ttl,
                                };
                                tracing::error!({error = %timeout_err, target = display(&target)}, "timeout");
                                return;
                            }
                        };

                        match res {
                            Ok(resp) => {
                                let _ = tx.send(Notify::VoteResponse {
                                    target,
                                    resp,
                                    sender_vote: vote,
                                });
                            }
                            Err(err) => tracing::error!({error=%err, target=display(&target)}, "while requesting vote"),
                        }
                    }
                }
                .instrument(tracing::debug_span!(
                    parent: &Span::current(),
                    "send_vote_req",
                    target = display(&target)
                )),
            );
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn handle_vote_request(&mut self, req: VoteRequest<C::NodeId>, tx: VoteTx<C>) {
        tracing::info!(req = display(req.summary()), func = func_name!());

        let resp = self.engine.handle_vote_req(req);
        self.engine.output.push_command(Command::Respond {
            when: None,
            resp: Respond::new(Ok(resp), tx),
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn handle_append_entries_request(&mut self, req: AppendEntriesRequest<C>, tx: AppendEntriesTx<C>) {

        let is_ok = self.engine.handle_append_entries(&req.vote, req.prev_log_id, req.entries, Some(tx));

        if is_ok {
            self.engine.handle_commit_entries(req.leader_commit);
        }
    }

    #[tracing::instrument(level = "debug", skip(self, msg), fields(state = debug(self.engine.state.server_state), id=display(&self.id)))]
    pub(crate) async fn handle_api_msg(&mut self, msg: RaftMsg<C>) {

        match msg {
            RaftMsg::AppendEntries { rpc, tx } => {
                self.handle_append_entries_request(rpc, tx);
            }
            RaftMsg::RequestVote { rpc, tx } => {
                let now = C::now();
                tracing::info!(
                    now = display(now.display()),
                    vote_request = display(&rpc),
                    "received RaftMsg::RequestVote: {}",
                    func_name!()
                );

                self.handle_vote_request(rpc, tx);
            }
            RaftMsg::BeginReceivingSnapshot { tx } => {
                self.engine.handle_begin_receiving_snapshot(tx);
            }
            RaftMsg::InstallFullSnapshot { vote, snapshot, tx } => {
                self.engine.handle_install_full_snapshot(vote, snapshot, tx);
            }
            RaftMsg::CheckIsLeaderRequest { tx } => {
                self.handle_check_is_leader_request(tx).await;
            }
            RaftMsg::ClientWriteRequest { app_data, tx } => {
                self.write_entry(C::Entry::from_app_data(app_data), Some(tx));
            }
            RaftMsg::Initialize { members, tx } => {
                tracing::info!(
                    members = debug(&members),
                    "received RaftMsg::Initialize: {}",
                    func_name!()
                );

                self.handle_initialize(members, tx);
            }
            RaftMsg::ChangeMembership { changes, retain, tx } => {
                tracing::info!(
                    members = debug(&changes),
                    retain = debug(&retain),
                    "received RaftMsg::ChangeMembership: {}",
                    func_name!()
                );

                self.change_membership(changes, retain, tx);
            }
            RaftMsg::ExternalCoreRequest { req } => {
                req(&self.engine.state);
            }
            RaftMsg::ExternalCommand { cmd } => {
                tracing::info!(cmd = debug(&cmd), "received RaftMsg::ExternalCommand: {}", func_name!());

                match cmd {
                    ExternalCommand::Elect => {
                        if self.engine.state.membership_state.effective().is_voter(&self.id) {
                            self.engine.elect();
                        } else {
                        }
                    }
                    ExternalCommand::Heartbeat => {
                        self.send_heartbeat("ExternalCommand");
                    }
                    ExternalCommand::Snapshot => self.trigger_snapshot(),
                    ExternalCommand::GetSnapshot { tx } => {
                        let cmd = state_machine::Command::get_snapshot(tx);
                        let res = self.sm_handle.send(cmd);
                        if let Err(e) = res {
                            tracing::error!(error = display(e), "error sending GetSnapshot to sm worker");
                        }
                    }
                    ExternalCommand::PurgeLog { upto } => {
                        self.engine.trigger_purge_log(upto);
                    }
                }
            }
        };
    }

    #[tracing::instrument(level = "debug", skip_all, fields(state = debug(self.engine.state.server_state), id=display(&self.id)))]
    pub(crate) fn handle_notify(&mut self, notify: Notify<C>) -> Result<(), Fatal<C::NodeId>> {
        match notify {
            Notify::VoteResponse {
                target,
                resp,
                sender_vote,
            } => {
                let now = C::now();

                tracing::info!(
                    now = display(now.display()),
                    resp = display(&resp),
                    "received Notify::VoteResponse: {}",
                    func_name!()
                );

                if self.does_vote_match(&sender_vote, "VoteResponse") {
                    self.engine.handle_vote_resp(target, resp);
                }
            }

            Notify::HigherVote {
                target,
                higher,
                sender_vote,
            } => {
                tracing::info!(
                    target = display(&target),
                    higher_vote = display(&higher),
                    sending_vote = display(&sender_vote),
                    "received Notify::HigherVote: {}",
                    func_name!()
                );

                if self.does_vote_match(&sender_vote, "HigherVote") {
                    let _ = self.engine.vote_operator().update_vote(&higher);
                }
            }

            Notify::Tick { i: _ } => {

                let now = C::now();

                self.handle_tick_election();


                let heartbeat_at = self.leader_data.as_ref().map(|x| x.next_heartbeat);
                if let Some(t) = heartbeat_at {
                    if now >= t {
                        if self.runtime_config.enable_heartbeat.load(Ordering::Relaxed) {
                            self.send_heartbeat("tick");
                        }

                        if let Some(l) = &mut self.leader_data {
                            l.next_heartbeat = C::now() + Duration::from_millis(self.config.heartbeat_interval);
                        }
                    }
                }


                if self.engine.state.io_applied() >= self.engine.state.membership_state.effective().log_id().as_ref() {
                    self.engine.leader_step_down();
                }
            }

            Notify::Network { response } => {
                match response {
                    replication::Response::Progress {
                        target,
                        request_id: id,
                        result,
                        session_id,
                    } => {
                        if self.does_replication_session_match(&session_id, "UpdateReplicationMatched") {
                            self.handle_replication_progress(target, id, result);
                        }
                    }

                    replication::Response::StorageError { error } => {
                        tracing::error!(
                            error = display(&error),
                            "received Notify::ReplicationStorageError: {}",
                            func_name!()
                        );

                        return Err(Fatal::from(error));
                    }

                    replication::Response::HigherVote {
                        target,
                        higher,
                        sender_vote,
                    } => {
                        tracing::info!(
                            target = display(&target),
                            higher_vote = display(&higher),
                            sender_vote = display(&sender_vote),
                            "received Notify::HigherVote: {}",
                            func_name!()
                        );

                        if self.does_vote_match(&sender_vote, "HigherVote") {
                            let _ = self.engine.vote_operator().update_vote(&higher);
                        }
                    }
                }
            }

            Notify::StateMachine { command_result } => {

                let seq = command_result.command_seq;
                let res = command_result.result?;

                match res {
                    state_machine::Response::BuildSnapshot(_) => {}
                    _ => {
                    }
                }
                self.command_state.finished_sm_seq = seq;

                match res {
                    state_machine::Response::BuildSnapshot(meta) => {
                        tracing::info!(
                            "state_machine::StateMachine command done: BuildSnapshot: {}: {}",
                            meta.summary(),
                            func_name!()
                        );


                        let last_log_id = meta.last_log_id.clone();
                        self.engine.finish_building_snapshot(meta);

                        let st = self.engine.state.io_state_mut();
                        st.update_snapshot(last_log_id);
                    }
                    state_machine::Response::InstallSnapshot(meta) => {
                        tracing::info!(
                            "state_machine::StateMachine command done: InstallSnapshot: {}: {}",
                            meta.summary(),
                            func_name!()
                        );

                        if let Some(meta) = meta {
                            let st = self.engine.state.io_state_mut();
                            st.update_applied(meta.last_log_id.clone());
                            st.update_snapshot(meta.last_log_id);
                        }
                    }
                    state_machine::Response::Apply(res) => {
                        self.engine.state.io_state_mut().update_applied(Some(res.last_applied.clone()));

                        self.handle_apply_result(res);
                    }
                }
            }
        };
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn handle_tick_election(&mut self) {
        let now = C::now();

        if self.engine.state.server_state == ServerState::Leader {
            return;
        }

        if !self.engine.state.membership_state.effective().is_voter(&self.id) {
            return;
        }

        if !self.runtime_config.enable_elect.load(Ordering::Relaxed) {
            return;
        }

        if self.engine.state.membership_state.effective().voter_ids().count() != 1 {

            let current_vote = self.engine.state.vote_ref();
            let utime = self.engine.state.vote_last_modified();
            let timer_config = &self.engine.config.timer_config;

            let mut election_timeout = if current_vote.is_committed() {
                timer_config.leader_lease + timer_config.election_timeout
            } else {
                timer_config.election_timeout
            };

            if self.engine.is_there_greater_log() {
                election_timeout += timer_config.smaller_log_timeout;
            }

            if utime > Some(now - election_timeout) {
                return;
            }

            tracing::info!("election timeout passed, check if it is a voter for election");
        }

        self.engine.reset_greater_log();

        tracing::info!("do trigger election");
        self.engine.elect();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn handle_replication_progress(
        &mut self,
        target: C::NodeId,
        request_id: RequestId,
        result: Result<ReplicationResult<C>, String>,
    ) {
        #[allow(clippy::collapsible_if)]
        if self.engine.leader.is_some() {
            self.engine.replication_operator().update_progress(target, request_id, result);
        }
    }

    fn does_vote_match(&self, sender_vote: &Vote<C::NodeId>, msg: impl Display) -> bool {
        let my_vote = if sender_vote.is_committed() {
            let l = self.engine.leader.as_ref();
            l.map(|x| x.vote.clone())
        } else {
            let candidate = self.engine.candidate_ref();
            candidate.map(|x| x.vote_ref().clone())
        };

        if Some(sender_vote) != my_vote.as_ref() {
            tracing::warn!(
                "A message will be ignored because vote changed: msg sent by vote: {}; current my vote: {}; when ({})",
                sender_vote,
                my_vote.display(),
                msg
            );
            false
        } else {
            true
        }
    }
    fn does_replication_session_match(
        &self,
        session_id: &ReplicationSessionId<C::NodeId>,
        msg: impl Display + Copy,
    ) -> bool {
        if !self.does_vote_match(session_id.vote_ref(), msg) {
            return false;
        }

        if &session_id.membership_log_id != self.engine.state.membership_state.effective().log_id() {
            tracing::warn!(
                "membership_log_id changed: msg sent by: {}; curr: {}; ignore when ({})",
                session_id.membership_log_id.summary(),
                self.engine.state.membership_state.effective().log_id().summary(),
                msg
            );
            return false;
        }
        true
    }
}

impl<C, N, LS, SM> RaftRuntime<C> for RaftCore<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    async fn run_command<'e>(&mut self, cmd: Command<C>) -> Result<Option<Command<C>>, StorageError<C::NodeId>> {
        let condition = cmd.condition();

        if let Some(condition) = condition {
            match condition {
                Condition::LogFlushed { .. } => {
                    todo!()
                }
                Condition::Applied { log_id } => {
                    if self.engine.state.io_applied() < log_id.as_ref() {
                        return Ok(Some(cmd));
                    }
                }
                Condition::StateMachineCommand { command_seq } => {
                    if self.command_state.finished_sm_seq < *command_seq {
                        return Ok(Some(cmd));
                    }
                }
            }
        }

        match cmd {
            Command::BecomeLeader => {
                self.leader_data = Some(LeaderData::new());
            }
            Command::QuitLeader => {
                self.leader_data = None;
            }
            Command::AppendInputEntries { vote, entries } => {
                let last_log_id = entries.last().unwrap().get_log_id().clone();

                self.append_to_log(entries, vote, last_log_id.clone()).await?;

                if let Ok(mut lh) = self.engine.leader_operator() {
                    lh.replication_operator().update_local_progress(Some(last_log_id));
                }
            }
            Command::SaveVote { vote } => {
                self.log_store.save_vote(&vote).await?;
                self.engine.state.io_state_mut().update_vote(vote.clone());

                let _ = self.tx_notify.send(Notify::VoteResponse {
                    target: self.id.clone(),
                    resp: VoteResponse::new(vote.clone(), None, true),
                    sender_vote: vote,
                });
            }
            Command::PurgeLog { upto } => {
                self.log_store.purge(upto.clone()).await?;
                self.engine.state.io_state_mut().update_purged(Some(upto));
            }
            Command::DeleteConflictLog { since } => {
                self.log_store.truncate(since.clone()).await?;

                let removed = self.client_resp_channels.split_off(&since.index);
                if !removed.is_empty() {
                    let leader_id = self.current_leader();
                    let leader_node = self.get_leader_node(leader_id.clone());

                    #[allow(clippy::let_underscore_future)]
                    let _ = C::spawn(async move {
                        for (_log_index, tx) in removed.into_iter() {
                            tx.send(Err(ClientWriteError::ForwardToLeader(ForwardToLeader {
                                leader_id: leader_id.clone(),
                                leader_node: leader_node.clone(),
                            })));
                        }
                    });
                }
            }
            Command::SendVote { vote_req } => {
                self.spawn_parallel_vote_requests(&vote_req).await;
            }
            Command::ReplicateCommitted { committed } => {
                for node in self.replications.values() {
                    let _ = node.tx_repl.send(Replicate::Committed(committed.clone()));
                }
            }
            Command::Commit {
                seq,
                ref already_committed,
                ref upto,
            } => {
                self.log_store.save_committed(Some(upto.clone())).await?;
                self.apply_to_state_machine(seq, already_committed.next_index(), upto.index).await?;
            }
            Command::Replicate { req, target } => {
                let node = self.replications.get(&target).expect("replication to target node exists");

                match req {
                    Inflight::None => {
                        let _ = node.tx_repl.send(Replicate::Heartbeat);
                    }
                    Inflight::Logs { id, log_id_range } => {
                        let _ = node.tx_repl.send(Replicate::logs(RequestId::new_append_entries(id), log_id_range));
                    }
                    Inflight::Snapshot { id, last_log_id } => {
                        node.tx_repl.send(Replicate::snapshot(RequestId::new_snapshot(id), last_log_id)).map_err(
                            |_e| StorageIOError::read_snapshot(None, AnyError::error("replication channel closed")),
                        )?;
                    }
                }
            }
            Command::RebuildReplicationStreams { targets } => {
                self.remove_all_replication().await;

                for (target, matching) in targets.iter() {
                    let handle = self.spawn_replication_stream(target.clone(), matching.clone()).await;
                    self.replications.insert(target.clone(), handle);
                }
            }
            Command::StateMachine { command } => {
                self.sm_handle.send(command).map_err(|_e| {
                    StorageIOError::write_state_machine(AnyError::error("can not send to state_machine::Worker".to_string()))
                })?;
            }
            Command::Respond { resp: send, .. } => {
                send.send();
            }
        }

        Ok(None)
    }
}
