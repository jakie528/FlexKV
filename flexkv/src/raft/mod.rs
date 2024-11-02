
mod external_request;
mod impl_raft_blocking_write;
pub(crate) mod message;
mod raft_inner;
pub mod responder;
mod runtime_config_handle;
pub mod trigger;

use std::collections::BTreeMap;
use std::error::Error;

pub(crate) use self::external_request::BoxCoreFn;

pub(in crate::raft) mod core_state;

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use core_state::CoreState;
pub use message::AppendEntriesRequest;
pub use message::AppendEntriesResponse;
pub use message::ClientWriteResponse;
pub use message::ClientWriteResult;
pub use message::InstallSnapshotRequest;
pub use message::InstallSnapshotResponse;
pub use message::SnapshotResponse;
pub use message::VoteRequest;
pub use message::VoteResponse;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::trace_span;
use tracing::Instrument;
use tracing::Level;

use crate::async_runtime::AsyncOneshotSendExt;
use crate::config::Config;
use crate::config::RuntimeConfig;
use crate::core::command_state::CommandState;
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::RaftMsg;
use crate::core::replication_lag;
use crate::core::state_machine::worker;
use crate::core::RaftCore;
use crate::core::Tick;
use crate::engine::Engine;
use crate::engine::EngineConfig;
use crate::error::CheckIsLeaderError;
use crate::error::ClientWriteError;
use crate::error::Fatal;
use crate::error::Infallible;
use crate::error::InitializeError;
use crate::error::RaftError;
use crate::membership::IntoNodes;
use crate::metrics::RaftDataMetrics;
use crate::metrics::RaftMetrics;
use crate::metrics::RaftServerMetrics;
use crate::metrics::Wait;
use crate::metrics::WaitError;
use crate::network::RaftNetworkFactory;
use crate::raft::raft_inner::RaftInner;
use crate::raft::responder::Responder;
pub use crate::raft::runtime_config_handle::RuntimeConfigHandle;
use crate::raft::trigger::Trigger;
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::type_config::alias::ResponderOf;
use crate::type_config::alias::ResponderReceiverOf;
use crate::type_config::alias::SnapshotDataOf;
use crate::type_config::TypeConfigExt;
use crate::AsyncRuntime;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::MessageSummary;
use crate::OptionalSend;
use crate::RaftState;
pub use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::StorageHelper;
use crate::Vote;

#[macro_export]
macro_rules! declare_raft_types {
    ($(#[$outer:meta])* $visibility:vis $id:ident) => {
        $crate::declare_raft_types!($(#[$outer])* $visibility $id:);
    };

    ($(#[$outer:meta])* $visibility:vis $id:ident: $($(#[$inner:meta])* $type_id:ident = $type:ty),* $(,)? ) => {
        $(#[$outer])*
        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
        #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        $visibility struct $id {}

        impl $crate::RaftTypeConfig for $id {
            $crate::flex_macros::expand!(
                KEYED,
                (T, ATTR, V) => {ATTR type T = V;},
                $(($type_id, $(#[$inner])*, $type),)*

                (D            , , String                                ),
                (R            , , String                                ),
                (NodeId       , , u64                                   ),
                (Node         , , $crate::impls::BasicNode              ),
                (Entry        , , $crate::impls::Entry<Self>            ),
                (SnapshotData , , Cursor<Vec<u8>>                       ),
                (Responder    , , $crate::impls::OneshotResponder<Self> ),
                (AsyncRuntime , , $crate::impls::TokioRuntime           ),
            );

        }
    };
}

#[derive(Clone)]
pub struct Raft<C>
where C: RaftTypeConfig
{
    inner: Arc<RaftInner<C>>,
}

impl<C> Raft<C>
where C: RaftTypeConfig
{
    #[tracing::instrument(level="debug", skip_all, fields(cluster=%config.cluster_name))]
    pub async fn new<LS, N, SM>(
        id: C::NodeId,
        config: Arc<Config>,
        network: N,
        mut log_store: LS,
        mut state_machine: SM,
    ) -> Result<Self, Fatal<C::NodeId>>
    where
        N: RaftNetworkFactory<C>,
        LS: RaftLogStorage<C>,
        SM: RaftStateMachine<C>,
    {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_notify, rx_notify) = mpsc::unbounded_channel();
        let (tx_metrics, rx_metrics) = watch::channel(RaftMetrics::new_initial(id.clone()));
        let (tx_data_metrics, rx_data_metrics) = watch::channel(RaftDataMetrics::default());
        let (tx_server_metrics, rx_server_metrics) = watch::channel(RaftServerMetrics::default());
        let (tx_shutdown, rx_shutdown) = C::AsyncRuntime::oneshot();

        let tick_handle = Tick::spawn(
            Duration::from_millis(config.heartbeat_interval * 3 / 2),
            tx_notify.clone(),
            config.enable_tick,
        );

        let runtime_config = Arc::new(RuntimeConfig::new(&config));

        let core_span = tracing::span!(
            parent: tracing::Span::current(),
            Level::DEBUG,
            "RaftCore",
            id = display(&id),
            cluster = display(&config.cluster_name)
        );

        let eng_config = EngineConfig::new::<C::AsyncRuntime>(id.clone(), config.as_ref());

        let state = {
            let mut helper = StorageHelper::new(&mut log_store, &mut state_machine);
            helper.get_initial_state().await?
        };

        let engine = Engine::new(state, eng_config);

        let sm_handle = worker::Worker::spawn(state_machine, tx_notify.clone());

        let core: RaftCore<C, N, LS, SM> = RaftCore {
            id: id.clone(),
            config: config.clone(),
            runtime_config: runtime_config.clone(),
            network,
            log_store,
            sm_handle,

            engine,

            client_resp_channels: BTreeMap::new(),

            replications: Default::default(),
            leader_data: None,

            tx_api: tx_api.clone(),
            rx_api,

            tx_notify,
            rx_notify,

            tx_metrics,
            tx_data_metrics,
            tx_server_metrics,

            command_state: CommandState::default(),
            span: core_span,

            _p: Default::default(),
        };

        let core_handle = C::AsyncRuntime::spawn(core.main(rx_shutdown).instrument(trace_span!("spawn").or_current()));

        let inner = RaftInner {
            id,
            config,
            runtime_config,
            tick_handle,
            tx_api,
            rx_metrics,
            rx_data_metrics,
            rx_server_metrics,
            tx_shutdown: Mutex::new(Some(tx_shutdown)),
            core_state: Mutex::new(CoreState::Running(core_handle)),

            snapshot: Mutex::new(None),
        };

        Ok(Self { inner: Arc::new(inner) })
    }

    pub fn runtime_config(&self) -> RuntimeConfigHandle<C> {
        RuntimeConfigHandle::new(self.inner.as_ref())
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.inner.config
    }

    pub fn trigger(&self) -> Trigger<C> {
        Trigger::new(self.inner.as_ref())
    }

    #[tracing::instrument(level = "debug", skip(self, rpc))]
    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RaftError<C::NodeId>> {

        let (tx, rx) = C::AsyncRuntime::oneshot();
        self.inner.call_core(RaftMsg::AppendEntries { rpc, tx }, rx).await
    }

    #[tracing::instrument(level = "debug", skip(self, rpc))]
    pub async fn vote(&self, rpc: VoteRequest<C::NodeId>) -> Result<VoteResponse<C::NodeId>, RaftError<C::NodeId>> {
        tracing::info!(rpc = display(rpc.summary()), "Raft::vote()");

        let (tx, rx) = C::AsyncRuntime::oneshot();
        self.inner.call_core(RaftMsg::RequestVote { rpc, tx }, rx).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_snapshot(&self) -> Result<Option<Snapshot<C>>, RaftError<C::NodeId>> {

        let (tx, rx) = C::AsyncRuntime::oneshot();
        let cmd = ExternalCommand::GetSnapshot { tx };
        self.inner.call_core(RaftMsg::ExternalCommand { cmd }, rx).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn begin_receiving_snapshot(&self) -> Result<Box<SnapshotDataOf<C>>, RaftError<C::NodeId, Infallible>> {
        tracing::info!("Raft::begin_receiving_snapshot()");

        let (tx, rx) = C::oneshot();
        let resp = self.inner.call_core(RaftMsg::BeginReceivingSnapshot { tx }, rx).await?;
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn install_full_snapshot(
        &self,
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
    ) -> Result<SnapshotResponse<C::NodeId>, Fatal<C::NodeId>> {
        tracing::info!("Raft::install_full_snapshot()");

        let (tx, rx) = C::AsyncRuntime::oneshot();
        let res = self.inner.call_core(RaftMsg::InstallFullSnapshot { vote, snapshot, tx }, rx).await;
        match res {
            Ok(x) => Ok(x),
            Err(e) => {
                Err(e.into_fatal().unwrap())
            }
        }
    }

    pub async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse<C::NodeId>, RaftError<C::NodeId, crate::error::InstallSnapshotError>>
    where
        C::SnapshotData: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin,
    {

        let req_vote = req.vote.clone();
        let my_vote = self.with_raft_state(|state| state.vote_ref().clone()).await?;
        let resp = InstallSnapshotResponse { vote: my_vote.clone() };

        {
            if req_vote >= my_vote {
            } else {
                tracing::info!("vote {} is rejected by local vote: {}", req_vote, my_vote);
                return Ok(resp);
            }
        }

        let finished_snapshot = {
            use crate::network::snapshot_transport::Chunked;
            use crate::network::snapshot_transport::SnapshotTransport;

            let mut streaming = self.inner.snapshot.lock().await;
            Chunked::receive_snapshot(&mut *streaming, self, req).await?
        };

        if let Some(snapshot) = finished_snapshot {
            let resp = self.install_full_snapshot(req_vote, snapshot).await?;
            return Ok(resp.into());
        }
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn current_leader(&self) -> Option<C::NodeId> {
        self.metrics().borrow().current_leader.clone()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn ensure_linearizable(
        &self,
    ) -> Result<Option<LogId<C::NodeId>>, RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>> {
        let (read_log_id, applied) = self.get_read_log_id().await?;

        if read_log_id.index() > applied.index() {
            self.wait(None)
                .applied_index_at_least(read_log_id.index(), "ensure_linearizable")
                .await
                .map_err(|e| match e {
                    WaitError::Timeout(_, _) => {
                        unreachable!("did not specify timeout")
                    }
                    WaitError::ShuttingDown => Fatal::Stopped,
                })?;
        }
        Ok(read_log_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_read_log_id(
        &self,
    ) -> Result<
        (Option<LogId<C::NodeId>>, Option<LogId<C::NodeId>>),
        RaftError<C::NodeId, CheckIsLeaderError<C::NodeId, C::Node>>,
    > {
        let (tx, rx) = C::AsyncRuntime::oneshot();
        let (read_log_id, applied) = self.inner.call_core(RaftMsg::CheckIsLeaderRequest { tx }, rx).await?;
        Ok((read_log_id, applied))
    }

    #[tracing::instrument(level = "debug", skip(self, app_data))]
    pub async fn client_write<E>(
        &self,
        app_data: C::D,
    ) -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>>
    where
        ResponderReceiverOf<C>: Future<Output = Result<ClientWriteResult<C>, E>>,
        E: Error + OptionalSend,
    {
        let rx = self.client_write_ff(app_data).await?;

        let res: ClientWriteResult<C> = self.inner.recv_msg(rx).await?;

        let client_write_response = res.map_err(RaftError::APIError)?;
        Ok(client_write_response)
    }

    #[tracing::instrument(level = "debug", skip(self, app_data))]
    pub async fn client_write_ff(&self, app_data: C::D) -> Result<ResponderReceiverOf<C>, Fatal<C::NodeId>> {
        let (app_data, tx, rx) = ResponderOf::<C>::from_app_data(app_data);

        self.inner.send_msg(RaftMsg::ClientWriteRequest { app_data, tx }).await?;

        Ok(rx)
    }

    pub async fn is_initialized(&self) -> Result<bool, Fatal<C::NodeId>> {
        let initialized = self.with_raft_state(|st| st.is_initialized()).await?;

        Ok(initialized)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn initialize<T>(
        &self,
        members: T,
    ) -> Result<(), RaftError<C::NodeId, InitializeError<C::NodeId, C::Node>>>
    where
        T: IntoNodes<C::NodeId, C::Node> + Debug,
    {
        let (tx, rx) = C::AsyncRuntime::oneshot();
        self.inner
            .call_core(
                RaftMsg::Initialize {
                    members: members.into_nodes(),
                    tx,
                },
                rx,
            )
            .await
    }

    fn check_replication_upto_date(
        &self,
        metrics: &RaftMetrics<C::NodeId, C::Node>,
        node_id: C::NodeId,
        membership_log_id: Option<LogId<C::NodeId>>,
    ) -> Result<Option<LogId<C::NodeId>>, ()> {
        if metrics.membership_config.log_id() < &membership_log_id {
            return Err(());
        }

        if metrics.membership_config.membership().get_node(&node_id).is_none() {
            return Ok(None);
        }

        let repl = match &metrics.replication {
            None => {
                return Ok(None);
            }
            Some(x) => x,
        };

        let replication_metrics = repl;
        let target_metrics = match replication_metrics.get(&node_id) {
            None => {
                return Err(());
            }
            Some(x) => x,
        };

        let matched = target_metrics.clone();

        let distance = replication_lag(&matched.index(), &metrics.last_log_index);

        if distance <= self.inner.config.replication_lag_threshold {
            return Ok(matched);
        }

        Err(())
    }

    pub async fn with_raft_state<F, V>(&self, func: F) -> Result<V, Fatal<C::NodeId>>
    where
        F: FnOnce(&RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>) -> V
            + OptionalSend
            + 'static,
        V: OptionalSend + 'static,
    {
        let (tx, rx) = C::AsyncRuntime::oneshot();

        self.external_request(|st| {
            let result = func(st);
            if let Err(_err) = tx.send(result) {
                tracing::error!("{}: to-Raft tx send error", func_name!());
            }
        });

        match rx.await {
            Ok(res) => Ok(res),
            Err(err) => {
                tracing::error!(error = display(&err), "{}: rx recv error", func_name!());

                let when = format!("{}: rx recv", func_name!());
                let fatal = self.inner.get_core_stopped_error(when, None::<u64>).await;
                Err(fatal)
            }
        }
    }

    pub fn external_request<F>(&self, req: F)
    where F: FnOnce(&RaftState<C::NodeId, C::Node, <C::AsyncRuntime as AsyncRuntime>::Instant>) + OptionalSend + 'static
    {
        let req: BoxCoreFn<C> = Box::new(req);
        let _ignore_error = self.inner.tx_api.send(RaftMsg::ExternalCoreRequest { req });
    }

    pub fn metrics(&self) -> watch::Receiver<RaftMetrics<C::NodeId, C::Node>> {
        self.inner.rx_metrics.clone()
    }

    pub fn data_metrics(&self) -> watch::Receiver<RaftDataMetrics<C::NodeId>> {
        self.inner.rx_data_metrics.clone()
    }

    pub fn server_metrics(&self) -> watch::Receiver<RaftServerMetrics<C::NodeId, C::Node>> {
        self.inner.rx_server_metrics.clone()
    }

    pub fn wait(&self, timeout: Option<Duration>) -> Wait<C::NodeId, C::Node, C::AsyncRuntime> {
        let timeout = match timeout {
            Some(t) => t,
            None => Duration::from_secs(86400 * 365 * 100),
        };
        Wait {
            timeout,
            rx: self.inner.rx_metrics.clone(),
            _phantom: PhantomData,
        }
    }

    pub async fn shutdown(&self) -> Result<(), <C::AsyncRuntime as AsyncRuntime>::JoinError> {
        if let Some(tx) = self.inner.tx_shutdown.lock().await.take() {
            let send_res = tx.send(());
            tracing::info!("sending shutdown signal to RaftCore, sending res: {:?}", send_res);
        }
        self.inner.join_core_task().await;
        if let Some(join_handle) = self.inner.tick_handle.shutdown() {
            let _ = join_handle.await;
        }

        Ok(())
    }
}
