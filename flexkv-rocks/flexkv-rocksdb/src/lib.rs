#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::fmt::Display;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use flexkv::Config;
use tokio::net::TcpListener;
use tokio::task;

use crate::app::App;
use crate::network::api;
use crate::network::management;
use crate::network::Network;
use crate::store::new_storage;
use crate::store::Request;
use crate::store::Response;

pub mod app;
pub mod client;
pub mod network;
pub mod store;
#[cfg(feature = "ffi")]
pub mod ffi;

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node {{ rpc_addr: {}, api_addr: {} }}", self.rpc_addr, self.api_addr)
    }
}

pub type SnapshotData = Cursor<Vec<u8>>;

flexkv::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
);

pub mod typ {
    use flexkv::error::Infallible;

    use crate::Node;
    use crate::NodeId;
    use crate::TypeConfig;

    pub type Entry = flexkv::Entry<TypeConfig>;

    pub type RaftError<E = Infallible> = flexkv::error::RaftError<NodeId, E>;
    pub type RPCError<E = Infallible> = flexkv::error::RPCError<NodeId, Node, RaftError<E>>;

    pub type ClientWriteError = flexkv::error::ClientWriteError<NodeId, Node>;
    pub type CheckIsLeaderError = flexkv::error::CheckIsLeaderError<NodeId, Node>;
    pub type ForwardToLeader = flexkv::error::ForwardToLeader<NodeId, Node>;
    pub type InitializeError = flexkv::error::InitializeError<NodeId, Node>;

    pub type ClientWriteResponse = flexkv::raft::ClientWriteResponse<TypeConfig>;
}

pub type ExampleRaft = flexkv::Raft<TypeConfig>;

type Server = tide::Server<Arc<App>>;

pub async fn start_example_raft_node<P>(
    node_id: NodeId,
    dir: P,
    http_addr: String,
    rpc_addr: String,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let config = Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    let (log_store, state_machine_store) = new_storage(&dir).await;

    let kvs = state_machine_store.data.kvs.clone();

    let network = Network {};

    let raft = flexkv::Raft::new(node_id, config.clone(), network, log_store, state_machine_store).await.unwrap();

    let app = Arc::new(App {
        id: node_id,
        api_addr: http_addr.clone(),
        rpc_addr: rpc_addr.clone(),
        raft,
        key_values: kvs,
        config,
    });

    let echo_service = Arc::new(network::raft::Raft::new(app.clone()));

    let server = toy_rpc::Server::builder().register(echo_service).build();

    let listener = TcpListener::bind(rpc_addr).await.unwrap();
    let handle = task::spawn(async move {
        server.accept_websocket(listener).await.unwrap();
    });

    let mut app: Server = tide::Server::with_state(app);

    management::rest(&mut app);
    api::rest(&mut app);

    app.listen(http_addr.clone()).await?;
    tracing::info!("App Server listening on: {}", http_addr);
    _ = handle.await;
    Ok(())
}
