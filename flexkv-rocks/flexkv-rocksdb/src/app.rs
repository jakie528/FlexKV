use std::collections::BTreeMap;
use std::sync::Arc;

use flexkv::Config;
use tokio::sync::RwLock;

use crate::ExampleRaft;
use crate::NodeId;

pub struct App {
    pub id: NodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: ExampleRaft,
    pub key_values: Arc<RwLock<BTreeMap<String, String>>>,
    pub config: Arc<Config>,
}
