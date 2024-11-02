use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use flexkv::error::NetworkError;
use flexkv::error::RPCError;
use flexkv::error::RemoteError;
use flexkv::error::Unreachable;
use flexkv::RaftMetrics;
use flexkv::TryAsRef;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::typ;
use crate::Node;
use crate::NodeId;
use crate::Request;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct ExampleClient {
    pub leader: Arc<Mutex<(NodeId, String)>>,

    pub inner: Client,
}

impl ExampleClient {
    pub fn new(leader_id: NodeId, leader_addr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: Client::new(),
        }
    }


    pub async fn write(&self, req: &Request) -> Result<typ::ClientWriteResponse, typ::RPCError<typ::ClientWriteError>> {
        self.send_rpc_to_leader("api/write", Some(req)).await
    }

    pub async fn read(&self, req: &String) -> Result<String, typ::RPCError> {
        self.do_send_rpc_to_leader("api/read", Some(req)).await
    }

    pub async fn consistent_read(&self, req: &String) -> Result<String, typ::RPCError<typ::CheckIsLeaderError>> {
        self.do_send_rpc_to_leader("api/consistent_read", Some(req)).await
    }


    pub async fn init(&self) -> Result<(), typ::RPCError<typ::InitializeError>> {
        self.do_send_rpc_to_leader("cluster/init", Some(&Empty {})).await
    }

    pub async fn add_learner(
        &self,
        req: (NodeId, String, String),
    ) -> Result<typ::ClientWriteResponse, typ::RPCError<typ::ClientWriteError>> {
        self.send_rpc_to_leader("cluster/add-learner", Some(&req)).await
    }

    pub async fn change_membership(
        &self,
        req: &BTreeSet<NodeId>,
    ) -> Result<typ::ClientWriteResponse, typ::RPCError<typ::ClientWriteError>> {
        self.send_rpc_to_leader("cluster/change-membership", Some(req)).await
    }

    pub async fn metrics(&self) -> Result<RaftMetrics<NodeId, Node>, typ::RPCError> {
        self.do_send_rpc_to_leader("cluster/metrics", None::<&()>).await
    }


    async fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<NodeId, Node, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let t = self.leader.lock().unwrap();
            let target_addr = &t.1;
            (t.0, format!("http://{}/{}", target_addr, uri))
        };

        let resp = if let Some(r) = req {
            self.inner.post(url.clone()).json(r)
        } else {
            self.inner.get(url.clone())
        }
        .send()
        .await
        .map_err(|e| {
            if e.is_connect() {
                return RPCError::Unreachable(Unreachable::new(&e));
            }
            RPCError::Network(NetworkError::new(&e))
        })?;

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }

    async fn send_rpc_to_leader<Req, Resp, Err>(&self, uri: &str, req: Option<&Req>) -> Result<Resp, typ::RPCError<Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned + TryAsRef<typ::ForwardToLeader> + Clone,
    {
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, typ::RPCError<Err>> = self.do_send_rpc_to_leader(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let raft_err: &typ::RaftError<_> = &remote_err.source;

                if let Some(typ::ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                }) = raft_err.forward_to_leader()
                {
                    {
                        let mut t = self.leader.lock().unwrap();
                        let api_addr = leader_node.api_addr.clone();
                        *t = (*leader_id, api_addr);
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }
}
