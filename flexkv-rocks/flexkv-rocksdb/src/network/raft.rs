use std::sync::Arc;

use flexkv::raft::AppendEntriesRequest;
use flexkv::raft::AppendEntriesResponse;
use flexkv::raft::InstallSnapshotRequest;
use flexkv::raft::InstallSnapshotResponse;
use flexkv::raft::VoteRequest;
use flexkv::raft::VoteResponse;
use toy_rpc::macros::export_impl;

use crate::app::App;
use crate::TypeConfig;

pub struct Raft {
    app: Arc<App>,
}

#[export_impl]
impl Raft {
    pub fn new(app: Arc<App>) -> Self {
        Self { app }
    }

    #[export_method]
    pub async fn vote(&self, vote: VoteRequest<u64>) -> Result<VoteResponse<u64>, toy_rpc::Error> {
        self.app.raft.vote(vote).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn append(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<u64>, toy_rpc::Error> {
        self.app.raft.append_entries(req).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }

    #[export_method]
    pub async fn snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<u64>, toy_rpc::Error> {
        self.app.raft.install_snapshot(req).await.map_err(|e| toy_rpc::Error::Internal(Box::new(e)))
    }
}
