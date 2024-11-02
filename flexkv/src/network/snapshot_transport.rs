
use std::future::Future;
use std::io::SeekFrom;
use std::time::Duration;

use futures::FutureExt;
use flex_macros::add_async_trait;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

use crate::error::Fatal;
use crate::error::InstallSnapshotError;
use crate::error::RPCError;
use crate::error::RaftError;
use crate::error::ReplicationClosed;
use crate::error::StreamingError;
use crate::network::RPCOption;
use crate::raft::InstallSnapshotRequest;
use crate::raft::SnapshotResponse;
use crate::type_config::TypeConfigExt;
use crate::ErrorSubject;
use crate::ErrorVerb;
use crate::OptionalSend;
use crate::Raft;
use crate::RaftNetwork;
use crate::RaftTypeConfig;
use crate::Snapshot;
use crate::SnapshotId;
use crate::StorageError;
use crate::StorageIOError;
use crate::ToStorageResult;
use crate::Vote;

#[add_async_trait]
pub trait SnapshotTransport<C: RaftTypeConfig> {
    async fn send_snapshot<Net>(
        net: &mut Net,
        vote: Vote<C::NodeId>,
        snapshot: Snapshot<C>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C::NodeId>, StreamingError<C, Fatal<C::NodeId>>>
    where
        Net: RaftNetwork<C> + ?Sized;

    async fn receive_snapshot(
        streaming: &mut Option<Streaming<C>>,
        raft: &Raft<C>,
        req: InstallSnapshotRequest<C>,
    ) -> Result<Option<Snapshot<C>>, RaftError<C::NodeId, InstallSnapshotError>>;
}

pub struct Chunked {}

impl<C: RaftTypeConfig> SnapshotTransport<C> for Chunked
where C::SnapshotData: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin
{
    async fn send_snapshot<Net>(
        net: &mut Net,
        vote: Vote<C::NodeId>,
        mut snapshot: Snapshot<C>,
        mut cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C::NodeId>, StreamingError<C, Fatal<C::NodeId>>>
    where
        Net: RaftNetwork<C> + ?Sized,
    {
        let subject_verb = || (ErrorSubject::Snapshot(Some(snapshot.meta.signature())), ErrorVerb::Read);

        let mut offset = 0;
        let end = snapshot.snapshot.seek(SeekFrom::End(0)).await.sto_res(subject_verb)?;

        let mut c = std::pin::pin!(cancel);
        loop {
            if let Some(err) = c.as_mut().now_or_never() {
                return Err(err.into());
            }

            C::sleep(Duration::from_millis(1)).await;

            snapshot.snapshot.seek(SeekFrom::Start(offset)).await.sto_res(subject_verb)?;

            let chunk_size = option.snapshot_chunk_size().unwrap();
            let mut buf = Vec::with_capacity(chunk_size);
            while buf.capacity() > buf.len() {
                let n = snapshot.snapshot.read_buf(&mut buf).await.sto_res(subject_verb)?;
                if n == 0 {
                    break;
                }
            }

            let n_read = buf.len();

            let done = (offset + n_read as u64) == end;
            let req = InstallSnapshotRequest {
                vote: vote.clone(),
                meta: snapshot.meta.clone(),
                offset,
                data: buf,
                done,
            };


            #[allow(deprecated)]
            let res = C::timeout(option.hard_ttl(), net.install_snapshot(req, option.clone())).await;

            let resp = match res {
                Ok(outer_res) => match outer_res {
                    Ok(res) => res,
                    Err(err) => {
                        let err: RPCError<C::NodeId, C::Node, RaftError<C::NodeId, InstallSnapshotError>> = err;

                        tracing::warn!(error=%err, "error sending InstallSnapshot RPC to target");

                        match err {
                            RPCError::Timeout(_) => {}
                            RPCError::Unreachable(_) => {}
                            RPCError::PayloadTooLarge(_) => {}
                            RPCError::Network(_) => {}
                            RPCError::RemoteError(remote_err) => {
                                match remote_err.source {
                                    RaftError::Fatal(_) => {}
                                    RaftError::APIError(snapshot_err) => {
                                        match snapshot_err {
                                            InstallSnapshotError::SnapshotMismatch(mismatch) => {
                                                tracing::warn!(
                                                    mismatch = display(&mismatch),
                                                    "snapshot mismatch, reset offset and retry"
                                                );
                                                offset = 0;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        continue;
                    }
                },
                Err(err) => {
                    tracing::warn!(error=%err, "timeout while sending InstallSnapshot RPC to target");
                    continue;
                }
            };

            if resp.vote > vote {
                return Ok(SnapshotResponse::new(resp.vote));
            }

            if done {
                return Ok(SnapshotResponse::new(resp.vote));
            }

            offset += n_read as u64;
        }
    }

    async fn receive_snapshot(
        streaming: &mut Option<Streaming<C>>,
        raft: &Raft<C>,
        req: InstallSnapshotRequest<C>,
    ) -> Result<Option<Snapshot<C>>, RaftError<C::NodeId, InstallSnapshotError>> {
        let snapshot_id = &req.meta.snapshot_id;
        let snapshot_meta = req.meta.clone();
        let done = req.done;

        tracing::info!(req = display(&req), "{}", func_name!());

        let curr_id = streaming.as_ref().map(|s| s.snapshot_id());

        if curr_id != Some(snapshot_id) {
            if req.offset != 0 {
                let mismatch = InstallSnapshotError::SnapshotMismatch(crate::error::SnapshotMismatch {
                    expect: crate::SnapshotSegmentId {
                        id: snapshot_id.clone(),
                        offset: 0,
                    },
                    got: crate::SnapshotSegmentId {
                        id: snapshot_id.clone(),
                        offset: req.offset,
                    },
                });
                return Err(RaftError::APIError(mismatch));
            }

            let snapshot_data = raft.begin_receiving_snapshot().await.map_err(|e| {
                RaftError::Fatal(e.into_fatal().unwrap())
            })?;

            *streaming = Some(Streaming::new(snapshot_id.clone(), snapshot_data));
        }

        {
            let s = streaming.as_mut().unwrap();
            s.receive(req).await?;
        }

        tracing::info!("Done received snapshot chunk");

        if done {
            let streaming = streaming.take().unwrap();
            let mut data = streaming.into_snapshot_data();

            data.as_mut().shutdown().await.map_err(|e| {
                let io_err = StorageIOError::write_snapshot(Some(snapshot_meta.signature()), &e);
                StorageError::from(io_err)
            })?;

            tracing::info!("finished streaming snapshot: {:?}", snapshot_meta);
            return Ok(Some(Snapshot::new(snapshot_meta, data)));
        }

        Ok(None)
    }
}

pub struct Streaming<C>
where C: RaftTypeConfig
{
    offset: u64,

    snapshot_id: SnapshotId,

    snapshot_data: Box<C::SnapshotData>,
}

impl<C> Streaming<C>
where C: RaftTypeConfig
{
    pub fn new(snapshot_id: SnapshotId, snapshot_data: Box<C::SnapshotData>) -> Self {
        Self {
            offset: 0,
            snapshot_id,
            snapshot_data,
        }
    }

    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    pub fn into_snapshot_data(self) -> Box<C::SnapshotData> {
        self.snapshot_data
    }
}

impl<C> Streaming<C>
where
    C: RaftTypeConfig,
    C::SnapshotData: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin,
{
    pub async fn receive(&mut self, req: InstallSnapshotRequest<C>) -> Result<bool, StorageError<C::NodeId>> {

        if req.offset != self.offset {
            if let Err(err) = self.snapshot_data.as_mut().seek(SeekFrom::Start(req.offset)).await {
                return Err(StorageError::from_io_error(
                    ErrorSubject::Snapshot(Some(req.meta.signature())),
                    ErrorVerb::Seek,
                    err,
                ));
            }
            self.offset = req.offset;
        }

        let res = self.snapshot_data.as_mut().write_all(&req.data).await;
        if let Err(err) = res {
            return Err(StorageError::from_io_error(
                ErrorSubject::Snapshot(Some(req.meta.signature())),
                ErrorVerb::Write,
                err,
            ));
        }
        self.offset += req.data.len() as u64;
        Ok(req.done)
    }
}
