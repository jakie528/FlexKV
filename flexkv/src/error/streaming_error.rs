use std::error::Error;

use crate::error::Fatal;
use crate::error::Infallible;
use crate::error::NetworkError;
use crate::error::RPCError;
use crate::error::RemoteError;
use crate::error::ReplicationClosed;
use crate::error::ReplicationError;
use crate::error::Timeout;
use crate::error::Unreachable;
use crate::RaftTypeConfig;
use crate::StorageError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(serialize = "E: serde::Serialize")),
    serde(bound(deserialize = "E: for <'d> serde::Deserialize<'d>"))
)]
pub enum StreamingError<C: RaftTypeConfig, E: Error = Infallible> {
    #[error(transparent)]
    Closed(#[from] ReplicationClosed),

    #[error(transparent)]
    StorageError(#[from] StorageError<C::NodeId>),

    #[error(transparent)]
    Timeout(#[from] Timeout<C::NodeId>),

    #[error(transparent)]
    Unreachable(#[from] Unreachable),

    #[error(transparent)]
    Network(#[from] NetworkError),

    #[error(transparent)]
    RemoteError(#[from] RemoteError<C::NodeId, C::Node, E>),
}

impl<C: RaftTypeConfig> From<StreamingError<C, Fatal<C::NodeId>>> for ReplicationError<C::NodeId, C::Node> {
    fn from(e: StreamingError<C, Fatal<C::NodeId>>) -> Self {
        match e {
            StreamingError::Closed(e) => ReplicationError::Closed(e),
            StreamingError::StorageError(e) => ReplicationError::StorageError(e),
            StreamingError::Timeout(e) => ReplicationError::RPCError(RPCError::Timeout(e)),
            StreamingError::Unreachable(e) => ReplicationError::RPCError(RPCError::Unreachable(e)),
            StreamingError::Network(e) => ReplicationError::RPCError(RPCError::Network(e)),
            StreamingError::RemoteError(e) => {
                ReplicationError::RPCError(RPCError::Unreachable(Unreachable::new(&e.source)))
            }
        }
    }
}

impl<C: RaftTypeConfig> From<StreamingError<C>> for ReplicationError<C::NodeId, C::Node> {
    fn from(e: StreamingError<C>) -> Self {
        #[allow(unreachable_patterns)]
        match e {
            StreamingError::Closed(e) => ReplicationError::Closed(e),
            StreamingError::StorageError(e) => ReplicationError::StorageError(e),
            StreamingError::Timeout(e) => ReplicationError::RPCError(RPCError::Timeout(e)),
            StreamingError::Unreachable(e) => ReplicationError::RPCError(RPCError::Unreachable(e)),
            StreamingError::Network(e) => ReplicationError::RPCError(RPCError::Network(e)),
            StreamingError::RemoteError(_e) => {
                unreachable!("Infallible error should not be converted to ReplicationError")
            }
        }
    }
}
