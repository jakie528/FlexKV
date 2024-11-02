#![allow(clippy::bool_assert_comparison, clippy::type_complexity, clippy::result_large_err)]
#![deny(unused_qualifications)]

macro_rules! func_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let n = &name[..name.len() - 3];
        let nn = n.replace("::{{closure}}", "");
        nn
    }};
}

pub extern crate flex_macros;

mod change_members;
mod config;
mod core;
mod defensive;
mod display_ext;
mod membership;
mod node;
mod progress;
mod quorum;
mod raft_types;
mod replication;
mod runtime;
mod storage_error;
mod summary;
mod try_as_ref;
mod vote;

pub(crate) mod engine;
pub(crate) mod log_id_range;
pub(crate) mod proposer;
pub(crate) mod raft_state;
pub(crate) mod timer;
pub(crate) mod utime;

pub mod async_runtime;
pub mod entry;
pub mod error;
pub mod impls;
pub mod instant;
pub mod log_id;
pub mod metrics;
pub mod network;
pub mod raft;
pub mod storage;
pub mod type_config;

pub use anyerror;
pub use anyerror::AnyError;
pub use flex_macros::add_async_trait;

pub use crate::async_runtime::AsyncRuntime;
pub use crate::async_runtime::TokioRuntime;
pub use crate::change_members::ChangeMembers;
pub use crate::config::Config;
pub use crate::config::ConfigError;
pub use crate::config::SnapshotPolicy;
pub use crate::core::ServerState;
pub use crate::entry::Entry;
pub use crate::entry::EntryPayload;
pub use crate::instant::Instant;
pub use crate::instant::TokioInstant;
pub use crate::log_id::LogId;
pub use crate::log_id::LogIdOptionExt;
pub use crate::log_id::LogIndexOptionExt;
pub use crate::log_id::RaftLogId;
pub use crate::membership::EffectiveMembership;
pub use crate::membership::Membership;
pub use crate::membership::StoredMembership;
pub use crate::metrics::RaftMetrics;
pub use crate::network::RPCTypes;
pub use crate::network::RaftNetwork;
pub use crate::network::RaftNetworkFactory;
pub use crate::node::BasicNode;
pub use crate::node::EmptyNode;
pub use crate::node::Node;
pub use crate::node::NodeId;
pub use crate::raft::Raft;
pub use crate::raft_state::MembershipState;
pub use crate::raft_state::RaftState;
pub use crate::raft_types::SnapshotId;
pub use crate::raft_types::SnapshotSegmentId;
pub use crate::storage::LogState;
pub use crate::storage::RaftLogReader;
pub use crate::storage::RaftSnapshotBuilder;
pub use crate::storage::Snapshot;
pub use crate::storage::SnapshotMeta;
pub use crate::storage::StorageHelper;
pub use crate::storage_error::DefensiveError;
pub use crate::storage_error::ErrorSubject;
pub use crate::storage_error::ErrorVerb;
pub use crate::storage_error::StorageError;
pub use crate::storage_error::StorageIOError;
pub use crate::storage_error::ToStorageResult;
pub use crate::storage_error::Violation;
pub use crate::summary::MessageSummary;
pub use crate::try_as_ref::TryAsRef;
pub use crate::type_config::RaftTypeConfig;
pub use crate::vote::CommittedLeaderId;
pub use crate::vote::LeaderId;
pub use crate::vote::Vote;

pub trait OptionalSerde: serde::Serialize + for<'a> serde::Deserialize<'a> {}

impl<T> OptionalSerde for T where T: serde::Serialize + for<'a> serde::Deserialize<'a> {}

pub trait OptionalSend: Send {}

pub trait OptionalSync: Sync {}

impl<T: Send + ?Sized> OptionalSend for T {}

impl<T: Sync + ?Sized> OptionalSync for T {}

pub trait AppData: OptionalSend + OptionalSync + 'static + OptionalSerde {}

impl<T> AppData for T where T: OptionalSend + OptionalSync + 'static + OptionalSerde {}

pub trait AppDataResponse: OptionalSend + OptionalSync + 'static + OptionalSerde {}

impl<T> AppDataResponse for T where T: OptionalSend + OptionalSync + 'static + OptionalSerde {}
