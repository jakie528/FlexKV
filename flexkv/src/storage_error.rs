use std::fmt;
use std::ops::Bound;

use anyerror::AnyError;

use crate::storage::SnapshotSignature;
use crate::LogId;
use crate::NodeId;
use crate::Vote;

pub trait ToStorageResult<NID, T>
where NID: NodeId
{
    fn sto_res<F>(self, f: F) -> Result<T, StorageError<NID>>
    where F: FnOnce() -> (ErrorSubject<NID>, ErrorVerb);
}

impl<NID, T> ToStorageResult<NID, T> for Result<T, std::io::Error>
where NID: NodeId
{
    fn sto_res<F>(self, f: F) -> Result<T, StorageError<NID>>
    where F: FnOnce() -> (ErrorSubject<NID>, ErrorVerb) {
        match self {
            Ok(x) => Ok(x),
            Err(e) => {
                let (subject, verb) = f();
                let io_err = StorageIOError::new(subject, verb, AnyError::new(&e));
                Err(io_err.into())
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct DefensiveError<NID>
where NID: NodeId
{
    pub subject: ErrorSubject<NID>,

    pub violation: Violation<NID>,

    pub backtrace: Option<String>,
}

impl<NID> DefensiveError<NID>
where NID: NodeId
{
    pub fn new(subject: ErrorSubject<NID>, violation: Violation<NID>) -> Self {
        Self {
            subject,
            violation,
            backtrace: anyerror::backtrace_str(),
        }
    }
}

impl<NID> fmt::Display for DefensiveError<NID>
where NID: NodeId
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{:?}' violates: '{}'", self.subject, self.violation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum ErrorSubject<NID>
where NID: NodeId
{
    Store,

    Vote,

    Logs,

    Log(LogId<NID>),

    LogIndex(u64),

    Apply(LogId<NID>),

    StateMachine,

    Snapshot(Option<SnapshotSignature<NID>>),

    None,
}

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum ErrorVerb {
    Read,
    Write,
    Seek,
    Delete,
}

impl fmt::Display for ErrorVerb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum Violation<NID: NodeId> {
    #[error("term can only be change to a greater value, current: {curr}, change to {to}")]
    VoteNotAscending { curr: Vote<NID>, to: Vote<NID> },

    #[error("voted_for can not change from Some() to other Some(), current: {curr:?}, change to {to:?}")]
    NonIncrementalVote { curr: Vote<NID>, to: Vote<NID> },

    #[error("log at higher index is obsolete: {higher_index_log_id:?} should GT {lower_index_log_id:?}")]
    DirtyLog {
        higher_index_log_id: LogId<NID>,
        lower_index_log_id: LogId<NID>,
    },

    #[error("try to get log at index {want} but got {got:?}")]
    LogIndexNotFound { want: u64, got: Option<u64> },

    #[error("range is empty: start: {start:?}, end: {end:?}")]
    RangeEmpty { start: Option<u64>, end: Option<u64> },

    #[error("range is not half-open: start: {start:?}, end: {end:?}")]
    RangeNotHalfOpen { start: Bound<u64>, end: Bound<u64> },

    #[error("empty log vector")]
    LogsEmpty,

    #[error("all logs are removed. It requires at least one log to track continuity")]
    StoreLogsEmpty,

    #[error("logs are not consecutive, prev: {prev:?}, next: {next}")]
    LogsNonConsecutive { prev: Option<LogId<NID>>, next: LogId<NID> },

    #[error("invalid next log to apply: prev: {prev:?}, next: {next}")]
    ApplyNonConsecutive { prev: Option<LogId<NID>>, next: LogId<NID> },

    #[error("applied log can not conflict, last_applied: {last_applied:?}, delete since: {first_conflict_log_id}")]
    AppliedWontConflict {
        last_applied: Option<LogId<NID>>,
        first_conflict_log_id: LogId<NID>,
    },

    #[error("not allowed to purge non-applied logs, last_applied: {last_applied:?}, purge upto: {purge_upto}")]
    PurgeNonApplied {
        last_applied: Option<LogId<NID>>,
        purge_upto: LogId<NID>,
    },
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum StorageError<NID>
where NID: NodeId
{
    #[error(transparent)]
    Defensive {
        #[from]
        source: DefensiveError<NID>,
    },

    #[error(transparent)]
    IO {
        #[from]
        source: StorageIOError<NID>,
    },
}

impl<NID> StorageError<NID>
where NID: NodeId
{
    pub fn into_defensive(self) -> Option<DefensiveError<NID>> {
        match self {
            StorageError::Defensive { source } => Some(source),
            _ => None,
        }
    }

    pub fn into_io(self) -> Option<StorageIOError<NID>> {
        match self {
            StorageError::IO { source } => Some(source),
            _ => None,
        }
    }

    pub fn from_io_error(subject: ErrorSubject<NID>, verb: ErrorVerb, io_error: std::io::Error) -> Self {
        let sto_io_err = StorageIOError::new(subject, verb, AnyError::new(&io_error));
        StorageError::IO { source: sto_io_err }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct StorageIOError<NID>
where NID: NodeId
{
    subject: ErrorSubject<NID>,
    verb: ErrorVerb,
    source: AnyError,
    backtrace: Option<String>,
}

impl<NID> fmt::Display for StorageIOError<NID>
where NID: NodeId
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "when {:?} {:?}: {}", self.verb, self.subject, self.source)
    }
}

impl<NID> StorageIOError<NID>
where NID: NodeId
{
    pub fn new(subject: ErrorSubject<NID>, verb: ErrorVerb, source: impl Into<AnyError>) -> Self {
        Self {
            subject,
            verb,
            source: source.into(),
            backtrace: anyerror::backtrace_str(),
        }
    }

    pub fn write_log_entry(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Log(log_id), ErrorVerb::Write, source)
    }

    pub fn read_log_at_index(log_index: u64, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::LogIndex(log_index), ErrorVerb::Read, source)
    }

    pub fn read_log_entry(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Log(log_id), ErrorVerb::Read, source)
    }

    pub fn write_logs(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Logs, ErrorVerb::Write, source)
    }

    pub fn read_logs(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Logs, ErrorVerb::Read, source)
    }

    pub fn write_vote(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Vote, ErrorVerb::Write, source)
    }

    pub fn read_vote(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Vote, ErrorVerb::Read, source)
    }

    pub fn apply(log_id: LogId<NID>, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Apply(log_id), ErrorVerb::Write, source)
    }

    pub fn write_state_machine(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::StateMachine, ErrorVerb::Write, source)
    }

    pub fn read_state_machine(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::StateMachine, ErrorVerb::Read, source)
    }

    pub fn write_snapshot(signature: Option<SnapshotSignature<NID>>, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Snapshot(signature), ErrorVerb::Write, source)
    }

    pub fn read_snapshot(signature: Option<SnapshotSignature<NID>>, source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Snapshot(signature), ErrorVerb::Read, source)
    }

    pub fn read(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Store, ErrorVerb::Read, source)
    }

    pub fn write(source: impl Into<AnyError>) -> Self {
        Self::new(ErrorSubject::Store, ErrorVerb::Write, source)
    }
}
