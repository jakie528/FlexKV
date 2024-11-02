use std::fmt::Debug;
use std::fmt::Display;

use crate::log_id::RaftLogId;
use crate::LogId;
use crate::Membership;
use crate::Node;
use crate::NodeId;
use crate::OptionalSend;
use crate::OptionalSerde;
use crate::OptionalSync;

pub trait RaftPayload<NID, N>
where
    N: Node,
    NID: NodeId,
{
    fn is_blank(&self) -> bool;

    fn get_membership(&self) -> Option<&Membership<NID, N>>;
}

pub trait RaftEntry<NID, N>: RaftPayload<NID, N> + RaftLogId<NID>
where
    N: Node,
    NID: NodeId,
    Self: OptionalSerde + Debug + Display + OptionalSend + OptionalSync,
{
    fn new_blank(log_id: LogId<NID>) -> Self;

    fn new_membership(log_id: LogId<NID>, m: Membership<NID, N>) -> Self;
}

pub trait FromAppData<T> {
    fn from_app_data(t: T) -> Self;
}
