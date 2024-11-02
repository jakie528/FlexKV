use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;

use crate::OptionalSend;
use crate::OptionalSync;

#[doc(hidden)]
pub trait NodeIdEssential:
    Sized
    + OptionalSend
    + OptionalSync
    + Eq
    + PartialEq
    + Ord
    + PartialOrd
    + Debug
    + Display
    + Hash
    + Clone
    + Default
    + 'static
{
}

impl<T> NodeIdEssential for T where T: Sized
        + OptionalSend
        + OptionalSync
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + Debug
        + Display
        + Hash
        + Copy
        + Clone
        + Default
        + 'static
{
}

pub trait NodeId: NodeIdEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

impl<T> NodeId for T where T: NodeIdEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

pub trait NodeEssential:
    Sized + OptionalSend + OptionalSync + Eq + PartialEq + Debug + Clone + Default + 'static
{
}
impl<T> NodeEssential for T where T: Sized + OptionalSend + OptionalSync + Eq + PartialEq + Debug + Clone + Default + 'static
{}

pub trait Node: NodeEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

impl<T> Node for T where T: NodeEssential + serde::Serialize + for<'a> serde::Deserialize<'a> {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct EmptyNode {}

impl EmptyNode {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for EmptyNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{}}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct BasicNode {
    pub addr: String,
}

impl BasicNode {
    pub fn new(addr: impl ToString) -> Self {
        Self { addr: addr.to_string() }
    }
}

impl Display for BasicNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}
