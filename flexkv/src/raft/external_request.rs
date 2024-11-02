
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::NodeIdOf;
use crate::type_config::alias::NodeOf;
use crate::OptionalSend;
use crate::RaftState;
use crate::RaftTypeConfig;

pub trait BoxCoreFnInternal<C>: FnOnce(&RaftState<NodeIdOf<C>, NodeOf<C>, InstantOf<C>>) + OptionalSend
where C: RaftTypeConfig
{
}

impl<C: RaftTypeConfig, T: FnOnce(&RaftState<NodeIdOf<C>, NodeOf<C>, InstantOf<C>>) + OptionalSend> BoxCoreFnInternal<C>
    for T
{
}

pub(crate) type BoxCoreFn<C> = Box<dyn BoxCoreFnInternal<C> + 'static>;
