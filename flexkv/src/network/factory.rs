use flex_macros::add_async_trait;

use crate::network::RaftNetwork;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;

#[add_async_trait]
pub trait RaftNetworkFactory<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    type Network: RaftNetwork<C>;

    async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network;
}
