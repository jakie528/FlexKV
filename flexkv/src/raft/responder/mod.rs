
pub(crate) mod impls;
pub use impls::OneshotResponder;

use crate::raft::message::ClientWriteResult;
use crate::OptionalSend;
use crate::RaftTypeConfig;

pub trait Responder<C>: OptionalSend + 'static
where C: RaftTypeConfig
{
    type Receiver;

    fn from_app_data(app_data: C::D) -> (C::D, Self, Self::Receiver)
    where Self: Sized;

    fn send(self, result: ClientWriteResult<C>);
}
