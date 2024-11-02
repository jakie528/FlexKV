use crate::async_runtime::AsyncOneshotSendExt;
use crate::raft::message::ClientWriteResult;
use crate::raft::responder::Responder;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::TypeConfigExt;
use crate::RaftTypeConfig;

pub struct OneshotResponder<C>
where C: RaftTypeConfig
{
    tx: OneshotSenderOf<C, ClientWriteResult<C>>,
}

impl<C> OneshotResponder<C>
where C: RaftTypeConfig
{
    pub fn new(tx: OneshotSenderOf<C, ClientWriteResult<C>>) -> Self {
        Self { tx }
    }
}

impl<C> Responder<C> for OneshotResponder<C>
where C: RaftTypeConfig
{
    type Receiver = OneshotReceiverOf<C, ClientWriteResult<C>>;

    fn from_app_data(app_data: C::D) -> (C::D, Self, Self::Receiver)
    where Self: Sized {
        let (tx, rx) = C::oneshot();
        (app_data, Self { tx }, rx)
    }

    fn send(self, res: ClientWriteResult<C>) {
        let res = self.tx.send(res);

        if !res.is_ok() {
            tracing::warn!("OneshotConsumer.tx.send: is_ok: {}", res.is_ok());
        }
    }
}
