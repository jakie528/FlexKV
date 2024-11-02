
use maplit::btreemap;

use crate::core::raft_msg::RaftMsg;
use crate::error::ClientWriteError;
use crate::error::RaftError;
use crate::raft::message::ClientWriteResult;
use crate::raft::responder::OneshotResponder;
use crate::raft::ClientWriteResponse;
use crate::summary::MessageSummary;
use crate::type_config::alias::OneshotReceiverOf;
use crate::AsyncRuntime;
use crate::ChangeMembers;
use crate::Raft;
use crate::RaftTypeConfig;

impl<C> Raft<C>
where C: RaftTypeConfig<Responder = OneshotResponder<C>>
{
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<C::NodeId, C::Node>>,
        retain: bool,
    ) -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>> {
        let changes: ChangeMembers<C::NodeId, C::Node> = members.into();

        tracing::info!(
            changes = debug(&changes),
            retain = display(retain),
            "change_membership: start to commit joint config"
        );

        let (tx, rx) = oneshot_channel::<C>();

        let res = self
            .inner
            .call_core(
                RaftMsg::ChangeMembership {
                    changes: changes.clone(),
                    retain,
                    tx,
                },
                rx,
            )
            .await;

        if let Err(e) = &res {
            tracing::error!("the first step error: {}", e);
        }
        let res = res?;

        let joint = res.membership.clone().unwrap();

        if joint.get_joint_config().len() == 1 {
            return Ok(res);
        }

        let (tx, rx) = oneshot_channel::<C>();

        let res = self.inner.call_core(RaftMsg::ChangeMembership { changes, retain, tx }, rx).await;

        if let Err(e) = &res {
            tracing::error!("the second step error: {}", e);
        }
        let res = res?;

        tracing::info!("res of second step of do_change_membership: {}", res.summary());

        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self, id), fields(target=display(&id)))]
    pub async fn add_learner(
        &self,
        id: C::NodeId,
        node: C::Node,
        blocking: bool,
    ) -> Result<ClientWriteResponse<C>, RaftError<C::NodeId, ClientWriteError<C::NodeId, C::Node>>> {
        let (tx, rx) = oneshot_channel::<C>();

        let msg = RaftMsg::ChangeMembership {
            changes: ChangeMembers::AddNodes(btreemap! {id.clone()=>node}),
            retain: true,
            tx,
        };

        let resp = self.inner.call_core(msg, rx).await?;

        if !blocking {
            return Ok(resp);
        }

        if self.inner.id == id {
            return Ok(resp);
        }


        let membership_log_id = resp.log_id.clone();

        let wait_res = self
            .wait(None)
            .metrics(
                |metrics| match self.check_replication_upto_date(metrics, id.clone(), Some(membership_log_id.clone())) {
                    Ok(_matching) => true,
                    Err(_) => false,
                },
                "wait new learner to become line-rate",
            )
            .await;

        tracing::info!(wait_res = debug(&wait_res), "waiting for replication to new learner");

        Ok(resp)
    }
}

fn oneshot_channel<C>() -> (OneshotResponder<C>, OneshotReceiverOf<C, ClientWriteResult<C>>)
where C: RaftTypeConfig {
    let (tx, rx) = C::AsyncRuntime::oneshot();

    let tx = OneshotResponder::new(tx);

    (tx, rx)
}
