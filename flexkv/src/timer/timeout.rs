use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::select;
use futures::future::Either;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tracing::trace_span;
use tracing::Instrument;

use crate::AsyncRuntime;
use crate::Instant;
use crate::OptionalSend;

#[allow(dead_code)]
pub(crate) trait RaftTimer<RT: AsyncRuntime> {
    fn new<F: FnOnce() + OptionalSend + 'static>(callback: F, timeout: Duration) -> Self;

    fn update_timeout(&self, timeout: Duration);
}

#[allow(dead_code)]
pub(crate) struct Timeout<RT: AsyncRuntime> {
    #[allow(dead_code)]
    tx: Sender<()>,

    inner: Arc<TimeoutInner<RT>>,
}

#[allow(dead_code)]
pub(crate) struct TimeoutInner<RT: AsyncRuntime> {
    init: RT::Instant,

    relative_deadline: AtomicU64,
}

impl<RT: AsyncRuntime> RaftTimer<RT> for Timeout<RT> {
    fn new<F: FnOnce() + OptionalSend + 'static>(callback: F, timeout: Duration) -> Self {
        let (tx, rx) = oneshot::channel();

        let inner = TimeoutInner {
            init: RT::Instant::now(),
            relative_deadline: AtomicU64::new(timeout.as_micros() as u64),
        };

        let inner = Arc::new(inner);

        let t = Timeout {
            tx,
            inner: inner.clone(),
        };

        #[allow(clippy::let_underscore_future)]
        let _ = RT::spawn(inner.sleep_loop(rx, callback).instrument(trace_span!("timeout-loop").or_current()));

        t
    }

    fn update_timeout(&self, timeout: Duration) {
        let since_init = RT::Instant::now() + timeout - self.inner.init;

        let new_at = since_init.as_micros() as u64;

        self.inner.relative_deadline.fetch_max(new_at, Ordering::Relaxed);
    }
}

impl<RT: AsyncRuntime> TimeoutInner<RT> {
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sleep_loop<F: FnOnce() + OptionalSend + 'static>(
        self: Arc<Self>,
        rx: Receiver<()>,
        callback: F,
    ) {
        let mut wake_up_at = None;

        let mut rx = rx;
        loop {
            let curr_deadline = self.relative_deadline.load(Ordering::Relaxed);

            if wake_up_at == Some(curr_deadline) {
                callback();
                return;
            }


            wake_up_at = Some(curr_deadline);

            let deadline = self.init + Duration::from_micros(curr_deadline);

            let either = select(Box::pin(RT::sleep_until(deadline)), rx).await;
            rx = match either {
                Either::Left((_sleep_res, rx)) => {
                    rx
                }
                Either::Right((_sleep_fut, _rx_res)) => {
                    return;
                }
            };
        }
    }
}
