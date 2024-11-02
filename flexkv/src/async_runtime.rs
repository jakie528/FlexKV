use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use crate::Instant;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::TokioInstant;

pub trait AsyncRuntime: Debug + Default + PartialEq + Eq + OptionalSend + OptionalSync + 'static {
    type JoinError: Debug + Display + OptionalSend;

    type JoinHandle<T: OptionalSend + 'static>: Future<Output = Result<T, Self::JoinError>>
        + OptionalSend
        + OptionalSync
        + Unpin;

    type Sleep: Future<Output = ()> + OptionalSend + OptionalSync;

    type Instant: Instant;

    type TimeoutError: Debug + Display + OptionalSend;

    type Timeout<R, T: Future<Output = R> + OptionalSend>: Future<Output = Result<R, Self::TimeoutError>> + OptionalSend;

    type ThreadLocalRng: rand::Rng;

    type OneshotSender<T: OptionalSend>: AsyncOneshotSendExt<T> + OptionalSend + OptionalSync + Debug + Sized;

    type OneshotReceiverError: std::error::Error + OptionalSend;

    type OneshotReceiver<T: OptionalSend>: OptionalSend
        + OptionalSync
        + Future<Output = Result<T, Self::OneshotReceiverError>>
        + Unpin;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static;

    fn sleep(duration: Duration) -> Self::Sleep;

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep;

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F>;

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F>;

    fn is_panic(join_error: &Self::JoinError) -> bool;

    fn thread_rng() -> Self::ThreadLocalRng;

    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where T: OptionalSend;
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TokioRuntime;

pub struct TokioOneShotSender<T: OptionalSend>(pub tokio::sync::oneshot::Sender<T>);

impl AsyncRuntime for TokioRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = TokioInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = tokio::time::Timeout<T>;
    type ThreadLocalRng = rand::rngs::ThreadRng;
    type OneshotSender<T: OptionalSend> = TokioOneShotSender<T>;
    type OneshotReceiver<T: OptionalSend> = tokio::sync::oneshot::Receiver<T>;
    type OneshotReceiverError = tokio::sync::oneshot::error::RecvError;

    #[inline]
    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
            tokio::task::spawn(future)
    }

    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    #[inline]
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(deadline)
    }

    #[inline]
    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout(duration, future)
    }

    #[inline]
    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout_at(deadline, future)
    }

    #[inline]
    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    #[inline]
    fn thread_rng() -> Self::ThreadLocalRng {
        rand::thread_rng()
    }

    #[inline]
    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where T: OptionalSend {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (TokioOneShotSender(tx), rx)
    }
}

pub trait AsyncOneshotSendExt<T> {
    fn send(self, t: T) -> Result<(), T>;
}

impl<T: OptionalSend> AsyncOneshotSendExt<T> for TokioOneShotSender<T> {
    #[inline]
    fn send(self, t: T) -> Result<(), T> {
        self.0.send(t)
    }
}

impl<T: OptionalSend> Debug for TokioOneShotSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TokioSendWrapper").finish()
    }
}
