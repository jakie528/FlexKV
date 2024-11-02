use std::future::Future;
use std::time::Duration;

use crate::type_config::alias::AsyncRuntimeOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::SleepOf;
use crate::type_config::alias::TimeoutOf;
use crate::AsyncRuntime;
use crate::Instant;
use crate::OptionalSend;
use crate::RaftTypeConfig;

pub trait TypeConfigExt: RaftTypeConfig {

    fn now() -> InstantOf<Self> {
        InstantOf::<Self>::now()
    }

    fn sleep(duration: Duration) -> SleepOf<Self> {
        AsyncRuntimeOf::<Self>::sleep(duration)
    }

    fn sleep_until(deadline: InstantOf<Self>) -> SleepOf<Self> {
        AsyncRuntimeOf::<Self>::sleep_until(deadline)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> TimeoutOf<Self, R, F> {
        AsyncRuntimeOf::<Self>::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: InstantOf<Self>,
        future: F,
    ) -> TimeoutOf<Self, R, F> {
        AsyncRuntimeOf::<Self>::timeout_at(deadline, future)
    }


    fn oneshot<T>() -> (OneshotSenderOf<Self, T>, OneshotReceiverOf<Self, T>)
    where T: OptionalSend {
        AsyncRuntimeOf::<Self>::oneshot()
    }


    fn spawn<T>(future: T) -> JoinHandleOf<Self, T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        AsyncRuntimeOf::<Self>::spawn(future)
    }
}

impl<T> TypeConfigExt for T where T: RaftTypeConfig {}
