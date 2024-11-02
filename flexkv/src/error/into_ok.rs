use crate::error::Infallible;

pub(crate) trait UnwrapInfallible<T> {
    fn into_ok(self) -> T;
}

impl<T, E> UnwrapInfallible<T> for Result<T, E>
where E: Into<Infallible>
{
    fn into_ok(self) -> T {
        match self {
            Ok(t) => t,
            Err(_) => unreachable!(),
        }
    }
}

pub(crate) fn into_ok<T, E>(result: Result<T, E>) -> T
where E: Into<Infallible> {
    UnwrapInfallible::into_ok(result)
}
