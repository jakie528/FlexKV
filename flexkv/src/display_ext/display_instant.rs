use std::fmt;

use chrono::DateTime;
use chrono::Local;
use chrono::Utc;

use crate::Instant;

pub(crate) struct DisplayInstant<'a, T, const SIMPLE: bool = true, const LOCAL: bool = true>(pub &'a T);

impl<'a, T, const SIMPLE: bool, const LOCAL: bool> DisplayInstant<'a, T, SIMPLE, LOCAL> {
    #[allow(dead_code)]
    pub fn full(self) -> DisplayInstant<'a, T, false, LOCAL> {
        DisplayInstant(self.0)
    }

    #[allow(dead_code)]
    pub fn simple(self) -> DisplayInstant<'a, T, true, LOCAL> {
        DisplayInstant(self.0)
    }

    #[allow(dead_code)]
    pub fn local(self) -> DisplayInstant<'a, T, SIMPLE, true> {
        DisplayInstant(self.0)
    }

    #[allow(dead_code)]
    pub fn utc(self) -> DisplayInstant<'a, T, SIMPLE, false> {
        DisplayInstant(self.0)
    }
}

impl<'a, T, const SIMPLE: bool, const LOCAL: bool> fmt::Display for DisplayInstant<'a, T, SIMPLE, LOCAL>
where T: Instant
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sys_t = {
            let sys_now = std::time::SystemTime::now();
            let now = T::now();

            if &now >= self.0 {
                let d = now - *self.0;
                sys_now - d
            } else {
                let d = *self.0 - now;
                sys_now + d
            }
        };

        if LOCAL {
            let datetime: DateTime<Local> = sys_t.into();

            if SIMPLE {
                write!(f, "{}", datetime.format("%H:%M:%S%.6f"))
            } else {
                write!(f, "{}", datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ%z"))
            }
        } else {
            let datetime: DateTime<Utc> = sys_t.into();

            if SIMPLE {
                write!(f, "{}", datetime.format("%H:%M:%S%.6f"))
            } else {
                write!(f, "{}", datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ%z"))
            }
        }
    }
}

pub(crate) trait DisplayInstantExt<'a, T> {
    fn display(&'a self) -> DisplayInstant<'a, T, true>;
}

impl<T> DisplayInstantExt<'_, T> for T
where T: Instant
{
    fn display(&self) -> DisplayInstant<T, true> {
        DisplayInstant(self)
    }
}
