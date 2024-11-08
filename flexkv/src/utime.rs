use core::fmt;
use std::ops::Deref;
use std::ops::DerefMut;

use crate::display_ext::DisplayInstantExt;
use crate::Instant;

#[derive(Debug)]
pub(crate) struct UTime<T, I: Instant> {
    data: T,
    utime: Option<I>,
}

impl<T: fmt::Display, I: Instant> fmt::Display for UTime<T, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.utime {
            Some(utime) => write!(f, "{}@{}", self.data, utime.display()),
            None => write!(f, "{}", self.data),
        }
    }
}

impl<T: Clone, I: Instant> Clone for UTime<T, I> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            utime: self.utime,
        }
    }
}

impl<T: Default, I: Instant> Default for UTime<T, I> {
    fn default() -> Self {
        Self {
            data: T::default(),
            utime: None,
        }
    }
}

impl<T: PartialEq, I: Instant> PartialEq for UTime<T, I> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data && self.utime == other.utime
    }
}

impl<T: PartialEq + Eq, I: Instant> Eq for UTime<T, I> {}

impl<T, I: Instant> Deref for UTime<T, I> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T, I: Instant> DerefMut for UTime<T, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T, I: Instant> UTime<T, I> {
    pub(crate) fn new(now: I, data: T) -> Self {
        Self { data, utime: Some(now) }
    }

    #[allow(dead_code)]
    pub(crate) fn without_utime(data: T) -> Self {
        Self { data, utime: None }
    }

    pub(crate) fn utime(&self) -> Option<I> {
        self.utime
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> T {
        self.data
    }

    pub(crate) fn update(&mut self, now: I, data: T) {
        self.data = data;
        self.utime = Some(now);
    }

    pub(crate) fn touch(&mut self, now: I) {
        self.utime = Some(now);
    }
}
