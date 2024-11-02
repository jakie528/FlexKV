use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

use validit::Validate;

use crate::display_ext::DisplayOptionExt;
use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;


#[derive(Clone, Copy, Debug)]
#[derive(PartialEq, Eq)]
pub(crate) struct LogIdRange<NID: NodeId> {
    pub(crate) prev: Option<LogId<NID>>,

    pub(crate) last: Option<LogId<NID>>,
}

impl<NID: NodeId> Display for LogIdRange<NID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}]", self.prev.display(), self.last.display())
    }
}

impl<NID: NodeId> Validate for LogIdRange<NID> {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        validit::less_equal!(&self.prev, &self.last);
        Ok(())
    }
}

impl<NID: NodeId> LogIdRange<NID> {
    pub(crate) fn new(prev: Option<LogId<NID>>, last: Option<LogId<NID>>) -> Self {
        Self { prev, last }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> u64 {
        self.last.next_index() - self.prev.next_index()
    }
}
