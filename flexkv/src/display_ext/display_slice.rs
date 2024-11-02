use std::fmt;

pub(crate) struct DisplaySlice<'a, T: fmt::Display, const MAX: usize = 5>(pub &'a [T]);

impl<'a, T: fmt::Display, const MAX: usize> fmt::Display for DisplaySlice<'a, T, MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slice = self.0;
        let len = slice.len();

        write!(f, "[")?;

        if len > MAX {
            for (i, t) in slice[..(MAX - 1)].iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }

                write!(f, "{}", t)?;
            }

            write!(f, ",..,")?;
            write!(f, "{}", slice.last().unwrap())?;
        } else {
            for (i, t) in slice.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }

                write!(f, "{}", t)?;
            }
        }

        write!(f, "]")
    }
}

#[allow(dead_code)]
pub(crate) trait DisplaySliceExt<'a, T: fmt::Display> {
    fn display(&'a self) -> DisplaySlice<'a, T>;
}

impl<T> DisplaySliceExt<'_, T> for [T]
where T: fmt::Display
{
    fn display(&self) -> DisplaySlice<T> {
        DisplaySlice(self)
    }
}
