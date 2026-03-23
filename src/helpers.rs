use std::{ffi::CString, io, os::unix::ffi::OsStrExt, path::Path};

/// Convert a path to a CString for use with io_uring operations.
pub(crate) fn path_to_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains null byte at position {}", e.nul_position()),
        )
    })
}

pub(crate) fn syscall_cvt<R: Into<i64> + Copy>(result: R) -> io::Result<R> {
    if result.into() == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

// Several `read_to_string` and `read_line` methods in the standard library will
// append data into a `String` buffer, but we need to be pretty careful when
// doing this. The implementation will just call `.as_mut_vec()` and then
// delegate to a byte-oriented reading method, but we must ensure that when
// returning we never leave `buf` in a state such that it contains invalid UTF-8
// in its bounds.
//
// To this end, we use an RAII guard (to protect against panics) which updates
// the length of the string when it is dropped. This guard initially truncates
// the string to the prior length and only after we've validated that the
// new contents are valid UTF-8 do we allow it to set a longer length.
//
// The unsafety in this function is twofold:
//
// 1. We're looking at the raw bytes of `buf`, so we take on the burden of UTF-8
//    checks.
// 2. We're passing a raw buffer to the function `f`, and it is expected that
//    the function only *appends* bytes to the buffer. We'll get undefined
//    behavior if existing bytes are overwritten to have non-UTF-8 data.
pub(crate) async unsafe fn append_to_string<F>(buf: &mut String, f: F) -> io::Result<usize>
where
    F: AsyncFnOnce(&mut Vec<u8>) -> io::Result<usize>,
{
    struct Guard<'a> {
        buf: &'a mut Vec<u8>,
        len: usize,
    }

    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            unsafe {
                self.buf.set_len(self.len);
            }
        }
    }

    let mut g = Guard {
        len: buf.len(),
        buf: unsafe { buf.as_mut_vec() },
    };
    let ret = f(g.buf).await;

    // SAFETY: the caller promises to only append data to `buf`
    let appended = unsafe { g.buf.get_unchecked(g.len..) };
    if str::from_utf8(appended).is_err() {
        ret.map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "stream did not contain valid UTF-8",
            )
        })
    } else {
        g.len = g.buf.len();
        ret
    }
}
