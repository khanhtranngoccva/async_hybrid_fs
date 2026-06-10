#![allow(dead_code)]
#![allow(unused_macros)]

use core::ffi::c_void;
use core::mem::{align_of, size_of};
use core::ptr::{NonNull, null, null_mut};
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

/// Convert a syscall result to an `io::Result`
pub(crate) fn syscall_cvt<R: Into<i64> + Copy>(result: R) -> io::Result<R> {
    if result.into() == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

/// Retry the given function if it returns an `Interrupted` error
pub(crate) fn retry_on_eintr<T, F>(mut f: F) -> io::Result<T>
where
    F: FnMut() -> io::Result<T>,
{
    loop {
        match f() {
            Ok(res) => return Ok(res),
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Retry the given async function if it returns an `Interrupted` error
macro_rules! m_retry_on_eintr {
    ($f:expr) => {{
        loop {
            match $f {
                Ok(res) => break Ok(res),
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => break Err(e),
            }
        }
    }};
}
pub(crate) use m_retry_on_eintr;

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

/// Convert a `&T` into a `*const T` without using an `as`.
#[inline]
pub(crate) const fn as_ptr<T>(t: &T) -> *const T {
    t
}

/// Convert a `&mut T` into a `*mut T` without using an `as`.
#[inline]
pub(crate) fn as_mut_ptr<T>(t: &mut T) -> *mut T {
    t
}

/// Convert an `Option<&T>` into a possibly-null `*const T`.
#[inline]
pub(crate) const fn option_as_ptr<T>(t: Option<&T>) -> *const T {
    match t {
        Some(t) => t,
        None => null(),
    }
}

/// Convert an `Option<&mut T>` into a possibly-null `*mut T`.
#[inline]
pub(crate) fn option_as_mut_ptr<T>(t: Option<&mut T>) -> *mut T {
    match t {
        Some(t) => t,
        None => null_mut(),
    }
}

/// Convert a `*mut c_void` to a `*mut T`, checking that it is not null,
/// misaligned, or pointing to a region of memory that wraps around the address
/// space.
pub(crate) fn check_raw_pointer<T>(value: *mut c_void) -> Option<NonNull<T>> {
    if (value as usize).checked_add(size_of::<T>()).is_none()
        || !(value as usize).is_multiple_of(align_of::<T>())
    {
        return None;
    }

    NonNull::new(value.cast())
}

/// Create a union value containing a default value in one of its arms.
///
/// The field names a union field which must have the same size as the union
/// itself.
macro_rules! default_union {
    ($union:ident, $field:ident) => {{
        let u = $union {
            $field: Default::default(),
        };

        // Assert that the given field initializes the whole union.
        #[cfg(test)]
        unsafe {
            let field_value = u.$field;
            assert_eq!(
                core::mem::size_of_val(&u),
                core::mem::size_of_val(&field_value)
            );
            const_assert_eq!(memoffset::offset_of_union!($union, $field), 0);
        }

        u
    }};
}
