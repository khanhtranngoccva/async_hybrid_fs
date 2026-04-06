// We define custom buffer traits rather than using `Vec<u8>` or `Box<[u8]>` directly because:
//
// 1. **Aligned allocations**: O_DIRECT requires sector-aligned buffers (typically 512 or 4096 bytes).
//    `Vec<u8>` only guarantees pointer alignment, not allocation alignment. Custom allocators
//    can provide properly aligned buffers that implement these traits.
//
// 2. **Buffer pools**: High-performance applications reuse buffers to avoid allocation overhead.
//    Pool-managed buffers can implement these traits directly without conversion.
//
// 3. **Specialized memory**: GPU memory, mmap'd regions, or other exotic buffer types can
//    participate in io_uring operations by implementing these traits.
//
// 4. **Zero-copy**: Accepting generic buffers avoids the need to copy data into/out of
//    a library-owned buffer type.
//
// The traits are `unsafe` because implementors must guarantee pointer stability across moves,
// which is automatically true for heap allocations but NOT for stack arrays.

use std::{
    io::{IoSlice, IoSliceMut},
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};

/// A buffer that can be used for io_uring write operations.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The pointer returned by `as_ptr()` remains valid and at a stable address until the I/O operation completes, even if `self` is moved.
/// - This is automatically satisfied for heap-allocated buffers (`Vec<u8>`, `Box<[u8]>`, etc.) but NOT for stack-allocated arrays.
pub unsafe trait IoBuf: Send {
    /// Returns a pointer to the buffer's data.
    fn as_ptr(&self) -> *const u8;
    /// Returns the number of initialized bytes in the buffer.
    fn len(&self) -> usize;
    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns a slice of the buffer with its original lifetime.
    fn as_slice<'a>(&'a self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

/// A buffer that can be used for io_uring read operations.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The pointer returned by `as_mut_ptr()` remains valid and at a stable address until the I/O operation completes, even if `self` is moved.
/// - This is automatically satisfied for heap-allocated buffers (`Vec<u8>`, `Box<[u8]>`, etc.) but NOT for stack-allocated arrays.
pub unsafe trait IoBufMut: Send {
    /// Returns a mutable pointer to the buffer's data.
    fn as_mut_ptr(&mut self) -> *mut u8;
    /// Returns the buffer's total capacity (maximum bytes that can be read into it).
    fn capacity(&self) -> usize;
    /// Returns a slice of the buffer including the uninitialized part. This can be cast back to a raw slice.
    fn as_mut_slice_with_uninit<'a>(&'a mut self) -> &'a mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.as_mut_ptr() as *mut MaybeUninit<u8>,
                self.capacity(),
            )
        }
    }
    /// Sets the length of the buffer. If the reported capacity is more than the length of the original buffer, underlying algorithms will try to fill with bytes past the original length, so this method must be implemented.
    unsafe fn set_len(&mut self, len: usize);
}

// Implementations for Vec<u8>
unsafe impl IoBuf for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        Vec::as_ptr(self)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }
}

unsafe impl IoBufMut for Vec<u8> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    unsafe fn set_len(&mut self, len: usize) {
        unsafe { Vec::set_len(self, len) };
    }
}

// Implementations for Box<[u8]>
unsafe impl IoBuf for Box<[u8]> {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

unsafe impl IoBufMut for Box<[u8]> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        // Box<[u8]> has fixed size, capacity == len
        <[u8]>::len(self)
    }

    unsafe fn set_len(&mut self, _len: usize) {
        // Box<[u8]> has fixed size, capacity == len. Therefore, we do not need to do anything.
    }
}

// Implementations for slices (allowing parity with std-like I/O APIs). These slices satisfy IoBuf and IoBufMut
// because the underlying memory is never moved as long as these references are held
unsafe impl IoBuf for &[u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

unsafe impl IoBufMut for &mut [u8] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        <[u8]>::len(self)
    }

    unsafe fn set_len(&mut self, _len: usize) {
        // &mut [u8] has fixed size, capacity == len. Therefore, we do not need to do anything.
    }
}

unsafe impl<'buf> IoBuf for IoSlice<'buf> {
    fn as_ptr(&self) -> *const u8 {
        self.deref().as_ptr()
    }

    fn len(&self) -> usize {
        self.deref().len()
    }
}

unsafe impl<'buf> IoBufMut for IoSliceMut<'buf> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.deref_mut().as_mut_ptr()
    }

    fn capacity(&self) -> usize {
        self.deref().len()
    }

    unsafe fn set_len(&mut self, _len: usize) {
        // IoSliceMut has fixed size, capacity == len. Therefore, we do not need to do anything.
    }
}
