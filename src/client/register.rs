use super::Client;
use crate::client::{MAX_REGISTERED_FILES, UringTarget, requests::Target};
use dashmap::DashSet;
use io_uring::IoUring;
use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, IntoRawFd, OwnedFd, RawFd},
    sync::{Arc, atomic::Ordering},
};
use thiserror::Error;

// If the file is not successfully registered, the TemporaryToken will be dropped to release the index.
struct TemporaryToken {
    committed: bool,
    index: u32,
    registered_files: Arc<DashSet<u32>>,
}
impl TemporaryToken {
    fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for TemporaryToken {
    fn drop(&mut self) {
        if !self.committed {
            self.registered_files.remove(&self.index);
        }
    }
}

/// A borrowed file that is registered with the io_uring instance, which enables io_uring optimizations.
/// When the io_uring client exits, the file will no longer work as a fixed target.
pub struct RegisteredFile<'a> {
    index: Option<u32>,
    fd: BorrowedFd<'a>,
    uring_identity: Arc<()>,
    // Structures for upgrading to OwnedRegisteredFile
    _uring: Arc<IoUring>,
    _registered_files: Arc<DashSet<u32>>,
}

impl<'a> RegisteredFile<'a> {
    /// Upgrade the borrowed file to an owned file, which allows the file to be stored independently.
    pub fn try_into_owned(mut self) -> Result<OwnedRegisteredFile, io::Error> {
        let duplicated_fd = self.fd.try_clone_to_owned()?;
        // Reuse the index for the new file descriptor.
        self._uring
            .submitter()
            .register_files_update(self.index.unwrap(), &[duplicated_fd.as_raw_fd()])?;
        // Prevent the index from being dropped by the old file.
        self.index.take();
        Ok(OwnedRegisteredFile {
            index: self.index.unwrap(),
            fd: Some(duplicated_fd),
            uring_identity: self.uring_identity.clone(),
            _uring: self._uring.clone(),
            _registered_files: self._registered_files.clone(),
        })
    }
}

impl<'a> Drop for RegisteredFile<'a> {
    fn drop(&mut self) {
        let _ = self.index.take().map(|idx| {
            let _ = self._uring.submitter().register_files_update(idx, &[-1]);
            self._registered_files.remove(&idx);
        });
    }
}

impl UringTarget for RegisteredFile<'_> {
    fn as_file_descriptor(&self) -> BorrowedFd<'_> {
        self.fd
    }

    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
        if Arc::ptr_eq(&self.uring_identity, _uring_identity) {
            Target::Fixed {
                index: self.index.unwrap(),
                raw_fd: self.fd.as_raw_fd(),
                uring_identity: self.uring_identity.clone(),
            }
        } else {
            // If the RegisteredFile is used with a different Client instance, return the raw file descriptor to allow the file to be used normally,
            // at the cost of foregoing optimizations.
            Target::Fd(self.fd.as_raw_fd())
        }
    }
}

/// An owned file that is registered with the io_uring instance.
/// When the io_uring client exits, the file will no longer work as a fixed target.
pub struct OwnedRegisteredFile {
    index: u32,
    fd: Option<OwnedFd>,
    uring_identity: Arc<()>,
    // Note: it is not possible to include the whole ClientUring struct because it is owned by the outer client and its lifetime is controlled by the client.
    _uring: Arc<IoUring>,
    _registered_files: Arc<DashSet<u32>>,
}

impl OwnedRegisteredFile {
    fn _cleanup(&mut self) -> () {
        let _ = self
            ._uring
            .submitter()
            .register_files_update(self.index, &[-1]);
        self._registered_files.remove(&self.index);
    }
}

impl IntoRawFd for OwnedRegisteredFile {
    fn into_raw_fd(mut self) -> RawFd {
        self._cleanup();
        self.fd
            .take()
            .expect("fd removal can only be called once")
            .into_raw_fd()
    }
}

impl Drop for OwnedRegisteredFile {
    fn drop(&mut self) {
        self._cleanup();
    }
}

impl UringTarget for OwnedRegisteredFile {
    fn as_file_descriptor(&self) -> BorrowedFd<'_> {
        AsFd::as_fd(self.fd.as_ref().unwrap())
    }

    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
        if Arc::ptr_eq(&self.uring_identity, _uring_identity) {
            Target::Fixed {
                index: self.index,
                raw_fd: self.fd.as_ref().unwrap().as_raw_fd(),
                uring_identity: self.uring_identity.clone(),
            }
        } else {
            // If the OwnedRegisteredFile is used with a different Client instance, return the raw file descriptor to allow the file to be used normally,
            // at the cost of foregoing optimizations.
            Target::Fd(self.fd.as_ref().unwrap().as_raw_fd())
        }
    }
}

#[derive(Error, Debug)]
pub enum RegisterError {
    #[error("io_uring not supported")]
    IoUringNotSupported,
    #[error("too many files registered")]
    TooManyFilesRegistered,
    #[error("I/O error occurred while registering file")]
    IoError(#[from] io::Error),
}

impl Client {
    /// Borrow a file and register it with the io_uring instance.
    /// Returns the [`RegisteredFile`] struct if successful, or the error if not.
    pub fn register<'a>(
        &'a self,
        file: &'a (impl AsFd + ?Sized),
    ) -> Result<RegisteredFile<'a>, RegisterError> {
        let uring = self
            .uring
            .as_ref()
            .ok_or_else(|| RegisterError::IoUringNotSupported)?;
        // Cycle through the slots until we find an empty one.
        let index = loop {
            if uring.registered_files.len() > MAX_REGISTERED_FILES as usize {
                return Err(RegisterError::TooManyFilesRegistered);
            }
            let candidate_index =
                uring.next_file_slot.fetch_add(1, Ordering::SeqCst) % MAX_REGISTERED_FILES;
            if uring.registered_files.insert(candidate_index) {
                break candidate_index;
            }
        };
        let token = TemporaryToken {
            committed: false,
            index,
            registered_files: uring.registered_files.clone(),
        };
        uring
            .uring
            .submitter()
            .register_files_update(index, &[file.as_fd().as_raw_fd()])?;
        token.commit();
        Ok(RegisteredFile {
            index: Some(index),
            fd: file.as_fd(),
            uring_identity: uring.identity.clone(),
            _uring: uring.uring.clone(),
            _registered_files: uring.registered_files.clone(),
        })
    }

    /// Consume the [`OwnedFd`] and register it with the io_uring instance.
    /// Returns the [`OwnedRegisteredFile`] if successful, or the error and the [`OwnedFd`] if not.
    pub fn register_owned(
        &self,
        fd: OwnedFd,
    ) -> Result<OwnedRegisteredFile, (RegisterError, OwnedFd)> {
        let uring = match self.uring.as_ref() {
            Some(uring) => uring,
            None => return Err((RegisterError::IoUringNotSupported, fd)),
        };
        let index = loop {
            if uring.registered_files.len() > MAX_REGISTERED_FILES as usize {
                return Err((RegisterError::TooManyFilesRegistered, fd));
            }
            let candidate_index =
                uring.next_file_slot.fetch_add(1, Ordering::SeqCst) % MAX_REGISTERED_FILES;
            if uring.registered_files.insert(candidate_index) {
                break candidate_index;
            }
        };
        let token = TemporaryToken {
            committed: false,
            index,
            registered_files: uring.registered_files.clone(),
        };
        match uring
            .uring
            .submitter()
            .register_files_update(index, &[fd.as_raw_fd()])
        {
            Ok(_) => (),
            Err(e) => return Err((RegisterError::IoError(e), fd)),
        };
        token.commit();
        Ok(OwnedRegisteredFile {
            index,
            fd: Some(fd),
            uring_identity: uring.identity.clone(),
            _uring: uring.uring.clone(),
            _registered_files: uring.registered_files.clone(),
        })
    }
}
