mod command;
mod completion;
mod operations;
mod pending;
pub(crate) mod pending_io;
mod register;
mod requests;

use std::collections::VecDeque;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::thread::JoinHandle;
use std::{io, thread};

use command::Command;
use crossbeam_channel::TryRecvError;
use dashmap::{DashMap, DashSet};
use io_uring::IoUring;
use io_uring::cqueue::Entry as CEntry;
use io_uring::squeue::Entry as SEntry;
pub use register::OwnedRegisteredFile;
pub use register::RegisterError;
pub use register::RegisteredFile;
pub use requests::Target;

/// Maximum length for a single io_uring read/write operation.
///
/// io_uring uses i32 for return values, limiting single operations to ~2GB. The actual limit is 4096 bytes less than 2GB for unknown reasons.
pub const URING_LEN_MAX: u64 = 2 * 1024 * 1024 * 1024 - 4096;

/// Maximum number of files that can be registered with a single Uring instance.
const MAX_REGISTERED_FILES: u32 = 4096;
pub struct Client {
    uring: Option<ClientUring>,
    uring_enabled: Arc<AtomicBool>,
}

pub struct ClientUring {
    // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
    sender: crossbeam_channel::Sender<Command>,
    uring: Arc<IoUring>,
    probe: io_uring::Probe,
    uring_sthread: JoinHandle<()>,
    uring_cthread: JoinHandle<()>,
    registered_files: Arc<DashSet<u32>>,
    next_file_slot: Arc<AtomicU32>,
    identity: Arc<()>,
}

impl Drop for Client {
    fn drop(&mut self) {
        // Remove the uring instance, then join the threads.
        let uring = self.uring.take();
        if let Some(uring) = uring {
            drop(uring.sender);
            drop(uring.uring);
            uring
                .uring_sthread
                .join()
                .expect("uring_sthread join failed");
            // The completion queue thread is only joinable after the submission thread exits.
            uring
                .uring_cthread
                .join()
                .expect("uring_cthread join failed");
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    #[error("io-uring not supported")]
    IoUringNotSupported,
    #[error("io-uring build failed")]
    IoUringBuildFailed(#[from] io::Error),
}

/// Default ring size for io_uring (16384 entries).
///
/// This is a conservative default that works in most environments including containers
/// and memory-constrained systems. The kernel will further clamp this if needed via
/// `IORING_SETUP_CLAMP`.
pub const DEFAULT_RING_SIZE: u32 = 16384;

/// Configuration options for io_uring initialization.
///
/// These are advanced options that affect io_uring behavior. Most users should use `UringCfg::default()`.
/// Incorrect configuration may cause `EINVAL` errors or degraded performance.
///
/// # Kernel Requirements
///
/// Some options require specific kernel versions or capabilities:
/// - `coop_taskrun`: Linux 5.19+
/// - `defer_taskrun`: Linux 6.1+
/// - `sqpoll`: Requires `CAP_SYS_NICE` capability
/// - `iopoll`: Only works with O_DIRECT files on supported filesystems
#[derive(Clone, Debug)]
pub struct UringCfg {
    /// Size of the io_uring submission/completion queues (number of entries).
    ///
    /// Larger values allow more operations to be batched but consume more memory.
    /// The kernel will clamp this to the maximum supported size via `IORING_SETUP_CLAMP`.
    ///
    /// If you encounter `ENOMEM` errors during initialization, try reducing this value.
    /// Defaults to [`DEFAULT_RING_SIZE`] (16384 entries).
    pub ring_size: u32,

    /// Enable cooperative task running (Linux 5.19+). When enabled, the kernel will only process completions when the application explicitly asks for them, reducing overhead.
    pub coop_taskrun: bool,

    /// Enable deferred task running (Linux 6.1+). Similar to `coop_taskrun` but with additional deferral. Requires `coop_taskrun` to also be set.
    pub defer_taskrun: bool,

    /// Enable I/O polling mode. When enabled, the kernel will poll for completions instead of using interrupts. Only works with `O_DIRECT` files on supported filesystems. Can provide lower latency but uses more CPU.
    pub iopoll: bool,

    /// Enable submission queue polling with the given idle timeout in milliseconds. When enabled, a kernel thread will poll the submission queue, eliminating the need for system calls to submit I/O. The thread will go to sleep after being idle for the specified duration. **Requires `CAP_SYS_NICE` capability.**
    pub sqpoll: Option<u32>,

    /// Allow graceful degradation to non-io_uring mode for systems that do not support it. Note that if io_uring is only partially supported, the client still automatically falls back to non-io_uring mode for unsupported opcodes.   
    pub allow_fallback: bool,
}

impl Default for UringCfg {
    fn default() -> Self {
        Self {
            ring_size: DEFAULT_RING_SIZE,
            coop_taskrun: false,
            defer_taskrun: false,
            iopoll: false,
            sqpoll: None,
            allow_fallback: true,
        }
    }
}

impl Client {
    pub fn build(cfg: UringCfg) -> Result<Client, ClientBuildError> {
        let ring = {
            #[cfg(target_os = "linux")]
            let mut builder = IoUring::<SEntry, CEntry>::builder();
            // Use IORING_SETUP_CLAMP to let the kernel reduce the ring size if our requested size exceeds system limits. This is safer than failing outright.
            builder.setup_clamp();
            if cfg.coop_taskrun {
                builder.setup_coop_taskrun();
            };
            if cfg.defer_taskrun {
                builder.setup_defer_taskrun();
            };
            if cfg.iopoll {
                builder.setup_iopoll();
            }
            if let Some(sqpoll) = cfg.sqpoll {
                builder.setup_sqpoll(sqpoll);
            };
            match builder.build(cfg.ring_size) {
                Ok(uring) => Some(uring),
                Err(_) if cfg.allow_fallback => None,
                Err(e) => return Err(ClientBuildError::IoUringBuildFailed(e)),
            }
            #[cfg(not(target_os = "linux"))]
            if !cfg.allow_fallback {
                return Err(ClientBuildError::IoUringNotSupported);
            } else {
                None
            }
        };
        let pending: Arc<DashMap<u64, Command>> = Default::default();
        let mut client = Client {
            uring: None,
            uring_enabled: Arc::new(AtomicBool::new(true)),
        };
        if let Some(ring) = ring {
            // Pre-allocate sparse file table for registration (Linux 5.12+). If this fails, file registration won't work but unregistered fds will still function.
            let _ = ring.submitter().register_files_sparse(MAX_REGISTERED_FILES);
            let ring = Arc::new(ring);
            let (sender, receiver) = crossbeam_channel::unbounded::<Command>();
            let (wait_permit_tx, wait_permit_rx) = crossbeam_channel::unbounded::<()>();
            let sthread = thread::spawn({
                let pending = pending.clone();
                let ring = ring.clone();
                move || submission_thread(ring, pending, receiver, wait_permit_tx)
            });
            let cthread = thread::spawn({
                let pending = pending.clone();
                let ring = ring.clone();
                move || completion_thread(ring, pending, wait_permit_rx)
            });
            let mut probe = io_uring::Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            client.uring = Some(ClientUring {
                sender,
                uring: ring,
                probe: probe,
                uring_sthread: sthread,
                uring_cthread: cthread,
                registered_files: Arc::new(DashSet::new()),
                identity: Arc::new(()),
                next_file_slot: Arc::new(AtomicU32::new(0)),
            });
        }

        Ok(client)
    }
}

fn submission_thread(
    ring: Arc<IoUring>,
    pending: Arc<DashMap<u64, Command>>,
    receiver: crossbeam_channel::Receiver<Command>,
    wait_permit_tx: crossbeam_channel::Sender<()>,
) {
    // SAFETY: We ensure that the submission queue is only accessed from this single thread. The completion queue is accessed from a separate thread.
    let mut submission = unsafe { ring.submission_shared() };
    let mut next_id = 0u64;
    let mut queue = VecDeque::new();

    // Structure that allows entry removal from the pending map during a panic unwind.
    struct PendingEntryGuard {
        pending: Arc<DashMap<u64, Command>>,
        id: u64,
        armed: bool,
    }
    impl PendingEntryGuard {
        fn new(pending: Arc<DashMap<u64, Command>>, id: u64) -> Self {
            Self {
                pending,
                id,
                armed: true,
            }
        }
        fn disarm(&mut self) {
            self.armed = false;
        }
    }
    impl Drop for PendingEntryGuard {
        fn drop(&mut self) {
            if self.armed {
                self.pending.remove(&self.id);
            }
        }
    }

    while let Ok(command) = receiver.recv() {
        queue.push_back(command);
        while let Ok(command) = receiver.try_recv() {
            queue.push_back(command);
        }

        // How the io_uring submission queue works:
        // - The buffer is shared between the kernel and userspace.
        // - There are atomic head and tail indices that allow them to be shared mutably between kernel and userspace safely.
        // - The Rust library we're using abstracts over this by caching the head and tail as local values. Once we've made our inserts, we update the atomic tail and then tell the kernel to consume some of the queue. When we update the atomic tail, we also check the atomic head and update our local cached value; some entries may have been consumed by the kernel in some other thread since we last checked and we may actually have more free space than we thought.
        while let Some(mut command) = queue.pop_front() {
            if submission.is_full() {
                ring.submit_and_wait(1).expect("failed to submit to ring");
            }
            let (io_uring_entry, ack, id, mut guard) = 'entry_build: loop {
                let id = next_id;
                next_id = next_id.wrapping_add(1);

                let io_uring_entry = command::build_io_uring_entry(&command, id);
                match pending.entry(id) {
                    dashmap::Entry::Vacant(pending_guard) => {
                        let ack = command::take_command_ack(&mut command);
                        pending_guard.insert(command);
                        let guard = PendingEntryGuard::new(pending.clone(), id);
                        break 'entry_build (io_uring_entry, ack, id, guard);
                    }
                    dashmap::Entry::Occupied(_) => {
                        continue 'entry_build;
                    }
                }
            };
            // SAFETY: The submission entry references memory owned by the caller's future, which is awaiting completion.
            unsafe {
                submission
                    .push(&io_uring_entry)
                    .expect("failed to push to submission queue");
            }
            submission.sync();
            // This is still necessary even with sqpoll, as our kernel thread may have gone to sleep.
            ring.submit().unwrap();
            // Only commit the pending entry guard after the submission is successful.
            guard.disarm();
            // Send the operation ID to the pending I/O object to allow it to be cancelled.
            if let Some(ack) = ack {
                let _ = ack.send(id);
            }
            // Send a wait permit to the completion thread to indicate that it can now perform a blocking read of the next completion entry.
            wait_permit_tx
                .send(())
                .expect("completion thread must wait until submission thread exits");
        }
    }
}

fn completion_thread(
    ring: Arc<IoUring>,
    pending: Arc<DashMap<u64, Command>>,
    wait_permit_rx: crossbeam_channel::Receiver<()>,
) {
    let mut completion = unsafe { ring.completion_shared() };
    // This flag marks that the submission thread has terminated and we should drain all remaining operations in the pending map until it is empty.
    let mut submission_thread_terminated = false;
    loop {
        if submission_thread_terminated && pending.is_empty() {
            break;
        }
        let mut wait_permits = 1u64;
        if !submission_thread_terminated {
            // The completion thread relies on the submission thread to send us a message that an operation has to be completed.
            match wait_permit_rx.recv() {
                Ok(()) => {
                    if wait_permits < 65536 {
                        // Try to batch more wait permits.
                        match wait_permit_rx.try_recv() {
                            Ok(()) => wait_permits += 1,
                            Err(TryRecvError::Empty) => {}
                            Err(TryRecvError::Disconnected) => {
                                submission_thread_terminated = true;
                                continue;
                            }
                        }
                    }
                }
                Err(_) => {
                    submission_thread_terminated = true;
                    continue;
                }
            }
        }
        for _ in 0..wait_permits {
            // Blocking with io_uring_enter is OK now, since the submission thread permits a wait.
            let Some(e) = completion.next() else {
                log::debug!("no completion found, submitting and waiting");
                ring.submitter()
                    .submit_and_wait(1)
                    .expect("failed to wait for completion");
                log::debug!("exiting wait");
                completion.sync();
                continue;
            };
            let id = e.user_data();
            let (_, req) = pending
                .remove(&id)
                .expect("completion for unknown request id");
            completion::handle_completion(req, e.result());
        }
    }
}

pub trait UringTarget {
    /// Method for converting the target to a borrowed file descriptor.
    fn as_file_descriptor(&self) -> BorrowedFd<'_>;

    /// Method for converting the target to a raw target object that can be used by the io_uring client.
    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target;
}

impl<T> UringTarget for T
where
    T: AsFd + ?Sized,
{
    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
        Target::Fd(self.as_fd().as_raw_fd())
    }

    fn as_file_descriptor(&self) -> BorrowedFd<'_> {
        self.as_fd()
    }
}

pub type BoxedUringTarget<'a> = Box<dyn UringTarget + Send + Sync + 'a>;

impl<'a> UringTarget for BoxedUringTarget<'a> {
    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
        unsafe { self.as_ref().as_target(_uring_identity) }
    }

    fn as_file_descriptor(&self) -> BorrowedFd<'_> {
        self.as_ref().as_file_descriptor()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Client, UringCfg};

    #[tokio::test]
    #[test_log::test]
    async fn client_should_drop() {
        let client = Client::build(UringCfg::default()).expect("failed to build client");
        drop(client);
    }
}
