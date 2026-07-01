//! Core client module for asynchronous I/O operations.
mod completion;
mod operations;
pub(crate) mod pending_io;
#[cfg(target_os = "linux")]
mod register;
mod requests;
#[cfg(target_os = "linux")]
pub(crate) mod ticketing;
#[cfg(target_os = "linux")]
mod uring_background;

pub use completion::{ReadResult, ReadvResult, WriteResult, WritevResult};

#[cfg(target_os = "linux")]
use crate::client::uring_background::InterruptCommand;
#[cfg(target_os = "linux")]
use dashmap::DashSet;
#[cfg(target_os = "linux")]
use io_uring::IoUring;
#[cfg(target_os = "linux")]
use io_uring::cqueue::Entry as CEntry;
#[cfg(target_os = "linux")]
use io_uring::squeue::Entry as SEntry;
use pending_io::fallback::Spawnable;
#[cfg(target_os = "linux")]
use pending_io::uring::{PendingMap, UringPendingIoSubmitter};
#[cfg(target_os = "linux")]
pub use register::OwnedRegisteredFile;
#[cfg(target_os = "linux")]
pub use register::RegisterError;
#[cfg(target_os = "linux")]
pub use register::RegisteredFile;
pub use requests::Target;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::panic::UnwindSafe;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize};
#[cfg(target_os = "linux")]
use std::thread::JoinHandle;
#[cfg(target_os = "linux")]
use ticketing::SubmissionTicketQueue;

/// Maximum length for a single io_uring read/write operation.
///
/// io_uring uses i32 for return values, limiting single operations to ~2GB. The actual limit is 4096 bytes less than 2GB for unknown reasons.
pub const URING_LEN_MAX: u64 = 2 * 1024 * 1024 * 1024 - 4096;

/// Maximum number of files that can be registered with a single Uring instance.
#[cfg(target_os = "linux")]
const MAX_REGISTERED_FILES: u32 = 4096;

/// The client instance for asynchronous I/O operations.
pub struct Client {
    #[cfg(target_os = "linux")]
    uring: Option<ClientUring>,
    #[cfg(target_os = "linux")]
    uring_enabled: Arc<AtomicBool>,
    fallback_spawner: Arc<dyn Spawnable>,
}

#[cfg(target_os = "linux")]
pub(crate) struct ClientUring {
    submission_sender: crossbeam_channel::Sender<UringPendingIoSubmitter>,
    active_requests: Arc<AtomicUsize>,
    op_ticket_queue_size: usize,
    uring: Arc<IoUring>,
    probe: io_uring::Probe,
    sthread: JoinHandle<()>,
    cthread: JoinHandle<()>,
    interrupt_sender: crossbeam_channel::Sender<InterruptCommand>,
    registered_files: Arc<DashSet<u32>>,
    next_file_slot: Arc<AtomicU32>,
    identity: Arc<()>,
}

impl UnwindSafe for Client {}

impl Drop for Client {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        {
            // Remove the uring instance, then join the threads.
            let uring = self.uring.take();
            if let Some(uring) = uring {
                drop(uring.submission_sender);
                drop(uring.uring);
                let _ = uring.sthread.join().inspect_err(|e| {
                    log::error!("submission thread panicked: {:?}", e);
                });
                let _ = uring.cthread.join().inspect_err(|e| {
                    log::error!("completion thread panicked: {:?}", e);
                });
            }
        }
    }
}

/// Errors that can occur when building a client.
#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    /// io_uring is not supported on the target system.
    #[error("io-uring not supported")]
    IoUringNotSupported,
    /// io_uring build failed.
    #[error("io-uring build failed")]
    IoUringBuildFailed(#[from] io::Error),
}

/// Default operation queue size for io_uring (16384 - 512 entries). This leaves room for the cancel queue.
/// This is a conservative default that works in most environments including containers
/// and memory-constrained systems. The kernel will further clamp this if needed via
/// `IORING_SETUP_CLAMP`.
pub const DEFAULT_OP_QUEUE_SIZE: u32 = 16384 - 512;

/// Default cancel queue size for io_uring (512 entries).
/// This is a conservative default that works in most environments including containers
/// and memory-constrained systems. The kernel will further clamp this if needed via
/// `IORING_SETUP_CLAMP`.
pub const DEFAULT_CANCEL_QUEUE_SIZE: u32 = 512;

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
    /// Size of the io_uring submission/completion queues for normal operations (number of entries).
    ///
    /// Larger values allow more operations to be batched but consume more memory.
    /// The kernel will clamp this to the maximum supported size via `IORING_SETUP_CLAMP`.
    ///
    /// If you encounter `ENOMEM` errors during initialization, try reducing this value.
    /// Defaults to [`DEFAULT_OP_QUEUE_SIZE`] (16384 entries).
    pub operation_queue_size: u32,

    /// Size of the io_uring submission/completion queues for cancel operations (number of entries).
    ///
    /// Larger values allow more operations to be batched but consume more memory.
    /// The kernel will clamp this to the maximum supported size via `IORING_SETUP_CLAMP`.
    ///
    /// If you encounter `ENOMEM` errors during initialization, try reducing this value.
    /// Defaults to [`DEFAULT_CANCEL_QUEUE_SIZE`] (512 entries).
    pub cancel_queue_size: u32,

    /// Fallback spawner for fallback pending I/O operations.
    pub fallback_spawner: Arc<dyn Spawnable>,

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

/// Metrics for the io_uring instance.
/// Note that this does not cover metrics for Tokio pending requests as they are supposed to be dealt with by the runtime.
#[derive(Debug, Clone)]
pub struct UringMetrics {
    /// Maximum number of concurrent operations that the io_uring instance can handle.
    pub max_concurrent_operations: usize,
    /// Number of active io_uring operations in the io_uring instance.
    pub active_operations: usize,
    /// Utilization of the io_uring instance.
    pub utilization: f64,
}

impl Default for UringCfg {
    fn default() -> Self {
        Self {
            operation_queue_size: DEFAULT_OP_QUEUE_SIZE,
            cancel_queue_size: DEFAULT_CANCEL_QUEUE_SIZE,
            fallback_spawner: Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(0)
                    .thread_name(|_| "ahfs_fallback".to_string())
                    .build()
                    .unwrap(),
            ),
            coop_taskrun: false,
            defer_taskrun: false,
            iopoll: false,
            sqpoll: None,
            allow_fallback: true,
        }
    }
}

impl Client {
    /// Builds a new client with the given configuration.
    pub fn build(cfg: UringCfg) -> Result<Client, ClientBuildError> {
        #[cfg(target_os = "linux")]
        let expected_total_squeue_size = cfg
            .operation_queue_size
            .checked_add(cfg.cancel_queue_size)
            .ok_or_else(|| {
                ClientBuildError::IoUringBuildFailed(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "total queue size is too large",
                ))
            })?;
        #[cfg(target_os = "linux")]
        let ring = {
            let mut builder = IoUring::<SEntry, CEntry>::builder();
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
            builder.setup_clamp();
            match builder.build(expected_total_squeue_size) {
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
        #[allow(unused_mut)]
        let mut client = Client {
            #[cfg(target_os = "linux")]
            uring: None,
            #[cfg(target_os = "linux")]
            uring_enabled: Arc::new(AtomicBool::new(true)),
            fallback_spawner: cfg.fallback_spawner,
        };
        #[cfg(target_os = "linux")]
        if let Some(mut ring) = ring {
            // Pre-allocate sparse file table for registration (Linux 5.12+). If this fails, file registration won't work but unregistered fds will still function.

            use crate::client::uring_background::InterruptCommand;
            let _ = ring.submitter().register_files_sparse(MAX_REGISTERED_FILES);
            // Limit squeue to usize::MAX - 3 to avoid conflict with the reserved IDs.
            let actual_total_squeue_size = ring.submission().capacity().min(usize::MAX - 3);
            // Investigate the submission queue size.
            let (op_ticket_queue_size, cancel_ticket_queue_size) = if actual_total_squeue_size
                >= expected_total_squeue_size as usize
            {
                // We allocate the ticket queue sizes as planned.
                (
                    cfg.operation_queue_size as usize,
                    cfg.cancel_queue_size as usize,
                )
            } else {
                // We need to adjust the ticket queue sizes to fit the submission queue size using a ratio.
                log::debug!(
                    "actual_total_squeue_size: {}, expected_total_squeue_size: {}",
                    actual_total_squeue_size,
                    expected_total_squeue_size
                );
                let cancel_queue_size = actual_total_squeue_size
                    .saturating_mul(cfg.cancel_queue_size as usize)
                    .saturating_div(expected_total_squeue_size as usize);
                if cancel_queue_size == 0 {
                    return Err(ClientBuildError::IoUringBuildFailed(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cancel queue size after clamping is 0, try to increase cfg.cancel_queue_size",
                    )));
                }
                let operation_queue_size =
                    actual_total_squeue_size.saturating_sub(cancel_queue_size);
                if operation_queue_size == 0 {
                    return Err(ClientBuildError::IoUringBuildFailed(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "operation queue size after clamping is 0, try to increase cfg.operation_queue_size",
                    )));
                }
                (operation_queue_size, cancel_queue_size)
            };
            // Create the ticket queues.
            let ticket_queue =
                SubmissionTicketQueue::new(op_ticket_queue_size, cancel_ticket_queue_size, 0);
            // Use unbounded - it is better to allow the item to be dumped in memory instantly and perform backpressure inside one of the threads rather than having a newly freed thread signal multiple waiters all at once (unlike normal threads, tasks can be trivially cancelled, so a normal notify_one may lead to missed tickets leading to bugs)
            let (submission_sender, submission_receiver) =
                crossbeam_channel::unbounded::<UringPendingIoSubmitter>();
            let pending_map = Arc::new(PendingMap::new());
            let ring = Arc::new(ring);
            let (interrupt_sender, interrupt_receiver) =
                crossbeam_channel::bounded::<InterruptCommand>(1);
            let sthread = std::thread::Builder::new()
                .name(String::from("ahfs_worker"))
                .spawn({
                    let ring = ring.clone();
                    let pending_map = pending_map.clone();
                    move || {
                        uring_background::submission_thread(
                            ring,
                            pending_map,
                            submission_receiver,
                            ticket_queue,
                            interrupt_receiver,
                        )
                    }
                })
                .expect("should spawn thread");
            let cthread = std::thread::Builder::new()
                .name(String::from("ahfs_worker"))
                .spawn({
                    let pending = pending_map.clone();
                    let ring = ring.clone();
                    let interrupt_sender = interrupt_sender.clone();
                    move || uring_background::completion_thread(ring, pending, interrupt_sender)
                })
                .expect("should spawn thread");
            let mut probe = io_uring::Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            client.uring = Some(ClientUring {
                submission_sender,
                uring: ring,
                active_requests: Arc::new(AtomicUsize::new(0)),
                op_ticket_queue_size,
                probe,
                sthread,
                cthread,
                interrupt_sender,
                registered_files: Arc::new(DashSet::new()),
                identity: Arc::new(()),
                next_file_slot: Arc::new(AtomicU32::new(0)),
            });
        }
        Ok(client)
    }
}

/// Trait that represents valid objects as a raw `io_uring` target.
pub trait UringTarget {
    /// Method for converting the target to a borrowed file descriptor.
    fn as_file_descriptor(&self) -> BorrowedFd<'_>;

    /// Method for converting the target to a raw target object that can be used by the io_uring client.
    ///
    /// # Safety
    /// This method bypasses the borrow checker's restrictions.
    /// You must ensure that the file descriptor and index remains valid (e.g. by keeping the original object).
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

/// A boxed [`UringTarget`] object that can be sent and shared between threads.
pub type BoxedUringTarget<'a> = Box<dyn UringTarget + Send + Sync + 'a>;

/// Implementation of the [`UringTarget`] trait for a boxed [`UringTarget`] object.
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
    use std::{sync::Arc, time::Duration};

    use crate::{Client, UringCfg};

    #[tokio::test]
    #[test_log::test]
    async fn client_should_drop() {
        let client = Client::build(UringCfg::default()).expect("failed to build client");
        drop(client);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[test_log::test]
    async fn submission_thread_panic_should_not_hang() {
        let client = Arc::new(Client::build(UringCfg::default()).expect("failed to build client"));
        let task = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (mut reader, _writer) = std::io::pipe().expect("failed to create pipe");
                let mut buf = [0; 64];
                let mut pending = client.read(&mut reader, &mut buf);
                let completion = pending.completion().expect("no completion future returned");
                // Trigger the completion future to start. Panic in the submission thread should cause any remaining operations to be cancelled.
                completion.await.expect_err("completion should fail");
            }
        });
        tokio::time::sleep(Duration::from_secs_f64(0.01)).await;
        client.sthread_panic();
        task.await.expect("task should not panic");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[test_log::test]
    async fn completion_thread_panic_should_not_hang() {
        let client = Arc::new(Client::build(UringCfg::default()).expect("failed to build client"));
        let task = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (mut reader, _writer) = std::io::pipe().expect("failed to create pipe");
                let mut buf = [0; 64];
                let mut pending = client.read(&mut reader, &mut buf);
                let completion = pending.completion().expect("no completion future returned");
                // Trigger the completion future to start.
                completion.await.expect_err("completion should fail");
            }
        });
        tokio::time::sleep(Duration::from_secs_f64(0.01)).await;
        client.cthread_panic();
        task.await.expect("task should not panic");
    }
}
