//! Core client module for asynchronous I/O operations.
mod completion;
mod operations;
pub(crate) mod pending_io;
mod register;
mod requests;
pub(crate) mod ticketing;

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::panic::UnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use ticketing::SubmissionTicketQueue;

use dashmap::DashSet;
use io_uring::IoUring;
use io_uring::cqueue::Entry as CEntry;
use io_uring::squeue::Entry as SEntry;
pub use register::OwnedRegisteredFile;
pub use register::RegisterError;
pub use register::RegisteredFile;
pub use requests::Target;

use crate::client::pending_io::uring::{PendingMap, UringPendingIoStatus, UringPendingIoSubmitter};
use crate::client::ticketing::SubmissionTicketId;

/// Maximum length for a single io_uring read/write operation.
///
/// io_uring uses i32 for return values, limiting single operations to ~2GB. The actual limit is 4096 bytes less than 2GB for unknown reasons.
pub const URING_LEN_MAX: u64 = 2 * 1024 * 1024 * 1024 - 4096;

/// Maximum number of files that can be registered with a single Uring instance.
const MAX_REGISTERED_FILES: u32 = 4096;

/// The client instance for asynchronous I/O operations.
pub struct Client {
    uring: Option<ClientUring>,
    uring_enabled: Arc<AtomicBool>,
}

pub(crate) struct ClientUring {
    normal_submission_ticket_queue: SubmissionTicketQueue,
    cancel_submission_ticket_queue: SubmissionTicketQueue,
    submission_sender: crossbeam_channel::Sender<UringPendingIoSubmitter>,
    pending: Weak<PendingMap>,
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
            operation_queue_size: DEFAULT_OP_QUEUE_SIZE,
            cancel_queue_size: DEFAULT_CANCEL_QUEUE_SIZE,
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
        let expected_total_squeue_size = cfg
            .operation_queue_size
            .checked_add(cfg.cancel_queue_size)
            .ok_or_else(|| {
                ClientBuildError::IoUringBuildFailed(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "total queue size is too large",
                ))
            })?;

        let ring = {
            #[cfg(target_os = "linux")]
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
        let mut client = Client {
            uring: None,
            uring_enabled: Arc::new(AtomicBool::new(true)),
        };
        if let Some(mut ring) = ring {
            // Pre-allocate sparse file table for registration (Linux 5.12+). If this fails, file registration won't work but unregistered fds will still function.
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
            let mut ticket_queues = SubmissionTicketQueue::new_multiple(&[
                op_ticket_queue_size,
                cancel_ticket_queue_size,
            ]);
            let cancel_submission_ticket_queue = ticket_queues.pop().unwrap();
            let normal_submission_ticket_queue = ticket_queues.pop().unwrap();
            let (submission_sender, submission_receiver) =
                crossbeam_channel::bounded::<UringPendingIoSubmitter>(actual_total_squeue_size);
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
                        submission_thread(
                            ring,
                            pending_map,
                            submission_receiver,
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
                    move || completion_thread(ring, pending, interrupt_sender)
                })
                .expect("should spawn thread");
            let mut probe = io_uring::Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            client.uring = Some(ClientUring {
                normal_submission_ticket_queue,
                cancel_submission_ticket_queue,
                pending: Arc::downgrade(&pending_map),
                submission_sender,
                uring: ring,
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

/// Dropper for stopping the completion thread by submitting a poison ticket. Should not allocate memory from the heap, and works if the thread either exits normally or panics.
pub(crate) struct SubmissionDropper {
    uring: Arc<IoUring>,
    pending_map: Arc<PendingMap>,
}

impl SubmissionDropper {
    pub(crate) fn new(uring: Arc<IoUring>, pending_map: Arc<PendingMap>) -> Self {
        Self { uring, pending_map }
    }
}

impl Drop for SubmissionDropper {
    // Drop handler in case of exceptions like OOM. Should not allocate memory from the heap
    fn drop(&mut self) {
        let mut submission = unsafe { self.uring.submission_shared() };
        submission.sync();
        // Cancel everything in the pending map. At this stage, no operations can be marked as submitted anymore, so there is no race condition.
        // The existence of the cancellation queue where operations are completed instantaneously guarantees that there is always a space for these special operations.
        for kv in self.pending_map.iter() {
            let id = kv.key();
            let filler = kv.value();
            if filler.status() != UringPendingIoStatus::Submitted {
                continue;
            }
            // Allowing multiple operations to use the same ticket is OK here because we do not expect a response and no discriminator is necessary.
            let entry = io_uring::opcode::AsyncCancel::new(id.0)
                .build()
                .user_data(SubmissionTicketId::POISON_CANCEL.0);
            while unsafe { submission.push(&entry) }.is_err() {
                self.uring.submit_and_wait(1).expect(
                    "failed to wait for empty submission slot and submit cancellation operations",
                );
                submission.sync();
                continue;
            }
        }
        submission.sync();
        self.uring
            .submitter()
            .submit()
            .expect("failed to submit last batch of cancellation operations");
        // Poison pill is guaranteed to be the last item, after all operations are done or cancelled. The IO_DRAIN flag ensures that the cancellation have all finished.
        let entry = io_uring::opcode::Nop::new()
            .build()
            .user_data(SubmissionTicketId::POISON.0)
            .flags(io_uring::squeue::Flags::IO_DRAIN);
        while unsafe { submission.push(&entry) }.is_err() {
            // The queue is in latest state, so if we cannot submit, we can safely wait for an event.
            self.uring
                .submit_and_wait(1)
                .expect("failed to wait for empty submission slot");
            submission.sync();
        }
        submission.sync();
        self.uring
            .submitter()
            .submit()
            .expect("failed to submit poison entry");
    }
}

pub(crate) enum InterruptCommand {
    SubmissionCleanup,
    SubmissionPanic,
    CompletionPanic,
}

/// Thread for batching the io_uring_enter syscall to flush entries to the io_uring instance.
fn submission_thread(
    ring: Arc<IoUring>,
    pending_map: Arc<PendingMap>,
    receiver: crossbeam_channel::Receiver<UringPendingIoSubmitter>,
    interrupt: crossbeam_channel::Receiver<InterruptCommand>,
) {
    let _submission_dropper = SubmissionDropper::new(ring.clone(), pending_map.clone());
    let mut submission = unsafe { ring.submission_shared() };
    let capacity = submission.capacity();
    let mut queue = VecDeque::with_capacity(capacity);
    loop {
        let item = crossbeam_channel::select! {
            recv(interrupt) -> command => {
                match command {
                    Ok(InterruptCommand::SubmissionPanic) => {
                        panic!("submission thread intentionally panicked");
                    }
                    Ok(InterruptCommand::CompletionPanic) => {
                        let entry = io_uring::opcode::Nop::new()
                            .build()
                            .user_data(SubmissionTicketId::COMPLETION_PANIC.0);
                        while unsafe { submission.push(&entry) }.is_err() {
                            ring.submitter().submit_and_wait(1).expect("failed to wait for empty submission slot");
                            submission.sync();
                            continue;
                        }
                        submission.sync();
                        ring.submitter()
                            .submit()
                            .expect("failed to submit entry that triggers completion panics");
                        continue;
                    }
                    Ok(InterruptCommand::SubmissionCleanup) => {
                        break;
                    }
                    Err(crossbeam_channel::RecvError) => panic!("interrupt channel closed"),
                }
            }
            recv(receiver) -> item => {
                match item {
                    Ok(item) => item,
                    Err(crossbeam_channel::RecvError) => break,
                }
            }
        };
        queue.push_back(item);
        queue.extend(receiver.try_iter().take(capacity - 1));
        for submitter in queue.iter() {
            let entry = submitter.create_entry();
            while unsafe { submission.push(&entry) }.is_err() {
                // We need to synchronize the head and tail before retrying because it is stale. However, we do not need to block because the queues restrict the number of active tickets.
                submission.sync();
                continue;
            }
        }
        submission.sync();
        ring.submitter()
            .submit()
            .expect("failed to perform batch submit");
        for entry in queue.drain(..) {
            entry.mark_submitted();
        }
    }
}

struct CompletionDropper {
    interrupt_sender: crossbeam_channel::Sender<InterruptCommand>,
    poison_lock: std::sync::Mutex<()>,
}

impl CompletionDropper {
    pub(crate) fn new(interrupt_sender: crossbeam_channel::Sender<InterruptCommand>) -> Self {
        Self {
            interrupt_sender,
            poison_lock: std::sync::Mutex::new(()),
        }
    }
}

impl Drop for CompletionDropper {
    fn drop(&mut self) {
        // If completion thread is panicking, we need to stop the submission thread and mark all entries in the pending map as failed.
        if self.poison_lock.is_poisoned() {
            let _ = self
                .interrupt_sender
                .send(InterruptCommand::SubmissionCleanup);
        }
    }
}

/// Thread for handling completions from the io_uring completion queue.
fn completion_thread(
    ring: Arc<IoUring>,
    pending: Arc<PendingMap>,
    interrupt_sender: crossbeam_channel::Sender<InterruptCommand>,
) {
    let mut completion = unsafe { ring.completion_shared() };
    let completion_dropper = CompletionDropper::new(interrupt_sender.clone());
    {
        let _poison_guard = completion_dropper.poison_lock.lock().unwrap();
        loop {
            let e = loop {
                let Some(entry) = completion.next() else {
                    ring.submitter()
                        .submit_and_wait(1)
                        .expect("failed to wait for completion");
                    completion.sync();
                    continue;
                };
                break entry;
            };
            let id = SubmissionTicketId(e.user_data());
            if id.is_poison() {
                break;
            }
            // Anonymous cancellation operations, we do not need to respond.
            if id.is_poison_cancel() {
                continue;
            }
            if id.is_completion_panic() {
                panic!("completion thread intentionally panicked");
            }
            let (_, req) = pending
                .remove(&id)
                .expect("completion for unknown request id");
            completion::handle_completion(req, e.result());
        }
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
