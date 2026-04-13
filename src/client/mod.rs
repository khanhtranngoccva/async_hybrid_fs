mod command;
mod completion;
mod operations;
mod pending;
pub(crate) mod pending_io;
mod register;
mod requests;
pub(crate) mod ticketing;

use std::collections::VecDeque;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::thread::JoinHandle;
use std::{io, thread};
use ticketing::SubmissionTicketQueue;

use dashmap::{DashMap, DashSet};
use io_uring::IoUring;
use io_uring::cqueue::Entry as CEntry;
use io_uring::squeue::Entry as SEntry;
pub use register::OwnedRegisteredFile;
pub use register::RegisterError;
pub use register::RegisteredFile;
pub use requests::Target;

use crate::client::command::{Command, CommandWithTicket};
use crate::client::pending_io::PendingIoDebuggingEvent;
use crate::client::ticketing::{
    CompletionTicketQueue, CompletionTicketSubmitter, SubmissionTicketId,
};

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

pub(crate) struct ClientUring {
    normal_sender: crossbeam_channel::Sender<(
        Command,
        Option<tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    )>,
    cancel_sender: crossbeam_channel::Sender<(
        Command,
        Option<tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    )>,
    normal_backpressure_thread: JoinHandle<()>,
    cancel_backpressure_thread: JoinHandle<()>,
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
            // Drop the senders to prevent any more commands from being submitted.
            drop(uring.normal_sender);
            drop(uring.cancel_sender);
            drop(uring.uring);
            // Join the backpressure threads.
            uring
                .normal_backpressure_thread
                .join()
                .expect("normal_backpressure_thread join failed");
            uring
                .cancel_backpressure_thread
                .join()
                .expect("cancel_backpressure_thread join failed");
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
    /// Defaults to [`DEFAULT_RING_SIZE`] (16384 entries).
    pub operation_queue_size: u32,

    /// Size of the io_uring submission/completion queues for cancel operations (number of entries).
    ///
    /// Larger values allow more operations to be batched but consume more memory.
    /// The kernel will clamp this to the maximum supported size via `IORING_SETUP_CLAMP`.
    ///
    /// If you encounter `ENOMEM` errors during initialization, try reducing this value.
    /// Defaults to [`DEFAULT_RING_SIZE`] (16384 entries).
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
            // Clamp is not used here to ensure that the submission queue is not smaller than the requested size.
            // If there is a problem, .
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
        let pending: Arc<DashMap<SubmissionTicketId, CommandWithTicket>> = Default::default();
        let mut client = Client {
            uring: None,
            uring_enabled: Arc::new(AtomicBool::new(true)),
        };
        if let Some(mut ring) = ring {
            // Pre-allocate sparse file table for registration (Linux 5.12+). If this fails, file registration won't work but unregistered fds will still function.
            let _ = ring.submitter().register_files_sparse(MAX_REGISTERED_FILES);
            let actual_total_squeue_size = ring.submission().capacity();
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
            let (completion_ticket_submitter, completion_ticket_queue) =
                ticketing::completion_ticket_pair();

            let ring = Arc::new(ring);
            let (sender, receiver) = crossbeam_channel::unbounded::<CommandWithTicket>();
            let (normal_sender, normal_receiver) = crossbeam_channel::unbounded();
            let (cancel_sender, cancel_receiver) = crossbeam_channel::unbounded();
            let normal_backpressure_thread = thread::spawn({
                let dispatch = sender.clone();
                move || {
                    backpressure_thread(normal_receiver, normal_submission_ticket_queue, dispatch)
                }
            });
            let cancel_backpressure_thread = thread::spawn({
                let dispatch = sender.clone();
                move || {
                    backpressure_thread(cancel_receiver, cancel_submission_ticket_queue, dispatch)
                }
            });
            let sthread = thread::spawn({
                let pending = pending.clone();
                let ring = ring.clone();
                move || submission_thread(ring, pending, receiver, completion_ticket_submitter)
            });
            let cthread = thread::spawn({
                let pending = pending.clone();
                let ring = ring.clone();
                move || completion_thread(ring, pending, completion_ticket_queue)
            });
            let mut probe = io_uring::Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            client.uring = Some(ClientUring {
                normal_sender,
                cancel_sender,
                uring: ring,
                probe: probe,
                normal_backpressure_thread,
                cancel_backpressure_thread,
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

/// Thread for submitting events with backpressure - if a queue is full, the thread will block until a task is freed. As a result, the caller that sends commands to this thread does not need to block.
fn backpressure_thread(
    command_receiver: crossbeam_channel::Receiver<(
        Command,
        Option<tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    )>,
    ticket_queue: SubmissionTicketQueue,
    dispatch: crossbeam_channel::Sender<CommandWithTicket>,
) {
    loop {
        let (command, debugging_event_tx) = match command_receiver.recv() {
            Ok(item) => item,
            Err(crossbeam_channel::RecvError) => break,
        };
        let ticket = ticket_queue.request_submission_ticket(debugging_event_tx.as_ref());
        dispatch
            .send(CommandWithTicket {
                submission_ticket: ticket,
                command,
            })
            .expect("main submission thread dead");
    }
}

/// Thread for aggregating events from backpressure threads and submitting them to the io_uring submission queue.
fn submission_thread(
    ring: Arc<IoUring>,
    pending: Arc<DashMap<SubmissionTicketId, CommandWithTicket>>,
    receiver: crossbeam_channel::Receiver<CommandWithTicket>,
    completion_ticket_submitter: CompletionTicketSubmitter,
) {
    // SAFETY: We ensure that the submission queue is only accessed from this single thread. The completion queue is accessed from a separate thread.
    let mut submission = unsafe { ring.submission_shared() };
    let mut queue = VecDeque::new();

    // Structure that allows entry removal from the pending map during a panic unwind.
    struct PendingEntryGuard {
        pending: Arc<DashMap<SubmissionTicketId, CommandWithTicket>>,
        id: SubmissionTicketId,
        armed: bool,
    }
    impl PendingEntryGuard {
        fn new(
            pending: Arc<DashMap<SubmissionTicketId, CommandWithTicket>>,
            id: SubmissionTicketId,
        ) -> Self {
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
                // TODO: We do not want to wait using a syscall when we have a cancel event
                ring.submit_and_wait(1).expect("failed to submit to ring");
            }
            let io_uring_entry = command::build_io_uring_entry(&command);
            let ack = command::take_command_ack(&mut command.command);
            let id = command.submission_ticket.id();
            let insertion_res = pending.insert(id, command);
            assert!(
                insertion_res.is_none(),
                "command with id {} already exists in pending map",
                id.0
            );
            let mut guard = PendingEntryGuard::new(pending.clone(), id);
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
            completion_ticket_submitter.grant_completion_ticket();
        }
    }
}

/// Thread for handling completions from the io_uring completion queue.
fn completion_thread(
    ring: Arc<IoUring>,
    pending: Arc<DashMap<SubmissionTicketId, CommandWithTicket>>,
    queue: CompletionTicketQueue,
) {
    let mut completion = unsafe { ring.completion_shared() };
    // This flag marks that the submission thread has terminated and we should drain all remaining operations in the pending map until it is empty.
    let mut submission_thread_terminated = false;
    loop {
        if submission_thread_terminated && pending.is_empty() {
            break;
        }
        let mut wait_permits = 1usize;
        if !submission_thread_terminated {
            match queue.request_completion_tickets() {
                Some(count) => wait_permits = count,
                None => {
                    submission_thread_terminated = true;
                    continue;
                }
            }
        }
        for _ in 0..wait_permits {
            // Blocking and looping with io_uring_enter is OK now, since the submission thread permits a wait.
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
