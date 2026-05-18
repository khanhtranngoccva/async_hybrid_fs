use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// The submission ticket ID, which may be used as the user_data field/entry ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct SubmissionTicketId(pub(crate) u64);

impl SubmissionTicketId {
    /// Special operation to signal the submission thread to stop.
    /// Only sendable after the kernel has registered cancellations for all remaining Submitted operations.
    pub(crate) const POISON: Self = Self(u64::MAX);
    /// Special operation that cancels an operation in the pending operations map with the Submitted status.
    pub(crate) const POISON_CANCEL: Self = Self(u64::MAX - 1);
    /// Operation to signal the completion thread to panic.
    pub(crate) const COMPLETION_PANIC: Self = Self(u64::MAX - 2);

    pub(crate) fn is_poison(&self) -> bool {
        *self == Self::POISON
    }

    pub(crate) fn is_poison_cancel(&self) -> bool {
        *self == Self::POISON_CANCEL
    }

    pub(crate) fn is_completion_panic(&self) -> bool {
        *self == Self::COMPLETION_PANIC
    }
}

/// A submission ticket represents a permit to submit an operation to the io_uring submission queue, acting as a backpressure mechanism to prevent having to block using `io_uring_enter`.
/// The ticket must be held for the duration of the operation, as when it is dropped, the ticket is returned to the submission queue. Since it is used as the user_data field for cancelling, it must not be given to outside code until the kernel has acknowledged the operation.
pub(crate) struct SubmissionTicket {
    id: SubmissionTicketId,
    id_tx: crossfire::MTx<crossfire::mpmc::Array<SubmissionTicketId>>,
}

impl std::fmt::Debug for SubmissionTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubmissionTicket {{ id: {:?} }}", self.id.0)
    }
}

impl SubmissionTicket {
    pub(crate) fn id(&self) -> SubmissionTicketId {
        self.id
    }
}

impl Drop for SubmissionTicket {
    fn drop(&mut self) {
        if self.id.is_poison() || self.id.is_completion_panic() || self.id.is_poison_cancel() {
            return;
        }
        let _ = self.id_tx.send(self.id);
    }
}

/// A queue of submission tickets.
#[derive(Debug)]
pub(crate) struct SubmissionTicketQueue {
    /// Original capacity of the queue.
    capacity: usize,
    id_tx: crossfire::MTx<crossfire::mpmc::Array<SubmissionTicketId>>,
    id_async_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<SubmissionTicketId>>,
    id_rx: crossfire::MRx<crossfire::mpmc::Array<SubmissionTicketId>>,
    // /// Inner state of the queue.
    // state: Arc<Mutex<SubmissionTicketQueueState>>,
    // /// Condvar for notifying that a ticket is available.
    // condvar: Arc<Condvar>,
}

impl SubmissionTicketQueue {
    fn new(size: usize, starting_id: u64) -> Self {
        let (id_tx, id_async_rx) = crossfire::mpmc::bounded_blocking_async(size);
        let id_rx = id_async_rx.clone().into_blocking();
        for i in starting_id..starting_id + size as u64 {
            id_tx.send(SubmissionTicketId(i)).unwrap();
        }
        Self {
            capacity: size,
            id_tx,
            id_async_rx,
            id_rx,
        }
    }

    #[allow(unused)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Create new submission ticket queues with the given sizes. The queues are pre-populated with the given number of tickets starting with 1,
    /// and the total number of tickets across all ticket queues must not exceed the length of the io_uring submission queue.
    /// Panics if the total number of tickets exceeds numeric bounds.
    pub(crate) fn new_multiple(sizes: &[usize]) -> Vec<Self> {
        let mut starting_id = 0u64;
        let mut queues = Vec::with_capacity(sizes.len());
        for size in sizes {
            queues.push(Self::new(*size, starting_id));
            starting_id += *size as u64;
        }
        queues
    }

    /// Request a submission ticket. If the queue is empty, the caller will block until a ticket is available.
    pub(crate) fn request_submission_ticket(&self) -> SubmissionTicket {
        let id = self.id_rx.recv().unwrap();
        SubmissionTicket {
            id,
            id_tx: self.id_tx.clone(),
        }
    }

    /// Attempt to request a submission ticket. If the queue is empty, `Poll::Pending` is returned.
    pub(crate) fn poll_submission_ticket(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<SubmissionTicket> {
        let mut recv_future = self.id_async_rx.recv();
        match Pin::new(&mut recv_future).poll(context) {
            Poll::Ready(Ok(id)) => Poll::Ready(SubmissionTicket {
                id,
                id_tx: self.id_tx.clone(),
            }),
            Poll::Ready(Err(e)) => {
                panic!("failed to receive submission ticket: {:?}", e);
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
