use std::{
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
};

/// The submission ticket ID, which may be used as the user_data field/entry ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct SubmissionTicketId(pub(crate) u64);

/// A submission ticket represents a permit to submit an operation to the io_uring submission queue, acting as a backpressure mechanism to prevent having to block using `io_uring_enter`.
/// The ticket must be held for the duration of the operation, as when it is dropped, the ticket is returned to the submission queue. Since it is used as the user_data field for cancelling, it must not be given to outside code until the kernel has acknowledged the operation.
pub(crate) struct SubmissionTicket {
    id: SubmissionTicketId,
    state: Arc<Mutex<SubmissionTicketQueueState>>,
    condvar: Arc<Condvar>,
}

impl std::fmt::Debug for SubmissionTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubmissionTicket {{ id: {:?} }}", self.id.0)
    }
}

impl SubmissionTicket {
    pub(crate) fn id(&self) -> SubmissionTicketId {
        self.id.clone()
    }
}

impl Drop for SubmissionTicket {
    fn drop(&mut self) {
        let mut tickets = self.state.lock().unwrap();
        tickets.ids.push(self.id.clone());
        tickets.wake_all();
        self.condvar.notify_one();
    }
}

#[derive(Debug)]
struct SubmissionTicketQueueState {
    /// Internal IDs to assign.
    ids: Vec<SubmissionTicketId>,
    /// Asynchronous wakers to notify when a ticket is available.
    wakers: Vec<Waker>,
}

impl SubmissionTicketQueueState {
    // Some tasks may not poll (cancelled) so we may miss updates. Therefore, we have to notify all wakers. It might be better to simply use oneshot channels instead
    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// A queue of submission tickets.
#[derive(Debug)]
pub(crate) struct SubmissionTicketQueue {
    /// Original capacity of the queue.
    capacity: usize,
    /// Inner state of the queue.
    state: Arc<Mutex<SubmissionTicketQueueState>>,
    /// Condvar for notifying that a ticket is available.
    condvar: Arc<Condvar>,
}

impl SubmissionTicketQueue {
    fn new(size: usize, starting_id: u64) -> Self {
        let ids: Vec<_> = (starting_id..starting_id + size as u64)
            .map(SubmissionTicketId)
            .collect();
        let wakers = Vec::with_capacity(1024);
        let state = SubmissionTicketQueueState { ids, wakers };
        Self {
            capacity: size,
            state: Arc::new(Mutex::new(state)),
            condvar: Arc::new(Condvar::new()),
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
        let mut state = self.state.lock().unwrap();
        while state.ids.is_empty() {
            state = self.condvar.wait(state).unwrap();
        }
        let id = state.ids.pop().unwrap();
        SubmissionTicket {
            id,
            state: self.state.clone(),
            condvar: self.condvar.clone(),
        }
    }

    /// Attempt to request a submission ticket. If the queue is empty, `Poll::Pending` is returned.
    pub(crate) fn poll_submission_ticket(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<SubmissionTicket> {
        let mut state = self.state.lock().unwrap();
        if state.ids.is_empty() {
            state.wakers.push(context.waker().clone());
            return Poll::Pending;
        }
        let id = state.ids.pop().unwrap();
        Poll::Ready(SubmissionTicket {
            id,
            state: self.state.clone(),
            condvar: self.condvar.clone(),
        })
    }
}

#[derive(Debug)]
struct PermitState {
    permits: usize,
    dropped: bool,
}

#[derive(Debug)]
pub(crate) struct PermitSubmitter {
    permit_state: Arc<Mutex<PermitState>>,
    condvar: Arc<Condvar>,
}

impl PermitSubmitter {
    pub(crate) fn grant_permits(&self, count: usize) {
        let mut permit_state = self.permit_state.lock().unwrap();
        permit_state.permits += count;
        self.condvar.notify_all();
    }
}

impl Drop for PermitSubmitter {
    fn drop(&mut self) {
        let mut permit_state = self.permit_state.lock().unwrap();
        permit_state.dropped = true;
        self.condvar.notify_all();
    }
}

/// A queue of completion tickets.
#[derive(Debug)]
pub(crate) struct PermitQueue {
    permit_state: Arc<Mutex<PermitState>>,
    condvar: Arc<Condvar>,
}

impl PermitQueue {
    /// Request one or more permits.
    /// If `None` is returned, the permits are empty, the caller can exit immediately.
    pub(crate) fn request_permits(&self) -> Option<usize> {
        let mut permit_state_guard = self.permit_state.lock().unwrap();
        while permit_state_guard.permits == 0 {
            if permit_state_guard.dropped {
                return None;
            }
            permit_state_guard = self.condvar.wait(permit_state_guard).unwrap();
        }
        let take_count = permit_state_guard.permits.min(1048576);
        permit_state_guard.permits -= take_count;
        Some(take_count)
    }
}

pub(crate) fn permit_pair() -> (PermitSubmitter, PermitQueue) {
    let permit_state = Arc::new(Mutex::new(PermitState {
        permits: 0,
        dropped: false,
    }));
    let condvar = Arc::new(Condvar::new());
    (
        PermitSubmitter {
            permit_state: permit_state.clone(),
            condvar: condvar.clone(),
        },
        PermitQueue {
            permit_state,
            condvar,
        },
    )
}
