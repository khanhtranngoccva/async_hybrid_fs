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
#[cfg_attr(feature = "_low-level", visibility::make(pub))]
pub struct SubmissionTicket {
    id: SubmissionTicketId,
    id_tx: crossbeam_channel::Sender<SubmissionTicketId>,
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
    normal_op_cap: usize,
    cancel_op_cap: usize,
    id_tx: crossbeam_channel::Sender<SubmissionTicketId>,
    id_rx: crossbeam_channel::Receiver<SubmissionTicketId>,
}

impl SubmissionTicketQueue {
    pub(crate) fn new(normal_op_cap: usize, cancel_op_cap: usize, starting_id: u64) -> Self {
        let size = normal_op_cap + cancel_op_cap;
        let (id_tx, id_rx) = crossbeam_channel::bounded::<SubmissionTicketId>(size);
        for i in starting_id..starting_id + size as u64 {
            id_tx.send(SubmissionTicketId(i)).expect("queue is full");
        }
        Self {
            normal_op_cap,
            cancel_op_cap,
            id_tx,
            id_rx,
        }
    }

    /// Clones the queue's receiver struct. This is required for crossbeam multiplexing
    pub(crate) fn receiver(&self) -> crossbeam_channel::Receiver<SubmissionTicketId> {
        self.id_rx.clone()
    }

    /// Creates a submission ticket with the given ID.
    pub(crate) fn create_ticket(&self, id: SubmissionTicketId) -> SubmissionTicket {
        SubmissionTicket {
            id,
            id_tx: self.id_tx.clone(),
        }
    }

    /// Retrieve the total capacity.
    pub(crate) fn total_capacity(&self) -> usize {
        self.normal_op_cap + self.cancel_op_cap
    }

    /// Retrieve the normal operation capacity.
    pub(crate) fn normal_operation_capacity(&self) -> usize {
        self.normal_op_cap
    }

    /// Retrieve the cancel operation capacity.
    pub(crate) fn cancel_operation_capacity(&self) -> usize {
        self.cancel_op_cap
    }

    /// Checks if the ticket is a cancel operation.
    pub(crate) fn is_cancel_ticket(&self, ticket: &SubmissionTicket) -> bool {
        ticket.id.0 >= self.normal_op_cap as u64
    }
}
