use crate::client::pending_io::PendingIoDebuggingEvent;
use crossbeam_channel::TryRecvError;

/// The submission ticket ID, which may be used as the user_data field/entry ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct SubmissionTicketId(pub(crate) u64);

/// A submission ticket represents a permit to submit an operation to the io_uring submission queue, acting as a backpressure mechanism to prevent having to block using `io_uring_enter`.
/// The ticket must be held for the duration of the operation, as when it is dropped, the ticket is returned to the submission queue. Since it is used as the user_data field for cancelling, it must not be given to outside code until the kernel has acknowledged the operation.
#[derive(Debug)]
pub(crate) struct SubmissionTicket {
    id: SubmissionTicketId,
    return_tx: crossbeam_channel::Sender<SubmissionTicketId>,
}

impl SubmissionTicket {
    pub(crate) fn id(&self) -> SubmissionTicketId {
        self.id.clone()
    }
}

impl Drop for SubmissionTicket {
    fn drop(&mut self) {
        let _ = self.return_tx.send(self.id.clone());
    }
}

/// A queue of submission tickets.
#[derive(Debug)]
pub(crate) struct SubmissionTicketQueue {
    /// Channel for receiving submission tickets.
    submission_ticket_rx: crossbeam_channel::Receiver<SubmissionTicketId>,
    /// Channel for returning submission tickets to the submission queue.
    submission_ticket_tx: crossbeam_channel::Sender<SubmissionTicketId>,
}

impl SubmissionTicketQueue {
    /// Create new submission ticket queues with the given sizes. The queues are pre-populated with the given number of tickets starting with 1,
    /// and the total number of tickets across all ticket queues must not exceed the length of the io_uring submission queue.
    pub(crate) fn new_multiple(sizes: &[usize]) -> Vec<Self> {
        let mut starting_id = 0u64;
        let mut queues = Vec::new();
        for size in sizes {
            let (submission_ticket_tx, submission_ticket_rx) =
                crossbeam_channel::bounded::<SubmissionTicketId>(*size);
            for i in 0..*size {
                submission_ticket_tx
                    .send(SubmissionTicketId(starting_id + i as u64))
                    .unwrap();
            }
            starting_id += *size as u64;
            queues.push(Self {
                submission_ticket_rx,
                submission_ticket_tx,
            });
        }
        queues
    }

    /// Request a submission ticket from the queue.
    pub(crate) fn request_submission_ticket(
        &self,
        debug_event_tx: Option<&tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    ) -> SubmissionTicket {
        // Attempt to receive a ticket without blocking. When it would block, emit a debugging event for testing.
        match self.submission_ticket_rx.try_recv() {
            Ok(id) => {
                if let Some(debug_event_tx) = debug_event_tx {
                    let _ = debug_event_tx.send(PendingIoDebuggingEvent::SubmissionTicketGranted);
                }
                return SubmissionTicket {
                    id,
                    return_tx: self.submission_ticket_tx.clone(),
                };
            }
            Err(TryRecvError::Empty) => {
                if let Some(debug_event_tx) = debug_event_tx {
                    let _ =
                        debug_event_tx.send(PendingIoDebuggingEvent::NeedWaitForSubmissionTicket);
                }
            }
            Err(TryRecvError::Disconnected) => {
                panic!("submission ticket queue is disconnected");
            }
        };
        // Fall back to blocking recv.
        let ticket_id = self
            .submission_ticket_rx
            .recv()
            .expect("sender is owned by the submission queue");
        if let Some(debug_event_tx) = debug_event_tx {
            let _ = debug_event_tx.send(PendingIoDebuggingEvent::SubmissionTicketGranted);
        }
        SubmissionTicket {
            id: ticket_id,
            return_tx: self.submission_ticket_tx.clone(),
        }
    }
}

/// A completion ticket represent a permit to perform a blocking read of the completion queue using `io_uring_enter`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CompletionTicket();

#[derive(Debug)]
pub(crate) struct CompletionTicketSubmitter {
    completion_ticket_tx: crossbeam_channel::Sender<CompletionTicket>,
}

impl CompletionTicketSubmitter {
    pub(crate) fn grant_completion_ticket(&self) {
        self.completion_ticket_tx
            .send(CompletionTicket())
            .expect("completion thread must wait until submission thread exits");
    }
}

/// A queue of completion tickets.
#[derive(Debug)]
pub(crate) struct CompletionTicketQueue {
    /// Channel for receiving completion tickets.
    completion_ticket_rx: crossbeam_channel::Receiver<CompletionTicket>,
}

impl CompletionTicketQueue {
    /// Request one or more completion tickets. One completion ticket grants one permit to perform a blocking read using io_uring_enter. If `None` is returned, the completion queue is empty, the caller can exit immediately.
    pub(crate) fn request_completion_tickets(&self) -> Option<usize> {
        let _ticket = self.completion_ticket_rx.recv().ok()?;
        // Prevent potential unbounded bugs by limiting the number of tickets that can be received.
        let received = 1 + self
            .completion_ticket_rx
            .try_iter()
            .take(1048576 - 1)
            .count();
        Some(received)
    }
}

pub(crate) fn completion_ticket_pair() -> (CompletionTicketSubmitter, CompletionTicketQueue) {
    let (completion_ticket_tx, completion_ticket_rx) =
        crossbeam_channel::unbounded::<CompletionTicket>();
    (
        CompletionTicketSubmitter {
            completion_ticket_tx,
        },
        CompletionTicketQueue {
            completion_ticket_rx,
        },
    )
}
