use super::completion;
use super::pending_io::uring::{PendingMap, UringPendingIoStatus, UringPendingIoSubmitter};
use super::ticketing::{SubmissionTicketId, SubmissionTicketQueue};
use crate::helpers;
use io_uring::IoUring;
use std::collections::VecDeque;
use std::sync::Arc;

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
    // Drop handler. Should not allocate memory from the heap
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
        helpers::retry_on_eintr(|| self.uring.submitter().submit())
            .expect("failed to submit last batch of cancellation operations");
        // Poison pill is guaranteed to be the last item, after all operations are done or cancelled. The IO_DRAIN flag ensures that the cancellation have all finished.
        let entry = io_uring::opcode::Nop::new()
            .build()
            .user_data(SubmissionTicketId::POISON.0)
            .flags(io_uring::squeue::Flags::IO_DRAIN);
        while unsafe { submission.push(&entry) }.is_err() {
            // The queue is in latest state, so if we cannot submit, we can safely wait for an event.
            helpers::retry_on_eintr(|| self.uring.submit_and_wait(1))
                .expect("failed to wait for empty submission slot");
            submission.sync();
        }
        submission.sync();
        helpers::retry_on_eintr(|| self.uring.submitter().submit())
            .expect("failed to submit poison entry");
    }
}

pub(crate) enum InterruptCommand {
    SubmissionCleanup,
    SubmissionPanic,
    CompletionPanic,
}

/// Thread for batching the io_uring_enter syscall to flush entries to the io_uring instance.
pub(crate) fn submission_thread(
    ring: Arc<IoUring>,
    pending_map: Arc<PendingMap>,
    command_receiver: crossbeam_channel::Receiver<UringPendingIoSubmitter>,
    ticket_queue: SubmissionTicketQueue,
    interrupt: crossbeam_channel::Receiver<InterruptCommand>,
) {
    let _submission_dropper = SubmissionDropper::new(ring.clone(), pending_map.clone());
    let mut submission = unsafe { ring.submission_shared() };
    // Set up reserves for batching. If either the command queue or both ticket queues run out, we need to wait.
    let mut normal_command_queue =
        VecDeque::with_capacity(ticket_queue.normal_operation_capacity());
    let mut normal_ticket_queue = VecDeque::with_capacity(ticket_queue.normal_operation_capacity());
    let mut cancel_command_queue =
        VecDeque::with_capacity(ticket_queue.cancel_operation_capacity());
    let mut cancel_ticket_queue = VecDeque::with_capacity(ticket_queue.cancel_operation_capacity());
    let total_capacity = ticket_queue.total_capacity();
    let mut ll_entries = VecDeque::with_capacity(total_capacity);
    let mut command_submitters = VecDeque::with_capacity(total_capacity);
    // Low-level receiver is necessary for multiplexing
    let ticket_receiver = ticket_queue.receiver();
    loop {
        crossbeam_channel::select! {
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
            recv(ticket_receiver) -> id => {
                match id {
                    Ok(id) => {
                        let ticket = ticket_queue.create_ticket(id);
                        if ticket_queue.is_cancel_ticket(&ticket) {
                            cancel_ticket_queue.push_back(ticket);
                        } else {
                            normal_ticket_queue.push_back(ticket);
                        }
                    },
                    Err(crossbeam_channel::RecvError) => break,
                }
            }
            recv(command_receiver) -> item => {
                match item {
                    Ok(item) => {
                        if item.is_cancel() {
                            cancel_command_queue.push_back(item);
                        } else {
                            normal_command_queue.push_back(item);
                        }
                    },
                    Err(crossbeam_channel::RecvError) => break,
                }
            }
        };
        // Receive additional items from the channels if possible
        while let Ok(additional_item) = command_receiver.try_recv() {
            if additional_item.is_cancel() {
                cancel_command_queue.push_back(additional_item);
            } else {
                normal_command_queue.push_back(additional_item);
            }
        }
        while let Ok(additional_id) = ticket_receiver.try_recv() {
            let ticket = ticket_queue.create_ticket(additional_id);
            if ticket_queue.is_cancel_ticket(&ticket) {
                cancel_ticket_queue.push_back(ticket);
            } else {
                normal_ticket_queue.push_back(ticket);
            }
        }
        // Generate cancel entries from reserve
        while !cancel_command_queue.is_empty() && !cancel_ticket_queue.is_empty() {
            let command = cancel_command_queue
                .pop_front()
                .expect("cancel command queue should not be empty");
            let ticket = cancel_ticket_queue
                .pop_front()
                .expect("cancel ticket queue should not be empty");
            let ticket_id = ticket.id();
            let entry = command.assign_ticket(Arc::new(ticket));
            if let Some(entry) = entry {
                pending_map.insert(ticket_id, command.filler());
                ll_entries.push_back(entry);
                command_submitters.push_back(command);
            }
        }
        // Generate normal entries from reserve
        while !normal_command_queue.is_empty() && !normal_ticket_queue.is_empty() {
            let command = normal_command_queue
                .pop_front()
                .expect("normal command queue should not be empty");
            let ticket = normal_ticket_queue
                .pop_front()
                .expect("normal ticket queue should not be empty");
            let ticket_id = ticket.id();
            let entry = command.assign_ticket(Arc::new(ticket));
            if let Some(entry) = entry {
                pending_map.insert(ticket_id, command.filler());
                ll_entries.push_back(entry);
                command_submitters.push_back(command);
            }
        }
        // If there are no entries to submit, we resume the waiting.
        if ll_entries.is_empty() {
            continue;
        }
        // Submit entries
        for ll_entry in ll_entries.drain(..) {
            while unsafe { submission.push(&ll_entry) }.is_err() {
                // We need to synchronize the head and tail before retrying because it is stale. However, we do not need to block because the queues restrict the number of active tickets.
                submission.sync();
                continue;
            }
        }
        submission.sync();
        helpers::retry_on_eintr(|| ring.submitter().submit())
            .expect("failed to perform batch submit");
        // Mark entries as submitted
        for submitter in command_submitters.drain(..) {
            submitter.mark_submitted();
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
pub(crate) fn completion_thread(
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
                    helpers::retry_on_eintr(|| ring.submitter().submit_and_wait(1))
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
