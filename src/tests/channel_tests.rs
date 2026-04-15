use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use dashmap::DashMap;

use crate::client::ticketing::{
    SubmissionTicket, SubmissionTicketId, SubmissionTicketQueue, completion_ticket_pair,
};
// The request pipeline currently consists of 5 channels/barriers, which consumes extra time if they block:
// - A crossbeam channel for the caller to submit commands to the backpressure queues.
// - A mutex and condvar pair for implementing the ticket queue.
// - A crossbeam channel for sending commands to the submission thread.
// - A semaphore-like structure for implementing the completion ticket queue.
// - A tokio::sync::oneshot channel for sending the final operation result.

#[test_log::test]
fn test_crossbeam_channel_blocking_latency() {
    let mut durations = Vec::new();

    for _ in 0..100 {
        let (tx, rx) = crossbeam_channel::bounded::<Instant>(1);
        let thread_2 = thread::spawn(move || {
            let start_time = rx.recv().unwrap();
            let duration = start_time.elapsed();
            duration
        });
        let thread_1 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let start_time = Instant::now();
            tx.send(start_time).unwrap();
        });
        thread_1.join().unwrap();
        let duration = thread_2.join().unwrap();
        durations.push(duration);
    }
    log::info!(
        "crossbeam channel average latency: {:?}",
        durations.iter().sum::<Duration>() / durations.len() as u32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_log::test]
async fn test_tokio_oneshot_channel_blocking_latency() {
    let mut durations = Vec::new();

    for _ in 0..100 {
        let (tx, rx) = tokio::sync::oneshot::channel::<Instant>();
        let thread_2 = tokio::task::spawn(async move {
            let start_time = rx.await.unwrap();
            let duration = start_time.elapsed();
            duration
        });
        let thread_1 = tokio::task::spawn(async move {
            thread::sleep(Duration::from_millis(20));
            let start_time = Instant::now();
            tx.send(start_time).unwrap();
        });
        thread_1.await.unwrap();
        let duration = thread_2.await.unwrap();
        durations.push(duration);
    }
    log::info!(
        "tokio oneshot channel average latency: {:?}",
        durations.iter().sum::<Duration>() / durations.len() as u32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[test_log::test]
async fn test_architecture_blocking_latency() {
    let mut durations = Vec::new();
    let mapping = Arc::new(DashMap::<
        SubmissionTicketId,
        (
            Instant,
            tokio::sync::oneshot::Sender<Instant>,
            SubmissionTicket,
        ),
    >::new());
    let (tx_1, rx_1) =
        crossbeam_channel::unbounded::<(Instant, tokio::sync::oneshot::Sender<Instant>)>();
    let (tx_2, rx_2) = crossbeam_channel::bounded::<(
        Instant,
        tokio::sync::oneshot::Sender<Instant>,
        SubmissionTicket,
    )>(16384);
    // This channel signals the completion queue.
    let (completion_ticket_submitter, completion_ticket_queue) = completion_ticket_pair();
    // This channel simulates the io_uring processor.
    let (tx_3, rx_3) = crossbeam_channel::bounded::<SubmissionTicketId>(16384);
    let queue = SubmissionTicketQueue::new_multiple(&[16384]).pop().unwrap();

    // Represents the completion thread
    let thread_3 = tokio::task::spawn_blocking({
        let mapping = mapping.clone();
        move || {
            loop {
                let count = completion_ticket_queue.request_completion_tickets();
                let count = match count {
                    None => break,
                    Some(count) => count,
                };
                for _ in 0..count {
                    let id = match rx_3.recv() {
                        Ok(msg) => msg,
                        Err(crossbeam_channel::RecvError) => break,
                    };
                    let (_, (start_time, final_tx, _ticket)) = mapping.remove(&id).unwrap();
                    final_tx.send(start_time).unwrap();
                }
            }
        }
    });
    // Represents the submission thread.
    let thread_2 = tokio::task::spawn_blocking(move || {
        loop {
            let msg = match rx_2.recv() {
                Ok(msg) => msg,
                Err(crossbeam_channel::RecvError) => break,
            };
            let id = msg.2.id();
            mapping.insert(id, msg);
            completion_ticket_submitter.grant_completion_ticket();
            tx_3.send(id).unwrap();
        }
    });
    // Represents the backpressure thread
    let thread_1 = tokio::task::spawn_blocking(move || {
        loop {
            let (start_time, final_tx) = match rx_1.recv() {
                Ok(msg) => msg,
                Err(crossbeam_channel::RecvError) => break,
            };
            let ticket = queue.request_submission_ticket(None);
            tx_2.send((start_time, final_tx, ticket)).unwrap();
        }
    });
    for _ in 0..2000 {
        let (final_tx, final_rx) = tokio::sync::oneshot::channel::<Instant>();
        tx_1.send((Instant::now(), final_tx)).unwrap();
        let instant = final_rx.await.unwrap();
        let duration = instant.elapsed();
        durations.push(duration);
    }
    drop(tx_1);

    thread_1.await.unwrap();
    thread_2.await.unwrap();
    thread_3.await.unwrap();

    let lowest = durations
        .iter()
        .enumerate()
        .min_by_key(|(_, duration)| *duration)
        .map(|(index, duration)| (index, *duration))
        .unwrap();
    let highest = durations
        .iter()
        .enumerate()
        .max_by_key(|(_, duration)| *duration)
        .map(|(index, duration)| (index, *duration))
        .unwrap();
    durations.sort_by_key(|duration| *duration);
    let median = if durations.len() % 2 == 0 {
        (durations[durations.len() / 2 - 1] + durations[durations.len() / 2]) / 2
    } else {
        durations[durations.len() / 2]
    };
    let mean = durations.iter().sum::<Duration>() / durations.len() as u32;
    log::info!("lowest latency: {:?}", lowest);
    log::info!("mean latency: {:?}", mean);
    log::info!("median latency: {:?}", median);
    // Uncontended channels consume a lot of time.
    log::info!("highest latency: {:?}", highest);
}
