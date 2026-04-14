use std::{
    thread,
    time::{Duration, Instant},
};

// The request pipeline currently consists of 5 channels, which consumes extra time if they block:
// - A crossbeam channel for the caller to submit commands to the backpressure queues. 
// - A crossbeam channel for implementing the ticket queue.
// - A crossbeam channel for sending commands to the submission thread.
// - A crossbeam channel for receiving a wait permit.
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
