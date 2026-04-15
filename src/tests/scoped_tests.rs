use std::time::{Duration, Instant};

// Takeaway: async_scoped is zero-cost on hot paths.
#[tokio::test]
#[test_log::test]
async fn test_scoped_blocking_overhead() {
    let mut durations = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        let (_, mut futures) = unsafe {
            async_scoped::TokioScope::scope_and_collect(|scope| {
                scope.spawn_blocking(|| {});
            })
            .await
        };
        let future = futures.pop().expect("no future returned");
        future.expect("future failed to join");
        let duration = start.elapsed();
        durations.push(duration);
    }
    log::info!(
        "scoped overhead: {:?}",
        durations.iter().sum::<Duration>() / durations.len() as u32
    );
}

#[tokio::test]
#[test_log::test]
async fn test_tokio_normal_blocking_overhead() {
    let mut durations = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        let t = tokio::task::spawn_blocking(|| {});
        t.await.expect("future failed to join");
        let duration = start.elapsed();
        durations.push(duration);
    }
    log::info!(
        "tokio normal overhead: {:?}",
        durations.iter().sum::<Duration>() / durations.len() as u32
    );
}
