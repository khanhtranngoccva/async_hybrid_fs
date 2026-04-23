// Different crates use different functions.
#[allow(unused)]
use async_hybrid_fs::{Client, Permissions, default_client};
use nix::fcntl::OFlag;
use std::io::Read;
use std::num::NonZero;
use std::path::Path;
use std::{cmp, thread};
use tokio::runtime::Handle;

pub async fn read_hybrid(client: &Client, path: impl AsRef<Path>, size: usize) {
    let fd = client
        .open_path(
            path.as_ref(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
        .completion()
        .expect("no completion future returned")
        .await
        .expect("failed to open file");
    let mut file = std::fs::File::from(fd);
    let mut buffer = vec![0; size];
    let mut size_left = size;
    while size_left > 0 {
        let to_read = cmp::min(size, size_left);
        let bytes_read = client
            .read(&mut file, &mut buffer[..to_read])
            .completion()
            .expect("no completion future returned")
            .await
            .expect("failed to read");
        size_left -= bytes_read;
    }
    client
        .close(file)
        .completion()
        .expect("no completion future returned")
        .await
        .expect("failed to close file");
}

pub async fn read_tokio(path: impl AsRef<Path>, size: usize) {
    // Batching blocking operations reduces overhead.
    let path_owned = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || read_blocking(path_owned, size))
        .await
        .expect("failed to join task");
}

pub async fn read_hybrid_batched(
    client: &Client,
    path: impl AsRef<Path>,
    size: usize,
    count: usize,
) {
    let mut current_count = count;
    let batch_size = size;
    // let batch_size = 16384 - 512usize;
    while current_count > 0 {
        let current_batch = current_count.min(batch_size);
        let mut futures = Vec::with_capacity(current_batch);
        for _ in 0..current_batch {
            let future = read_hybrid(client, path.as_ref(), size);
            futures.push(future);
        }
        futures::future::join_all(futures).await;
        current_count -= current_batch;
    }
}

pub async fn read_hybrid_multi(
    clients: &[Client],
    path: impl AsRef<Path>,
    size: usize,
    count: usize,
) {
    let even_thread_count = count / clients.len();
    let extra_count = even_thread_count + 1;
    let remainder = count % clients.len();
    let (_, futures) = unsafe {
        async_scoped::TokioScope::scope_and_collect(move |scope| {
            for idx in 0..clients.len() {
                let client = &clients[idx];
                let path_owned = path.as_ref().to_owned();
                scope.spawn(read_hybrid_batched(
                    client,
                    path_owned,
                    size,
                    if idx < remainder {
                        extra_count
                    } else {
                        even_thread_count
                    },
                ));
            }
        })
    }
    .await;
    for future in futures {
        future.expect("future failed to join");
    }
}

pub async fn read_hybrid_default(path: impl AsRef<Path>, size: usize) {
    let fd = default_client()
        .open_path(
            path.as_ref(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
        .completion()
        .expect("no completion future returned")
        .await
        .expect("failed to open file");
    let mut file = std::fs::File::from(fd);
    let mut buffer = vec![0; size];
    let mut size_left = size;
    while size_left > 0 {
        let to_read = cmp::min(size, size_left);
        let bytes_read = default_client()
            .read(&mut file, &mut buffer[..to_read])
            .completion()
            .expect("no completion future returned")
            .await
            .expect("failed to read");
        size_left -= bytes_read;
    }
    default_client()
        .close(file)
        .completion()
        .expect("no completion future returned")
        .await
        .expect("failed to close file");
}

pub async fn read_hybrid_default_batched(path: impl AsRef<Path>, size: usize, count: usize) {
    let mut futures = Vec::with_capacity(count);
    for _ in 0..count {
        futures.push(read_hybrid_default(path.as_ref(), size));
    }
    futures::future::join_all(futures).await;
}

pub async fn read_hybrid_default_round_robin(path: impl AsRef<Path>, size: usize, count: usize) {
    let tasks = Handle::current().metrics().num_workers();
    let even_task_item_count = count / tasks;
    let extra_task_item_count = even_task_item_count + 1;
    let remainder = count % tasks;

    let mut futures = Vec::with_capacity(count);
    for idx in 0..tasks {
        let path_owned = path.as_ref().to_owned();
        let task = tokio::task::spawn(async move {
            let local_count = if idx < remainder {
                extra_task_item_count
            } else {
                even_task_item_count
            };
            read_hybrid_default_batched(&path_owned, size, local_count).await;
        });
        futures.push(task);
    }
    futures::future::join_all(futures).await;
}

pub async fn read_tokio_batched(path: impl AsRef<Path>, size: usize, count: usize) {
    let mut futures = Vec::with_capacity(count);
    for _ in 0..count {
        let future = read_tokio(path.as_ref(), size);
        futures.push(future);
    }
    futures::future::join_all(futures).await;
}

pub fn read_blocking_batched(path: impl AsRef<Path>, size: usize, count: usize) {
    let mut threads = Vec::with_capacity(NUM_THREADS);
    const NUM_THREADS: usize = 4;
    let num_ops = count / NUM_THREADS;
    let mut num_ops_per_thread = Vec::with_capacity(NUM_THREADS);
    for _ in 0..(NUM_THREADS - 1) {
        num_ops_per_thread.push(num_ops);
    }
    num_ops_per_thread.push(count - num_ops_per_thread.iter().sum::<usize>());

    for num_ops in num_ops_per_thread {
        let path_owned = path.as_ref().to_owned();
        threads.push(std::thread::spawn(move || {
            for _ in 0..num_ops {
                read_blocking(&path_owned, size);
            }
        }));
    }

    for thread in threads {
        thread.join().expect("failed to join thread");
    }
}

pub fn read_blocking(path: impl AsRef<Path>, size: usize) {
    let mut file = std::fs::File::open(path.as_ref()).expect("failed to open file");
    let mut buffer = vec![0; size];
    let mut size_left = size;
    while size_left > 0 {
        let to_read = cmp::min(size, size_left);
        let bytes_read = file.read(&mut buffer[..to_read]).expect("failed to read");
        size_left -= bytes_read;
    }
}
