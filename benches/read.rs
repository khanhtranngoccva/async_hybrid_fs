use async_hybrid_fs::{Client, Permissions, UringCfg};
use criterion::{Criterion, criterion_group, criterion_main};
use nix::fcntl::OFlag;
use std::cmp;
use std::io::Read;
use std::path::Path;
use tokio::{fs::File, runtime::Runtime};

async fn read_hybrid(client: &Client, path: impl AsRef<Path>, size: usize) {
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
    let mut file = File::from_std(std::fs::File::from(fd));
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
}

async fn read_tokio(path: impl AsRef<Path>, size: usize) {
    // Batching blocking operations reduces overhead.
    let path_owned = path.as_ref().to_owned();
    tokio::task::spawn_blocking(move || read_blocking(path_owned, size))
        .await
        .expect("failed to join task");
}

async fn read_hybrid_batched(client: &Client, path: impl AsRef<Path>, size: usize, count: usize) {
    let mut futures = Vec::new();
    for _ in 0..count {
        let future = read_hybrid(client, path.as_ref(), size);
        futures.push(future);
    }
    futures::future::join_all(futures).await;
}

async fn read_tokio_batched(path: impl AsRef<Path>, size: usize, count: usize) {
    let mut futures = Vec::new();
    for _ in 0..count {
        let future = read_tokio(path.as_ref(), size);
        futures.push(future);
    }
    futures::future::join_all(futures).await;
}

fn read_blocking_batched(path: impl AsRef<Path>, size: usize, count: usize) {
    for _ in 0..count {
        read_blocking(path.as_ref(), size);
    }
}

fn read_blocking(path: impl AsRef<Path>, size: usize) {
    let mut file = std::fs::File::open(path.as_ref()).expect("failed to open file");
    let mut buffer = vec![0; size];
    let mut size_left = size;
    while size_left > 0 {
        let to_read = cmp::min(size, size_left);
        let bytes_read = file.read(&mut buffer[..to_read]).expect("failed to read");
        size_left -= bytes_read;
    }
}

fn read_dev_zero_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    c.bench_function("read::dev_zero::hybrid", |b| {
        // Note: io_uring client initialization takes a long time.
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_hybrid(&client, "/dev/zero", 1024))
    });
    c.bench_function("read::dev_zero::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_tokio("/dev/zero", 1024))
    });
    c.bench_function("read::dev_zero::blocking", |b| {
        b.iter(|| read_blocking("/dev/zero", 1024))
    });
}

fn read_dev_urandom_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    c.bench_function("read::dev_urandom::hybrid", |b| {
        // Note: io_uring client initialization takes a long time.
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_hybrid(&client, "/dev/urandom", 1024))
    });
    c.bench_function("read::dev_urandom::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_tokio("/dev/urandom", 1024))
    });
    c.bench_function("read::dev_urandom::blocking", |b| {
        b.iter(|| read_blocking("/dev/urandom", 1024))
    });
}

fn read_dev_zero_batched_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");
    c.bench_function("read::dev_zero::batched::hybrid", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_hybrid_batched(&client, "/dev/urandom", 1024, 2000))
    });
    c.bench_function("read::dev_zero::batched::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_tokio_batched("/dev/urandom", 1024, 2000))
    });
    c.bench_function("read::dev_zero::batched::blocking", |b| {
        b.iter(|| read_blocking_batched("/dev/urandom", 1024, 2000))
    });
}

fn read_dev_urandom_batched_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");
    c.bench_function("read::dev_urandom::batched::hybrid", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_hybrid_batched(&client, "/dev/urandom", 1024, 2000))
    });
    c.bench_function("read::dev_urandom::batched::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read_tokio_batched("/dev/urandom", 1024, 2000))
    });
    c.bench_function("read::dev_urandom::batched::blocking", |b| {
        b.iter(|| read_blocking_batched("/dev/urandom", 1024, 2000))
    });
}

criterion_group!(
    benches,
    read_dev_zero_benchmark,
    read_dev_urandom_benchmark,
    read_dev_zero_batched_benchmark,
    read_dev_urandom_batched_benchmark,
);
criterion_main!(benches);
