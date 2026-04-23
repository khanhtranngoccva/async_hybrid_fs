use std::{num::NonZero, thread};

use async_hybrid_fs::{Client, UringCfg};
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::{self, Runtime};

use crate::scenarios::read;

mod scenarios;

fn read_dev_zero_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    c.bench_function("read::dev_zero::hybrid", |b| {
        // Note: io_uring client initialization takes a long time.
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_hybrid(&client, "/dev/zero", 1024))
    });
    c.bench_function("read::dev_zero::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_tokio("/dev/zero", 1024))
    });
    c.bench_function("read::dev_zero::blocking", |b| {
        b.iter(|| read::read_blocking("/dev/zero", 1024))
    });
}

fn read_dev_urandom_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    c.bench_function("read::dev_urandom::hybrid", |b| {
        // Note: io_uring client initialization takes a long time.
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_hybrid(&client, "/dev/urandom", 1024))
    });
    c.bench_function("read::dev_urandom::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_tokio("/dev/urandom", 1024))
    });
    c.bench_function("read::dev_urandom::blocking", |b| {
        b.iter(|| read::read_blocking("/dev/urandom", 1024))
    });
}

fn read_dev_zero_batched_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");
    c.bench_function("read::dev_zero::batched::hybrid", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_hybrid_batched(&client, "/dev/urandom", 1024, 4000))
    });
    c.bench_function("read::dev_zero::batched::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_tokio_batched("/dev/urandom", 1024, 4000))
    });
    c.bench_function("read::dev_zero::batched::blocking", |b| {
        b.iter(|| read::read_blocking_batched("/dev/urandom", 1024, 4000))
    });
}

fn read_dev_urandom_batched_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");
    c.bench_function("read::dev_urandom::batched::hybrid", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_hybrid_batched(&client, "/dev/urandom", 1024, 4000))
    });
    c.bench_function("read::dev_urandom::batched::tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| read::read_tokio_batched("/dev/urandom", 1024, 4000))
    });
    c.bench_function("read::dev_urandom::batched::blocking", |b| {
        b.iter(|| read::read_blocking_batched("/dev/urandom", 1024, 4000))
    });
}

fn read_dev_zero_with_contention_benchmark(c: &mut Criterion) {
    let client = Client::build(UringCfg::default()).expect("failed to build client");
    c.bench_function("read::dev_zero::with_contention::hybrid", |b| {
        b.to_async(Runtime::new().unwrap()).iter_batched(
            || {},
            // This should lead to queue overflowing
            |_| read::read_hybrid_batched(&client, "/dev/zero", 1024, 100000),
            // Avoid file descriptor exhaustion
            criterion::BatchSize::NumIterations(1),
        )
    });
    let available_parallelism = thread::available_parallelism()
        .unwrap_or(NonZero::new(8usize).unwrap())
        .get();
    let multiclients = (0..available_parallelism)
        .map(|_| Client::build(UringCfg::default()).expect("failed to build client"))
        .collect::<Vec<_>>();
    c.bench_function("read::dev_zero::with_contention::hybrid::multi", |b| {
        b.to_async(
            runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(multiclients.len())
                .build()
                .unwrap(),
        )
        .iter_batched(
            || {},
            // This should lead to queue overflowing
            |_| read::read_hybrid_multi(&multiclients, "/dev/zero", 1024, 100000),
            // Avoid file descriptor exhaustion
            criterion::BatchSize::NumIterations(1),
        )
    });
    c.bench_function(
        "read::dev_zero::with_contention::hybrid::default_round_robin",
        |b| {
            b.to_async(
                runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(available_parallelism)
                    .build()
                    .unwrap(),
            )
            .iter_batched(
                || {},
                |_| read::read_hybrid_default_round_robin("/dev/zero", 1024, 100000),
                // Avoid file descriptor exhaustion
                criterion::BatchSize::NumIterations(1),
            )
        },
    );
}

criterion_group!(
    benches,
    read_dev_zero_benchmark,
    read_dev_urandom_benchmark,
    read_dev_zero_batched_benchmark,
    read_dev_urandom_batched_benchmark,
    read_dev_zero_with_contention_benchmark,
);
criterion_main!(benches);
