use async_hybrid_fs::{Client, Permissions, UringCfg};
use criterion::{Criterion, criterion_group, criterion_main};
use nix::fcntl::OFlag;
use std::cmp;
use std::io::Read;
use std::path::Path;
use tokio::io::AsyncReadExt;
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
    let mut file = File::open(path.as_ref())
        .await
        .expect("failed to open file");
    let mut buffer = vec![0; size];
    let mut size_left = size;
    while size_left > 0 {
        let to_read = cmp::min(size, size_left);
        let bytes_read = file
            .read(&mut buffer[..to_read])
            .await
            .expect("failed to read");
        size_left -= bytes_read;
    }
}

async fn read_blocking(path: impl AsRef<Path>, size: usize) {
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
    let sizes = [4, 8, 16, 32];
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    for size in sizes {
        c.bench_function(&format!("read_dev_zero_hybrid_{}KB", size), |b| {
            // Note: io_uring client initialization takes a long time.
            b.to_async(Runtime::new().unwrap())
                .iter(|| read_hybrid(&client, "/dev/zero", size * 1024))
        });
        c.bench_function(&format!("read_dev_zero_tokio_{}KB", size), |b| {
            b.to_async(Runtime::new().unwrap())
                .iter(|| read_tokio("/dev/zero", size * 1024))
        });
        c.bench_function(&format!("read_dev_zero_blocking_{}KB", size), |b| {
            b.iter(|| read_blocking("/dev/zero", size * 1024))
        });
    }
}

fn read_dev_urandom_benchmark(c: &mut Criterion) {
    let sizes = [4, 8, 16, 32];
    let client = Client::build(UringCfg::default()).expect("failed to build client");

    for size in sizes {
        c.bench_function(&format!("read_dev_urandom_hybrid_{}KB", size), |b| {
            // Note: io_uring client initialization takes a long time.
            b.to_async(Runtime::new().unwrap())
                .iter(|| read_hybrid(&client, "/dev/urandom", size * 1024))
        });
        c.bench_function(&format!("read_dev_urandom_tokio_{}KB", size), |b| {
            b.to_async(Runtime::new().unwrap())
                .iter(|| read_tokio("/dev/urandom", size * 1024))
        });
        c.bench_function(&format!("read_dev_urandom_blocking_{}KB", size), |b| {
            b.iter(|| read_blocking("/dev/urandom", size * 1024))
        });
    }
}

criterion_group!(benches, read_dev_zero_benchmark, read_dev_urandom_benchmark);
criterion_main!(benches);
