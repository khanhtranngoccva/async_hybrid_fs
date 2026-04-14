mod channel_tests;

use crate::{HybridFile, HybridRead, HybridSeek, HybridWrite, fs::OpenOptions};
use nix::sys::time::TimeSpec;
use std::{
    io::{IoSlice, IoSliceMut, SeekFrom},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::fs::File;

#[tokio::test]
async fn is_uring_available() {
    println!(
        "uring available: {}",
        crate::default_client().is_uring_available_and_active()
    );
}

#[tokio::test]
async fn test_hybrid_create_and_read_write() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_all(b"Hello, world!").await.unwrap();
    file.hybrid_flush()
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let mut buffer = Vec::new();
    file.hybrid_read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, b"Hello, world!");
}

#[tokio::test]
async fn test_hybrid_seek() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_all(b"Hello, world!").await.unwrap();
    file.hybrid_flush()
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_seek(SeekFrom::Start(7))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let mut buffer = Vec::new();
    file.hybrid_read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, b"world!");
}

#[tokio::test]
async fn test_hybrid_set_len() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_set_len(10)
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    assert_eq!(
        file.hybrid_metadata()
            .await
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test]
async fn test_hybrid_set_times() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let tnow = TimeSpec::from_duration(now);
    file.set_times(Some(tnow), Some(tnow))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let metadata = <File as HybridFile>::metadata(&file)
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let accessed = metadata.accessed().unwrap();
    assert!(accessed.duration_since(UNIX_EPOCH).unwrap() == now);
    let modified = metadata.modified().unwrap();
    assert!(modified.duration_since(UNIX_EPOCH).unwrap() == now);
}

#[tokio::test]
async fn test_hybrid_read_write_vectored() {
    // TODO: test may be flaky if reads are short (which is very rare)
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_vectored(&[IoSlice::new(b"Hello, world!")])
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_flush()
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let mut buffer = vec![0u8; 128];
    let slice = IoSliceMut::new(&mut buffer);
    let bytes_read = file
        .hybrid_read_vectored(&mut [slice])
        .await
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    assert_eq!(&buffer[..bytes_read], b"Hello, world!");
}
