use crate::{Client, HybridFile, HybridRead, HybridSeek, HybridWrite, UringCfg, fs::OpenOptions};
use futures::StreamExt;
use nix::sys::time::TimeSpec;
use std::{
    collections::HashSet,
    io::{IoSlice, IoSliceMut, SeekFrom},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{fs::File, runtime::Runtime};

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
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_all(b"Hello, world!").await.unwrap();
    file.hybrid_flush()
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
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
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_all(b"Hello, world!").await.unwrap();
    file.hybrid_flush()
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_seek(SeekFrom::Start(7))
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
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_set_len(10)
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    assert_eq!(
        file.hybrid_metadata()
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
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let tnow = TimeSpec::from_duration(now);
    file.set_times(Some(tnow), Some(tnow))
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let metadata = <File as HybridFile>::metadata(&file)
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
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_write_vectored(&[IoSlice::new(b"Hello, world!")])
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    file.hybrid_flush()
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .open(temp_dir.path().join("test.txt"))
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    let mut buffer = vec![0u8; 128];
    let slice = IoSliceMut::new(&mut buffer);
    let bytes_read = file
        .hybrid_read_vectored(&mut [slice])
        .completion()
        .expect("no completion future returned")
        .await
        .unwrap();
    assert_eq!(&buffer[..bytes_read], b"Hello, world!");
}

#[tokio::test]
async fn test_create_dir_all() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    crate::fs::create_dir_all(temp_dir.path().join("test").join("test2"))
        .await
        .unwrap();
    assert!(temp_dir.path().join("test").join("test2").is_dir());
}

#[tokio::test]
async fn test_read_dir() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut files = HashSet::new();
    for i in 0..10 {
        let filename = format!("test{i}.txt");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join(&filename))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_write_all(b"Hello, world!").await.unwrap();
        drop(file);
        files.insert(filename);
    }
    let mut actual_files = HashSet::new();
    let mut read_dir = crate::fs::read_dir(temp_dir.path()).await.unwrap();
    while let Some(entry) = read_dir.next().await {
        let entry = entry.unwrap();
        actual_files.insert(entry.file_name().to_string_lossy().to_string());
    }
    assert_eq!(actual_files, files);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fcntl() {
    let runtime = Runtime::new().unwrap();
    let task = runtime.spawn(async move {
        let (read_half, _write_half) = std::io::pipe().expect("should be able to create a pipe");
        let client = Client::build(UringCfg::default()).expect("failed to build client");
        let mut registered_read_half = client
            .register_owned(read_half.into())
            .expect("should be able to register file");
        registered_read_half
            .hybrid_set_nonblocking(true)
            .await
            .expect("should be able to set nonblocking");
        registered_read_half
            .hybrid_set_nonblocking(false)
            .await
            .expect("should be able to set nonblocking");
    });
    task.await.unwrap();
    runtime.shutdown_background();
}
