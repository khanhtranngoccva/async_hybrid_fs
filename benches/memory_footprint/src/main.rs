use async_hybrid_fs::ll::{SubmissionTicket, UringPendingIoSubmitter};
use async_hybrid_fs::{Client, UringCfg};
use clap::{Parser, Subcommand};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, collections::VecDeque};

#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(subcommand)]
    scenario: Scenario,
}

#[derive(Clone, Debug, Subcommand)]
enum Scenario {
    InnerVecDeques,
    InnerUring,
    InnerThreads,
    IdleCustomClient,
    IdleDefaultClients,
}

#[global_allocator]
pub static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

pub async fn io_uring_inner(ready: tokio::sync::oneshot::Sender<()>) {
    let _ring = io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
        .setup_clamp()
        .build(16384)
        .expect("failed to build io_uring");
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn test_channel(ready: tokio::sync::oneshot::Sender<()>) {
    let (_sender, _receiver) = crossbeam_channel::unbounded::<UringPendingIoSubmitter>();
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn test_vecdeques(ready: tokio::sync::oneshot::Sender<()>) {
    let normal_cap = 16384 - 512;
    let cancel_cap = 512;
    let mut _normal_command_queue = VecDeque::<UringPendingIoSubmitter>::with_capacity(normal_cap);
    let mut _normal_ticket_queue = VecDeque::<SubmissionTicket>::with_capacity(normal_cap);
    let mut _cancel_command_queue = VecDeque::<UringPendingIoSubmitter>::with_capacity(cancel_cap);
    let mut _cancel_ticket_queue = VecDeque::<SubmissionTicket>::with_capacity(cancel_cap);
    let mut _ll_entries =
        VecDeque::<io_uring::squeue::Entry>::with_capacity(normal_cap + cancel_cap);
    let mut _command_submitters =
        VecDeque::<UringPendingIoSubmitter>::with_capacity(normal_cap + cancel_cap);
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn test_mock_threads(ready: tokio::sync::oneshot::Sender<()>) {
    let _sthread = std::thread::Builder::new().spawn(|| {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    let _cthread = std::thread::Builder::new().spawn(|| {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn idle_custom_client(ready: tokio::sync::oneshot::Sender<()>) {
    let _client = Client::build(UringCfg {
        operation_queue_size: 2048,
        ..Default::default()
    })
    .expect("failed to build client");
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn idle_default_clients(ready: tokio::sync::oneshot::Sender<()>) {
    // Clients are initialized lazily, so their memory costs are accounted here.
    let _clients = async_hybrid_fs::default_client();
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn trigger_known_globals() {
    let _ = async_hybrid_fs::default_client();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mem_initial = memory_stats::memory_stats().expect("failed to get memory stats");
    let mut r = Region::new(GLOBAL);
    r.reset();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let task = match args.scenario {
        Scenario::InnerVecDeques => tokio::spawn(test_vecdeques(ready)),
        Scenario::InnerUring => tokio::spawn(io_uring_inner(ready)),
        Scenario::InnerThreads => tokio::spawn(test_mock_threads(ready)),
        Scenario::IdleCustomClient => tokio::spawn(idle_custom_client(ready)),
        Scenario::IdleDefaultClients => tokio::spawn(idle_default_clients(ready)),
    };
    ready_rx.await.expect("failed to receive ready signal");
    let mem_final = memory_stats::memory_stats().expect("failed to get memory stats");
    let stats = r.change();
    let stats_alloc_mb_allocated = stats.bytes_allocated as f64 / 1024f64 / 1024f64;
    let stats_alloc_mb_deallocated = stats.bytes_deallocated as f64 / 1024f64 / 1024f64;
    let stats_alloc_mb_delta =
        (stats.bytes_allocated as i64 - stats.bytes_deallocated as i64) as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_initial = mem_initial.virtual_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_final = mem_final.virtual_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_mb_delta =
        (mem_final.virtual_mem as i64 - mem_initial.virtual_mem as i64) as f64 / 1024f64 / 1024f64;
    let memory_stats_phys_mb_initial = mem_initial.physical_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_phys_mb_final = mem_final.physical_mem as f64 / 1024f64 / 1024f64;
    let memory_stats_phys_mb_delta = (mem_final.physical_mem as i64
        - mem_initial.physical_mem as i64) as f64
        / 1024f64
        / 1024f64;

    println!("stats_alloc: allocated {} MB", stats_alloc_mb_allocated);
    println!("stats_alloc: deallocated {} MB", stats_alloc_mb_deallocated);
    println!("stats_alloc: delta {} MB", stats_alloc_mb_delta);
    println!("memory_stats: initial {} MB", memory_stats_mb_initial);
    println!("memory_stats: final {} MB", memory_stats_mb_final);
    println!("memory_stats: delta {} MB", memory_stats_mb_delta);
    println!(
        "memory_stats: physical initial {} MB",
        memory_stats_phys_mb_initial
    );
    println!(
        "memory_stats: physical final {} MB",
        memory_stats_phys_mb_final
    );
    println!(
        "memory_stats: physical delta {} MB",
        memory_stats_phys_mb_delta
    );

    task.abort();
    let _ = task.await;
    Ok(())
}
