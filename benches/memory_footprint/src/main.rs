use async_hybrid_fs::{Client, UringCfg};
use clap::{Parser, ValueEnum};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long, value_enum)]
    scenario: Scenario,
}

#[derive(Clone, Debug, ValueEnum)]
enum Scenario {
    IdleCustomClient,
    IdleDefaultClients,
}

#[global_allocator]
pub static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

pub async fn idle_custom_client(ready: tokio::sync::oneshot::Sender<()>) {
    let _client = Client::build(UringCfg::default()).expect("failed to build client");
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

pub async fn idle_default_clients(ready: tokio::sync::oneshot::Sender<()>) {
    // Clients are initialized lazily, so their memory costs are accounted here.
    let _clients = async_hybrid_fs::default_client();
    ready.send(()).expect("failed to send ready signal");
    std::future::pending().await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mem_initial = memory_stats::memory_stats().expect("failed to get memory stats");
    let mut r = Region::new(GLOBAL);
    r.reset();
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    match args.scenario {
        Scenario::IdleCustomClient => {
            tokio::spawn(idle_custom_client(ready));
        }
        Scenario::IdleDefaultClients => {
            tokio::spawn(idle_default_clients(ready));
        }
    }
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

    println!("stats_alloc: allocated {} MB", stats_alloc_mb_allocated);
    println!("stats_alloc: deallocated {} MB", stats_alloc_mb_deallocated);
    println!("stats_alloc: delta {} MB", stats_alloc_mb_delta);
    println!("memory_stats: initial {} MB", memory_stats_mb_initial);
    println!("memory_stats: final {} MB", memory_stats_mb_final);
    println!("memory_stats: delta {} MB", memory_stats_mb_delta);

    Ok(())
}
