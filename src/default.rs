use std::num::NonZero;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use crate::UringCfg;
use crate::client::Client;
use dashmap::DashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_CLIENT: Client = Client::build(UringCfg::default()).unwrap();
    static ref DEFAULT_CLIENT_SHARDS: DashMap<usize, Arc<Client>> = DashMap::new();
    static ref PARALLELISM: usize = std::thread::available_parallelism()
        .unwrap_or(NonZero::new(8).unwrap())
        .get();
    static ref MAX_DEFAULT_SHARDS: AtomicUsize = AtomicUsize::new(1);
}

/// Returns an owned reference to one of the default clients spawned by the library.
///
/// # Notes
/// - The library starts with a single default client shard. More of them can be spawned by calling [`set_max_default_shards`].
/// - The algorithm picks clients from left to right to prevent channel starvation. It attempts to perform a basic check whether an operation should be staged, and if no free client is found, it picks the client with the least utilization.
pub fn default_client_owned() -> Arc<Client> {
    let max_shards = MAX_DEFAULT_SHARDS.load(Ordering::Relaxed);
    let mut all_shards_metrics = Vec::with_capacity(max_shards);
    let mut clients = Vec::with_capacity(max_shards);
    for i in 0..max_shards {
        let client = DEFAULT_CLIENT_SHARDS
            .entry(i)
            .or_insert_with(|| {
                Client::build(UringCfg::default())
                    .map(|client| {
                        let client = Arc::new(client);
                        std::mem::forget(client.clone());
                        client
                    })
                    .expect("should create default io_uring client with fallback")
            })
            .clone();
        match client.uring_metrics() {
            // It is possible to cast the reference since the client lasts as long as the program
            None => {
                return client;
            }
            Some(metrics) => {
                // Best effort attempt to prevent multiple requests from being submitted to a client that is about to overflow
                if metrics.active_operations.saturating_add(*PARALLELISM)
                    < metrics.max_concurrent_operations
                {
                    return client;
                }
                clients.push(client);
                all_shards_metrics.push(metrics);
            }
        };
    }
    // Pick the client with the lowest utilization, since all clients are expected to be nearly or fully utilized
    let (picked_client_index, _) = all_shards_metrics
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.utilization
                .partial_cmp(&b.utilization)
                .expect("utilization metrics should not be NaN")
        })
        .unwrap();
    clients[picked_client_index].clone()
}

/// Returns a borrowed reference to one of the default clients. See [`default_client_owned`] for more details.
pub fn default_client() -> &'static Client {
    // let client = default_client_owned();
    // These Client instances live for the entire program duration
    // unsafe { std::mem::transmute::<&Client, &'static Client>(client.as_ref()) }
    &DEFAULT_CLIENT
}

/// Sets the maximum number of default client shards that the library will spawn.
///
/// # Notes
/// - Each client instance uses up approximately 135MB of memory.
/// - This function should only be called by application code and not library code.
/// - Due to backwards compatibility reasons (support for [`default_client`]), once a client already exists, it can no longer be destroyed.
pub fn set_max_default_shards(max_shards: usize) {
    MAX_DEFAULT_SHARDS.store(max_shards, Ordering::Release);
}

/// Returns the maximum number of default client shards that the library will spawn.
pub fn get_max_default_shards() -> usize {
    MAX_DEFAULT_SHARDS.load(Ordering::Acquire)
}
