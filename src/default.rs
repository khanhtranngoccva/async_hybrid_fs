use std::num::NonZero;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::available_parallelism;

use crate::client;
use crate::client::Client;
use crate::client::UringCfg;
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_CLIENT_SHARDS: Vec<Client> = generate_default_client_shards();
    static ref ACCESS_COUNTER: AtomicUsize = AtomicUsize::new(0);
}

/// Generate a vector of default clients, one for each shard.
pub fn generate_default_client_shards() -> Vec<Client> {
    let num_shards = available_parallelism()
        .unwrap_or(NonZero::new(8usize).unwrap())
        .get();
    let mut clients = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        clients.push(Client::build(UringCfg::default()).expect("failed to build client"));
    }
    clients
}

/// Get a reference to one of the default client objects. This function is optimized for default usage scenarios, where each operation calls default_client() once and registered files are not used. The function cycles through the shards in a round-robin fashion every time a default queue is estimated to be exhausted.
pub fn default_client() -> &'static Client {
    // We assume that every operation will try to retrieve a client reference once, and we know that each default queue has ~(16384-512) entries. After exhausting 16384 entries, we cycle to the next shard to allow the next operation to get started as quickly as possible.
    let access_count = ACCESS_COUNTER.fetch_add(1, Ordering::Relaxed);
    let bucket = access_count / client::DEFAULT_OP_QUEUE_SIZE as usize;
    let shard_index = bucket % DEFAULT_CLIENT_SHARDS.len();
    &DEFAULT_CLIENT_SHARDS[shard_index]
}
