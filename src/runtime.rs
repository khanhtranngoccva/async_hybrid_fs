use tokio::runtime::{Builder, Handle, RuntimeFlavor};

/// Attempts to execute a future on a temporary runtime (based on async_scoped's TokioScope implementation). This is generally used to facilitate drops of pending I/O operations.
///
/// # Note
/// - On current-thread runtimes, this function blocks the runtime and any non-blocking tasks or futures driven by it. Make sure the future in question does not await anything that is driven by the runtime, or you will risk a deadlock.
/// -
pub(crate) fn execute_future_from_sync<F>(future: F) -> F::Output
where
    F::Output: Send,
    F: Future + Send,
{
    let handle = Handle::try_current().ok();
    match handle {
        Some(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::CurrentThread => std::thread::scope(|s| {
                s.spawn(move || {
                    let backup_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
                    backup_runtime.block_on(future)
                })
                .join()
                .unwrap()
            }),
            RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(move || handle.block_on(future))
            }
            _ => {
                unreachable!("Unsupported runtime flavor: {:?}", handle.runtime_flavor())
            }
        },
        None => {
            let backup_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
            tokio::task::block_in_place(move || backup_runtime.block_on(future))
        }
    }
}
