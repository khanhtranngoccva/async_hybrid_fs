pub mod chained;
pub(crate) mod fallback;
pub(crate) mod fixed_value;
pub(crate) mod uring;

use crate::client::pending_io::chained::ChainedPendingIo;
use std::pin::Pin;

#[async_trait::async_trait]
pub(crate) trait PendingIoImpl<T>: Unpin + Send {
    /// Return a boxed future that can be awaited to get the result of the operation.
    fn _completion<'a>(&'a mut self) -> Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>;

    /// Cancel the pending I/O operation and wait for the result asynchronously. If the operation is to be executed to completion, the transforming behavior must be identical to completion()
    async fn _cancel_async(&mut self) -> Option<T>;

    /// Cancel the pending I/O operation synchronously. After execution this function, the inner object must be declared finished, the result must be extracted and transformed, and the transforming behavior must be identical to completion()
    fn _cancel(&mut self) -> Option<T>;
}

/// Wrapper for user-friendly methods for cancellable pending I/O operations.
/// Upon return from respective code, these futures are guaranteed to be running in the background, and should cancel I/O operations properly upon being dropped.
///
/// To prevent misuse, pending I/O objects do not implement [`Future`] by default - it is recommended to use the [`Self::completion`] method instead. If you wish to await the I/O operation directly, you can use the crate feature `pending-io-futures`. These convenience futures should not be multiplexed with other futures to avoid missed operations.
pub struct PendingIo<'lifetime, T>
where
    T: Send,
{
    inner: Box<dyn PendingIoImpl<T> + Send + 'lifetime>,
}

impl<'lifetime, T> PendingIo<'lifetime, T>
where
    T: Send,
{
    /// Create a type-erased pending I/O operation.
    pub(crate) fn new<I>(inner: I) -> Self
    where
        I: PendingIoImpl<T> + Send + 'lifetime,
    {
        Self {
            inner: Box::new(inner),
        }
    }

    /// Return a boxed future that can be awaited to get the result of the operation.
    /// This future is guaranteed to be trivially droppable to be multiplexed with other futures like cancellation tokens or timeouts.
    pub fn completion<'a>(&'a mut self) -> Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>> {
        self.inner._completion()
    }

    /// Consume the object and cancel the pending I/O operation.
    pub async fn cancel(mut self) -> Option<T> {
        self.inner._cancel_async().await
    }

    /// Chain a function to the pending I/O operation that is guaranteed to run if the operation is completed, even if the pending structure is dropped.
    pub fn map<Processor, Out>(self, processor: Processor) -> PendingIo<'lifetime, Out>
    where
        Processor: FnOnce(T) -> Out + Send + 'lifetime,
        T: 'lifetime,
        Out: Send + 'lifetime,
    {
        PendingIo::new(ChainedPendingIo::new(self.inner, processor))
    }
}

#[cfg(feature = "pending-io-futures")]
mod pending_io_futures {
    use super::*;
    use std::task::{Context, Poll};

    impl<'lifetime, T> Future for PendingIo<'lifetime, T>
    where
        T: Send,
    {
        type Output = T;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut fut = self
                .get_mut()
                .completion()
                .expect("future should not be cancelled");
            Pin::new(&mut fut).poll(cx)
        }
    }
}
