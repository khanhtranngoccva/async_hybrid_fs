use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::client::pending_io::PendingIoImpl;
use crate::runtime;

struct CompletionState<'inner_pending, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    input: Pin<Box<dyn Future<Output = In> + Send + 'inner_pending>>,
    processor: Processor,
}

struct Completion<'req, 'inner_pending, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    state: Option<CompletionState<'inner_pending, Processor, In, Out>>,
    processor_slot: &'req mut Option<Processor>,
}

impl<'req, 'inner_pending, Processor, In, Out> Future
    for Completion<'req, 'inner_pending, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    type Output = Out;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.take().expect("state should not be None");
        match Pin::new(&mut state.input).poll(cx) {
            Poll::Pending => {
                this.state = Some(state);
                Poll::Pending
            }
            Poll::Ready(input) => Poll::Ready((state.processor)(input)),
        }
    }
}

impl<'req, 'inner_pending, 'lifetime, Processor, In, Out> Unpin
    for Completion<'req, 'inner_pending, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
}

impl<'req, 'inner_pending, 'lifetime, Processor, In, Out> Drop
    for Completion<'req, 'inner_pending, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    fn drop(&mut self) {
        *self.processor_slot = self.state.take().map(|state| state.processor);
    }
}

pub(crate) struct ChainedPendingIo<'lifetime, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    input: Box<dyn PendingIoImpl<In> + 'lifetime + Send>,
    processor: Option<Processor>,
}

#[async_trait::async_trait]
impl<'lifetime, Processor, In, Out> PendingIoImpl<Out>
    for ChainedPendingIo<'lifetime, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Out> + Send + 'req>>> {
        match self.input._completion() {
            Some(completion) => Some(Box::pin(Completion {
                state: Some(CompletionState {
                    input: completion,
                    processor: self.processor.take().expect("processor should not be None"),
                }),
                processor_slot: &mut self.processor,
            })),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<Out> {
        self.input
            ._cancel()
            .await
            .map(|input| (self.processor.take().expect("processor should not be None"))(input))
    }
}

impl<'lifetime, Processor, In, Out> ChainedPendingIo<'lifetime, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    pub(crate) fn new(
        input: Box<dyn PendingIoImpl<In> + 'lifetime + Send>,
        processor: Processor,
    ) -> Self {
        Self {
            input,
            processor: Some(processor),
        }
    }
}

impl<'lifetime, Processor, In, Out> Unpin for ChainedPendingIo<'lifetime, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
}

impl<'lifetime, Processor, In, Out> Drop for ChainedPendingIo<'lifetime, Processor, In, Out>
where
    Processor: FnOnce(In) -> Out + Send,
    In: Send,
    Out: Send,
{
    fn drop(&mut self) {
        // Hot path: If the processor is removed, the operation is already completed or cancelled.
        if self.processor.is_none() {
            return;
        }
        // Must run cancel() on outer structure instead of the inner structure because we want
        // inner operations to be performed
        runtime::execute_future_from_sync(self._cancel());
    }
}

#[cfg(test)]
mod tests {
    use std::{io::pipe, os::fd::AsFd};

    use crate::{
        HybridRead, PendingIo, client::pending_io::fallback::TokioScopedPendingIo, default_client,
    };
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn should_not_run_processor_code_if_canceled() {
        if !default_client().is_uring_available_and_active() {
            println!("uring is not available, skipping test");
        }
        let (tx, rx) = oneshot::channel::<()>();
        let (pipe_read, _pipe_write) = pipe().expect("should be able to create a pipe");
        let mut buf = [0; 64];
        let mut pipe_read_fd = pipe_read.as_fd();
        let chained_io = pipe_read_fd.hybrid_read(&mut buf).map(|_| {
            let _ = tx.send(());
        });
        assert!(
            chained_io.cancel().await.is_none(),
            "pipe operation should be cancellable because the writer has not sent anything"
        );
        rx.await.expect_err(
            "should not be able to receive a message because the processor code should not run",
        );
    }

    #[tokio::test]
    async fn should_run_processor_code_on_drop() {
        let (tx, rx) = oneshot::channel::<String>();
        let raw_io = PendingIo::new(TokioScopedPendingIo::new(|| "test"));
        let chained = raw_io.map(|s| tx.send(s.to_uppercase()));
        drop(chained);
        assert_eq!(rx.await.unwrap(), "TEST");
    }

    #[tokio::test]
    async fn should_run_processor_code_on_uncancelables() {
        let raw_io = PendingIo::new(TokioScopedPendingIo::new(|| "test"));
        let chained = raw_io.map(|s| s.to_uppercase());
        assert_eq!(chained.cancel().await, Some("TEST".to_string()));
    }
}
