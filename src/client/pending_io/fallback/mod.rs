use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task::JoinError;

use crate::{client::pending_io::PendingIoImpl, runtime};

type TokioScopedBlockingTask<'a> =
    Pin<Box<dyn Future<Output = ((), Vec<Result<usize, JoinError>>)> + Send + 'a>>;

struct Completion<'req, 'a, T>
where
    T: Send,
{
    task: Option<TokioScopedBlockingTask<'a>>,
    request: &'req mut TokioScopedPendingIo<'a, T>,
}

impl<'req, 'a, T> Completion<'req, 'a, T>
where
    T: Send,
{
    fn new(
        request: &'req mut TokioScopedPendingIo<'a, T>,
        task: TokioScopedBlockingTask<'a>,
    ) -> Self {
        Self {
            task: Some(task),
            request,
        }
    }
}

impl<'req, 'a, T> Future for Completion<'req, 'a, T>
where
    T: Send,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut task = this.task.take().expect("task already completed");
        match Pin::new(&mut task).poll(cx) {
            Poll::Ready(((), mut results)) => {
                let result = results.pop().expect("no result returned");
                let pointer = result.expect("future failed to join");
                Poll::Ready(unsafe { *Box::from_raw(pointer as *mut T) })
            }
            Poll::Pending => {
                this.task = Some(task);
                Poll::Pending
            }
        }
    }
}

impl<'req, 'a, T> Unpin for Completion<'req, 'a, T> where T: Send {}

impl<'req, 'a, T> Drop for Completion<'req, 'a, T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.request.task = self.task.take();
    }
}

pub(crate) struct TokioScopedPendingIo<'a, T>
where
    T: Send,
{
    task: Option<TokioScopedBlockingTask<'a>>,
    _phantom: PhantomData<T>,
}

impl<'a, T> TokioScopedPendingIo<'a, T>
where
    T: Send,
{
    pub(crate) fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> T + Send + 'a,
    {
        let task = Box::pin(unsafe {
            async_scoped::TokioScope::scope_and_collect(move |scope| {
                scope.spawn_blocking(move || {
                    let response = Box::new(f());
                    let leaked = Box::into_raw(response);
                    leaked as usize
                })
            })
        });
        Self {
            task: Some(task),
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<'a, T> PendingIoImpl<T> for TokioScopedPendingIo<'a, T>
where
    T: Send,
{
    fn _completion<'req>(&'req mut self) -> Option<Pin<Box<dyn Future<Output = T> + Send + 'req>>> {
        match self.task.take() {
            Some(task) => Some(Box::pin(Completion::new(self, task))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<T> {
        Some(self._completion()?.await)
    }
}

impl<'a, T> Unpin for TokioScopedPendingIo<'a, T> where T: Send {}

impl<'a, T> Drop for TokioScopedPendingIo<'a, T>
where
    T: Send,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
