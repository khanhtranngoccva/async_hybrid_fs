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
        // Hot path: There is generally no point in waiting for the operation if it is
        // already completed or has not been submitted yet, executing the future costs more time.
        if self.task.is_none() {
            return;
        }
        runtime::execute_future_from_sync(self._cancel());
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Handle, RuntimeFlavor};

    use crate::HybridWrite;

    use super::TokioScopedPendingIo;
    use std::{
        io::{self, pipe},
        os::fd::AsFd,
    };

    #[tokio::test]
    #[test_log::test]
    async fn single_thread_runtime_should_be_able_to_drop_pending_io() {
        let handle = Handle::current();
        assert!(handle.runtime_flavor() == RuntimeFlavor::CurrentThread);
        let (tx, rx) = oneshot::channel::<()>();
        let (pipe_read, mut pipe_write) = pipe().expect("should be able to create a pipe");
        let pending_io = TokioScopedPendingIo::new(move || -> io::Result<()> {
            let mut buf = [0; 64];
            nix::unistd::read(pipe_read.as_fd(), &mut buf)?;
            let _ = tx.send(());
            Ok(())
        });
        pipe_write
            .write_all(b"test")
            .await
            .expect("should be able to write to pipe to allow completion");
        drop(pending_io);
        assert!(rx.recv().is_ok());
    }
}
