use super::Spawnable;
use crate::{client::pending_io::PendingIoImpl, runtime};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

struct Completion<'req, 'a, T>
where
    T: Send,
{
    rx: Option<oneshot::AsyncReceiver<usize>>,
    request: &'req mut SpawnablePendingIo<'a, T>,
}

impl<'req, 'a, T> Completion<'req, 'a, T>
where
    T: Send,
{
    fn new(
        request: &'req mut SpawnablePendingIo<'a, T>,
        rx: oneshot::AsyncReceiver<usize>,
    ) -> Self {
        Self {
            rx: Some(rx),
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
        let mut rx = this.rx.take().expect("rx already completed");
        match Pin::new(&mut rx).poll(cx) {
            Poll::Ready(Ok(pointer)) => Poll::Ready(unsafe { *Box::from_raw(pointer as *mut T) }),
            Poll::Ready(Err(e)) => panic!("future failed to join: {}", e),
            Poll::Pending => {
                this.rx = Some(rx);
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
        self.request.rx = self.rx.take();
    }
}

pub(crate) struct SpawnablePendingIo<'a, T>
where
    T: Send,
{
    rx: Option<oneshot::AsyncReceiver<usize>>,
    _phantom_receiver: PhantomData<T>,
    _phantom_lifetime: PhantomData<&'a ()>,
}

impl<'a, T> SpawnablePendingIo<'a, T>
where
    T: Send,
{
    pub(crate) fn new<F>(spawner: &dyn Spawnable, f: F) -> Self
    where
        F: FnOnce() -> T + Send + 'a,
    {
        let (tx, rx) = oneshot::async_channel::<usize>();
        let boxed: Box<dyn FnOnce() + Send + 'a> = Box::new(move || {
            let response = Box::new(f());
            let leaked = Box::into_raw(response);
            tx.send(leaked as usize).unwrap();
        });
        let transmuted = unsafe {
            std::mem::transmute::<Box<dyn FnOnce() + Send + 'a>, Box<dyn FnOnce() + Send + 'static>>(
                boxed,
            )
        };
        spawner.spawn_blocking(transmuted);
        Self {
            rx: Some(rx),
            _phantom_receiver: PhantomData,
            _phantom_lifetime: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<'a, T> PendingIoImpl<T> for SpawnablePendingIo<'a, T>
where
    T: Send,
{
    fn _completion<'req>(&'req mut self) -> Option<Pin<Box<dyn Future<Output = T> + Send + 'req>>> {
        match self.rx.take() {
            Some(rx) => Some(Box::pin(Completion::new(self, rx))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<T> {
        Some(self._completion()?.await)
    }

    fn _cancel(&mut self) -> Option<T> {
        if self.rx.is_some() {
            runtime::execute_future_from_sync(self._cancel_async())
        } else {
            None
        }
    }
}

impl<'a, T> Unpin for SpawnablePendingIo<'a, T> where T: Send {}

impl<'a, T> Drop for SpawnablePendingIo<'a, T>
where
    T: Send,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Handle, RuntimeFlavor};

    use crate::HybridWrite;

    use super::SpawnablePendingIo;
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
        let pending_io = SpawnablePendingIo::new(&handle, move || -> io::Result<()> {
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
