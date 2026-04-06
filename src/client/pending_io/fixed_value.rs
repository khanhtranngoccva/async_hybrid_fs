use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::client::pending_io::PendingIoImpl;

struct Completion<'req, T>
where
    T: Send,
{
    value: Option<T>,
    request: &'req mut FixedValuePendingIo<T>,
}

impl<'req, T> Completion<'req, T>
where
    T: Send,
{
    pub(crate) fn new(request: &'req mut FixedValuePendingIo<T>, value: T) -> Self {
        Self {
            value: Some(value),
            request,
        }
    }
}

impl<'req, T> Future for Completion<'req, T>
where
    T: Send,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut();
        Poll::Ready(inner.value.take().expect("value should exist"))
    }
}

impl<'req, T> Unpin for Completion<'req, T> where T: Send {}

impl<'req, T> Drop for Completion<'req, T>
where
    T: Send,
{
    fn drop(&mut self) {
        self.request.value = self.value.take();
    }
}

pub(crate) struct FixedValuePendingIo<T>
where
    T: Send,
{
    value: Option<T>,
}

impl<T> FixedValuePendingIo<T>
where
    T: Send,
{
    pub(crate) fn new(value: T) -> Self {
        Self { value: Some(value) }
    }
}

#[async_trait::async_trait]
impl<T> PendingIoImpl<T> for FixedValuePendingIo<T>
where
    T: Send,
{
    fn _completion<'req>(&'req mut self) -> Option<Pin<Box<dyn Future<Output = T> + Send + 'req>>> {
        match self.value.take() {
            Some(value) => Some(Box::pin(Completion::new(self, value))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<T> {
        Some(self._completion()?.await)
    }
}

impl<T> Unpin for FixedValuePendingIo<T> where T: Send {}
