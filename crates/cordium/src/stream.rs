use crate::{Client, Error, Result};
use futures_core::Stream;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// Settings for a server stream. Streaming is unlimited in duration by default.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct StreamOptions {
    /// Total time from opening the stream to its end. `None` disables the deadline.
    pub timeout: Option<Duration>,
}
impl StreamOptions {
    /// Sets a total stream deadline.
    pub fn timeout(mut self, t: Option<Duration>) -> Self {
        self.timeout = t;
        self
    }
    pub(crate) fn deadline(self) -> Result<Option<tokio::time::Instant>> {
        self.validate()?;
        self.timeout
            .map(|t| {
                tokio::time::Instant::now()
                    .checked_add(t)
                    .ok_or_else(|| Error::InvalidArgument("stream timeout is too large".into()))
            })
            .transpose()
    }
    pub(crate) fn validate(self) -> Result<Self> {
        if self.timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::InvalidArgument(
                "stream timeout must be positive".into(),
            ));
        }
        Ok(self)
    }
}
/// An owned, backpressured async stream. Dropping it cancels its RPC.
///
/// Import `futures_util::StreamExt` to call `next`. No SDK output task or unbounded
/// queue is created. Closing the client ends active streams with [`Error::Closed`].
#[must_use = "streams must be polled to make progress"]
pub struct EventStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
}
impl<T> fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}
impl<T> Stream for EventStream<T> {
    type Item = Result<T>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl<T> EventStream<T> {
    pub(crate) fn new(s: impl Stream<Item = Result<T>> + Send + 'static) -> Self {
        Self { inner: Box::pin(s) }
    }
}
pub(crate) fn rpc_stream<T: Send + 'static>(
    client: Client,
    mut rpc: tonic::Streaming<T>,
    deadline: Option<tokio::time::Instant>,
) -> EventStream<T> {
    EventStream::new(async_stream::try_stream! {
        loop {
            let next = client.operation_until(deadline, async { Ok(rpc.message().await?) }).await?;
            match next { Some(msg) => yield msg, None => break }
        }
    })
}
