use crate::{Bytes, Client, Error, EventStream, Result, StreamOptions, Workspace, proto};
use futures_core::Stream;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

/// Initial PTY dimensions and subscription deadline.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct TerminalOptions {
    /// Initial columns, default 80; must be positive.
    pub cols: u32,
    /// Initial rows, default 24; must be positive.
    pub rows: u32,
    /// Deadline for the attached event stream; defaults to unlimited.
    pub stream: StreamOptions,
}
impl Default for TerminalOptions {
    fn default() -> Self {
        Self {
            cols: 80,
            rows: 24,
            stream: StreamOptions::default(),
        }
    }
}
impl TerminalOptions {
    /// Changes the initial PTY dimensions.
    pub fn size(mut self, cols: u32, rows: u32) -> Self {
        self.cols = cols;
        self.rows = rows;
        self
    }
    /// Sets subscription settings.
    pub fn stream(mut self, stream: StreamOptions) -> Self {
        self.stream = stream;
        self
    }
    fn validate(self) -> Result<Self> {
        if self.cols == 0 || self.rows == 0 {
            return Err(Error::InvalidArgument(
                "terminal dimensions must be positive".into(),
            ));
        }
        self.stream.validate()?;
        Ok(self)
    }
}
/// Output, resize, or closure event from a persistent PTY shell.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalEvent {
    /// Binary PTY output.
    Output(Bytes),
    /// Another listener or the local caller resized the PTY.
    Resize {
        /// New column count.
        cols: u32,
        /// New row count.
        rows: u32,
    },
    /// The remote shell ended or was explicitly removed.
    Closed,
}
/// Persistent Workspace terminals. Multiple callers may attach to the same shell.
#[derive(Clone, Debug)]
pub struct Terminals {
    workspace: Workspace,
}
impl Terminals {
    pub(crate) fn new(workspace: Workspace) -> Self {
        Self { workspace }
    }
    /// Creates a persistent PTY and attaches to its event stream.
    /// If attaching fails the remote terminal remains available through `list`.
    pub async fn create(&self, options: TerminalOptions) -> Result<Terminal> {
        self.workspace
            .client
            .operation(self.workspace.client.default_timeout(), async {
                let options = options.validate()?;
                let response = self
                    .workspace
                    .client
                    .rpc(self.workspace.client.workspace_service().create_terminal(
                        proto::CreateTerminalRequest {
                            workspace_ref: Some(self.workspace.object()?),
                            cols: options.cols,
                            rows: options.rows,
                        },
                    ))
                    .await?;
                self.attach(&response.id, options.stream).await
            })
            .await
    }
    /// Attaches to an existing terminal. Dropping or detaching leaves the shell alive.
    pub async fn attach(&self, id: &str, options: StreamOptions) -> Result<Terminal> {
        crate::error::nonempty(id, "terminal ID")?;
        let deadline = options.deadline()?;
        let client = self.workspace.client.clone();
        let rpc = client
            .operation_until(deadline, async {
                client
                    .rpc(
                        client
                            .workspace_service()
                            .listen_terminal(proto::ListenTerminalRequest { id: id.into() }),
                    )
                    .await
            })
            .await?;
        let input = TerminalInput {
            client: client.clone(),
            id: Arc::from(id),
            detached: CancellationToken::new(),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        Ok(Terminal {
            events: Some(crate::stream::rpc_stream(client, rpc, deadline)),
            input,
        })
    }
    /// Returns identifiers of currently running PTY shells.
    pub async fn list(&self) -> Result<Vec<String>> {
        Ok(self
            .workspace
            .client
            .rpc(self.workspace.client.workspace_service().list_terminal(
                proto::ListTerminalRequest {
                    workspace_ref: Some(self.workspace.object()?),
                },
            ))
            .await?
            .items
            .into_iter()
            .map(|t| t.id)
            .collect())
    }
    /// Terminates a persistent terminal remotely, even without an attached listener.
    pub async fn remove(&self, id: &str) -> Result<()> {
        crate::error::nonempty(id, "terminal ID")?;
        self.workspace
            .client
            .rpc(
                self.workspace
                    .client
                    .workspace_service()
                    .remove_terminal(proto::RemoveTerminalRequest { id: id.into() }),
            )
            .await?;
        Ok(())
    }
}
/// A clonable terminal write/resize half, usable while another task consumes output.
#[derive(Clone, Debug)]
pub struct TerminalInput {
    client: Client,
    id: Arc<str>,
    detached: CancellationToken,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}
impl TerminalInput {
    /// Returns the persistent terminal identifier.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Writes binary stdin in chunks of at most 32 KiB.
    pub async fn write(&self, data: impl AsRef<[u8]>) -> Result<()> {
        self.client.operation(self.client.default_timeout(), async {
            if self.detached.is_cancelled() {
                return Err(Error::Protocol("terminal is detached or closed".into()));
            }
            let _guard = self.write_lock.lock().await;
            for chunk in data.as_ref().chunks(32 * 1024) {
                let mut service = self.client.workspace_service();
                let request=proto::WriteTerminalDataRequest {id:self.id.to_string(),data:Bytes::copy_from_slice(chunk)};
                tokio::select! {
                    biased;
                    _ = self.detached.cancelled() => return Err(Error::Protocol("terminal is detached or closed".into())),
                    result = self.client.rpc(service.write_terminal_data(request)) => {result?;}
                }
            }
            Ok(())
        }).await
    }
    /// Resizes the PTY. Both dimensions must be positive.
    pub async fn resize(&self, cols: u32, rows: u32) -> Result<()> {
        if cols == 0 || rows == 0 {
            return Err(Error::InvalidArgument(
                "terminal dimensions must be positive".into(),
            ));
        }
        if self.detached.is_cancelled() {
            return Err(Error::Protocol("terminal is detached or closed".into()));
        }
        self.client
            .rpc(self.client.workspace_service().set_terminal_window_size(
                proto::SetTerminalWindowSizeRequest {
                    id: self.id.to_string(),
                    cols,
                    rows,
                },
            ))
            .await?;
        Ok(())
    }
}
/// An attached PTY event stream. Drop detaches; only `remove` terminates the persistent shell.
#[derive(Debug)]
#[must_use = "consume terminal events or keep the handle to write and resize"]
pub struct Terminal {
    events: Option<EventStream<proto::ListenTerminalResponse>>,
    input: TerminalInput,
}
impl Terminal {
    /// Returns the remote terminal ID.
    pub fn id(&self) -> &str {
        self.input.id()
    }
    /// Clones a write/resize half for concurrent use.
    pub fn input(&self) -> TerminalInput {
        self.input.clone()
    }
    /// Writes binary stdin.
    pub async fn write(&self, data: impl AsRef<[u8]>) -> Result<()> {
        self.input.write(data).await
    }
    /// Resizes this terminal.
    pub async fn resize(&self, cols: u32, rows: u32) -> Result<()> {
        self.input.resize(cols, rows).await
    }
    /// Cancels the local subscription and disables local writes, leaving the shell alive.
    pub fn detach(&mut self) {
        self.events = None;
        self.input.detached.cancel();
    }
    /// Terminates the remote shell and detaches, including after local detachment.
    pub async fn remove(&mut self) -> Result<()> {
        self.input
            .client
            .rpc(self.input.client.workspace_service().remove_terminal(
                proto::RemoveTerminalRequest {
                    id: self.id().into(),
                },
            ))
            .await?;
        self.detach();
        Ok(())
    }
}
impl Stream for Terminal {
    type Item = Result<TerminalEvent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(events) = self.events.as_mut() else {
                return Poll::Ready(None);
            };
            let event = match Pin::new(events).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.detach();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.detach();
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(msg))) => match msg.r#type {
                    Some(proto::listen_terminal_response::Type::Stdout(s)) => {
                        TerminalEvent::Output(s.data)
                    }
                    Some(proto::listen_terminal_response::Type::WindowSize(s)) => {
                        TerminalEvent::Resize {
                            cols: s.cols,
                            rows: s.rows,
                        }
                    }
                    Some(proto::listen_terminal_response::Type::Close(_)) => {
                        self.detach();
                        TerminalEvent::Closed
                    }
                    None => continue,
                },
            };
            return Poll::Ready(Some(Ok(event)));
        }
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        self.input.detached.cancel();
    }
}
