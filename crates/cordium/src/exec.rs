use crate::{Bytes, BytesMut, Error, EventStream, Result, Workspace, proto};
use futures_core::Stream;
use futures_util::StreamExt;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

/// An explicit shell command or safely quoted argument vector.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Command {
    /// Shell syntax interpreted by the Workspace shell.
    Shell(String),
    /// Arguments quoted individually as literal POSIX shell words.
    Args(Vec<String>),
}
impl Command {
    /// Uses shell syntax, including pipes and redirection.
    pub fn shell(value: impl Into<String>) -> Self {
        Self::Shell(value.into())
    }
    /// Quotes an argument vector to prevent shell interpolation.
    pub fn argv<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Args(args.into_iter().map(Into::into).collect())
    }
    fn render(&self) -> Result<String> {
        match self {
            Self::Shell(v) => {
                crate::error::nonempty(v, "command")?;
                Ok(v.clone())
            }
            Self::Args(args) => {
                if args.is_empty() {
                    return Err(Error::InvalidArgument("argument vector is empty".into()));
                }
                crate::error::nonempty(&args[0], "executable")?;
                Ok(args
                    .iter()
                    .map(|s| shell_quote(s))
                    .collect::<Result<Vec<_>>>()?
                    .join(" "))
            }
        }
    }
}
impl From<&str> for Command {
    fn from(v: &str) -> Self {
        Self::Shell(v.into())
    }
}
impl From<String> for Command {
    fn from(v: String) -> Self {
        Self::Shell(v)
    }
}
/// Quotes one literal POSIX shell word. NUL cannot be represented and is rejected.
pub fn shell_quote(value: &str) -> Result<String> {
    if value.contains('\0') {
        return Err(Error::InvalidArgument(
            "shell words cannot contain NUL".into(),
        ));
    }
    Ok(format!("'{}'", value.replace('\'', "'\"'\"'")))
}

/// Collected binary output and the command's exit status.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExecResult {
    /// Process exit code reported by Cordium.
    pub exit_code: i32,
    /// Captured stdout bytes; bounded independently from stderr.
    pub stdout: Bytes,
    /// Captured stderr bytes.
    pub stderr: Bytes,
    /// True if either output exceeded its capture bound. Streaming events remain complete.
    pub truncated: bool,
    /// True if the SDK sent a kill request.
    pub killed: bool,
}
impl ExecResult {
    /// Whether the exit code is zero.
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }
    /// Decodes stdout with replacement for invalid UTF-8, preserving the original bytes.
    pub fn stdout_text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }
    /// Decodes stderr with replacement for invalid UTF-8.
    pub fn stderr_text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.stderr)
    }
    /// Returns this result or an error retaining all captured diagnostics for a nonzero exit.
    pub fn check(self) -> Result<Self> {
        if self.success() {
            Ok(self)
        } else {
            Err(Error::CommandFailed(Box::new(self)))
        }
    }
}
/// Binary command output or an explicit process exit event.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecEvent {
    /// Standard output chunk.
    Stdout(Bytes),
    /// Standard error chunk.
    Stderr(Bytes),
    /// Process exit code. This ends the SDK stream even if the server leaves the RPC open.
    Exit(i32),
}
/// Configures a command. Awaiting this builder collects output with bounded capture.
#[derive(Clone, Debug)]
#[must_use = "await the builder or call stream to execute the command"]
pub struct ExecBuilder {
    workspace: Workspace,
    command: Command,
    env: BTreeMap<String, String>,
    cwd: String,
    root: bool,
    timeout: Option<Duration>,
    capture: usize,
    check: bool,
    has_stdin: bool,
    stdin: Option<Bytes>,
}
impl ExecBuilder {
    pub(crate) fn new(workspace: Workspace, command: Command) -> Self {
        Self {
            workspace,
            command,
            env: BTreeMap::new(),
            cwd: String::new(),
            root: false,
            timeout: None,
            capture: 1024 * 1024,
            check: true,
            has_stdin: false,
            stdin: None,
        }
    }
    /// Adds a command environment variable.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
    /// Sets the remote working directory; no shell expansion is performed by the SDK.
    pub fn cwd(mut self, value: impl Into<String>) -> Self {
        self.cwd = value.into();
        self
    }
    /// Runs as root instead of as the Workspace user.
    pub fn as_root(mut self, value: bool) -> Self {
        self.root = value;
        self
    }
    /// Sets a total execution deadline, including authentication and stdin writes. Default is unlimited.
    pub fn timeout(mut self, value: Option<Duration>) -> Self {
        self.timeout = value;
        self
    }
    /// Sets the capture bound per output stream. Zero disables capture. Events are unaffected.
    pub fn max_capture_bytes(mut self, value: usize) -> Self {
        self.capture = value;
        self
    }
    /// Whether a nonzero exit becomes `Error::CommandFailed` during collected execution or wait. Default true.
    pub fn check(mut self, value: bool) -> Self {
        self.check = value;
        self
    }
    /// Supplies initial bytes for collected execution. The protocol has no stdin EOF message.
    /// Commands must read a known length or terminate independently; `cat` waiting for EOF will hang.
    pub fn stdin(mut self, value: impl Into<Bytes>) -> Self {
        self.stdin = Some(value.into());
        self.has_stdin = true;
        self
    }
    /// Enables manual stdin for streaming execution. Use `session.input()` to write.
    pub fn stdin_enabled(mut self, value: bool) -> Self {
        self.has_stdin = value;
        self
    }
    /// Executes and collects bounded output while draining both streams. No command retry is made.
    pub async fn run(mut self) -> Result<ExecResult> {
        let input = self.stdin.take();
        let client = self.workspace.client.clone();
        let timeout = self.timeout;
        client
            .operation(timeout, async {
                let session = self.stream().await?;
                if let Some(data) = input {
                    let writer = session.input();
                    let (_, result) = tokio::try_join!(writer.write(data), session.wait())?;
                    Ok(result)
                } else {
                    session.wait().await
                }
            })
            .await
    }
    /// Opens a backpressured command stream. Drop cancels the RPC and closes its input handle.
    /// Consume output concurrently with large stdin writes to avoid flow-control deadlocks.
    /// Initial `.stdin(...)` is for collected execution; use the input handle here.
    pub async fn stream(self) -> Result<ExecSession> {
        if self.stdin.is_some() {
            return Err(Error::InvalidArgument(
                "streaming stdin must use the input handle".into(),
            ));
        }
        if self.timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::InvalidArgument(
                "execution timeout must be positive".into(),
            ));
        }
        let command = self.command.render()?;
        for (key, value) in &self.env {
            crate::spec::validate_env_key(key)?;
            if value.contains('\0') {
                return Err(Error::InvalidArgument(
                    "environment values cannot contain NUL".into(),
                ));
            }
        }
        if self.cwd.contains('\0') {
            return Err(Error::InvalidArgument(
                "working directory cannot contain NUL".into(),
            ));
        }
        let deadline = self
            .timeout
            .map(|t| {
                tokio::time::Instant::now()
                    .checked_add(t)
                    .ok_or_else(|| Error::InvalidArgument("timeout is too large".into()))
            })
            .transpose()?;
        let (tx, rx) = mpsc::channel(16);
        let request = proto::ExecRequest {
            r#type: Some(proto::exec_request::Type::Request(
                proto::exec_request::Request {
                    workspace_ref: Some(self.workspace.object()?),
                    command,
                    working_dir: self.cwd,
                    env_vars: self
                        .env
                        .into_iter()
                        .map(|(key, value)| proto::exec_request::request::EnvVar { key, value })
                        .collect(),
                    run_as_root: self.root,
                    has_stdin: self.has_stdin,
                },
            )),
        };
        tx.try_send(request)
            .map_err(|_| Error::Protocol("could not initialize exec request queue".into()))?;
        let stream = self
            .workspace
            .client
            .operation_until(deadline, async {
                Ok(self
                    .workspace
                    .client
                    .workspace_service()
                    .exec(tokio_stream::wrappers::ReceiverStream::new(rx))
                    .await?
                    .into_inner())
            })
            .await?;
        let output = crate::stream::rpc_stream(self.workspace.client.clone(), stream, deadline);
        let ended = CancellationToken::new();
        let killed = Arc::new(AtomicBool::new(false));
        let input = ExecInput {
            sender: tx,
            ended: ended.clone(),
            client: self.workspace.client,
            lock: Arc::new(Mutex::new(())),
            enabled: self.has_stdin,
            killed: killed.clone(),
        };
        Ok(ExecSession {
            output: Some(output),
            input,
            stdout: BytesMut::new(),
            stderr: BytesMut::new(),
            capture: self.capture,
            truncated: false,
            check: self.check,
            result: None,
            error: None,
            ended,
            killed,
        })
    }
}
impl IntoFuture for ExecBuilder {
    type Output = Result<ExecResult>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
/// Clonable write/kill half of an exec session. Writes are serialized and chunked.
/// Dropping an input handle does not send EOF; only dropping the session cancels the RPC.
#[derive(Clone, Debug)]
pub struct ExecInput {
    sender: mpsc::Sender<proto::ExecRequest>,
    ended: CancellationToken,
    client: crate::Client,
    lock: Arc<Mutex<()>>,
    enabled: bool,
    killed: Arc<AtomicBool>,
}
impl ExecInput {
    /// Writes binary stdin in chunks of at most 32 KiB. Manual stdin must be enabled.
    pub async fn write(&self, data: impl AsRef<[u8]>) -> Result<()> {
        if !self.enabled {
            return Err(Error::InvalidArgument(
                "stdin is not enabled for this command".into(),
            ));
        }
        let _guard = self.lock.lock().await;
        for chunk in data.as_ref().chunks(32 * 1024) {
            self.send(proto::exec_request::Type::WriteData(
                proto::exec_request::WriteData {
                    data: Bytes::copy_from_slice(chunk),
                },
            ))
            .await?;
        }
        Ok(())
    }
    /// Requests termination of the remote process. Continue consuming output to receive its exit.
    pub async fn kill(&self) -> Result<()> {
        self.send(proto::exec_request::Type::Kill(
            proto::exec_request::Kill {},
        ))
        .await?;
        Ok(())
    }
    async fn send(&self, message: proto::exec_request::Type) -> Result<()> {
        tokio::select! {biased;
            _=self.client.inner.closed.cancelled()=>Err(Error::Closed),
            _=self.ended.cancelled()=>Err(Error::Protocol("exec session has ended".into())),
            permit=self.sender.reserve()=>{
                let permit=permit.map_err(|_|Error::Protocol("exec input stream has closed".into()))?;
                // Set the flag before enqueueing, so a very fast Exit cannot race it.
                if matches!(message,proto::exec_request::Type::Kill(_)){self.killed.store(true,Ordering::Release);}
                permit.send(proto::ExecRequest{r#type:Some(message)});Ok(())
            },
        }
    }
}
/// An owned command output stream with bounded capture and an independent input half.
///
/// Import `StreamExt` to consume events. [`Self::wait`] drains remaining events;
/// when an Exit is received the RPC is released immediately. Dropping this
/// session cancels its RPC. Remote command termination follows the server's
/// cancellation behavior; use `input().kill()` for an explicit kill request.
#[derive(Debug)]
#[must_use = "consume the session or call wait"]
pub struct ExecSession {
    output: Option<EventStream<proto::ExecResponse>>,
    input: ExecInput,
    stdout: BytesMut,
    stderr: BytesMut,
    capture: usize,
    truncated: bool,
    check: bool,
    result: Option<ExecResult>,
    error: Option<Error>,
    ended: CancellationToken,
    killed: Arc<AtomicBool>,
}
impl ExecSession {
    /// Obtains a clonable write/kill handle that can be used concurrently with output consumption.
    pub fn input(&self) -> ExecInput {
        self.input.clone()
    }
    /// Returns the completed result after an Exit event, before checking nonzero status.
    pub fn result(&self) -> Option<&ExecResult> {
        self.result.as_ref()
    }
    /// Drains remaining output and returns the result, applying this builder's check setting.
    pub async fn wait(mut self) -> Result<ExecResult> {
        while let Some(event) = self.next().await {
            event?;
        }
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        let r = self
            .result
            .take()
            .ok_or_else(|| Error::Protocol("execution ended without an exit event".into()))?;
        if self.check { r.check() } else { Ok(r) }
    }
    fn finish(&mut self) {
        self.output = None;
        self.ended.cancel();
    }
}
fn capture(target: &mut BytesMut, data: &Bytes, limit: usize) -> bool {
    let remaining = limit.saturating_sub(target.len());
    target.extend_from_slice(&data[..data.len().min(remaining)]);
    data.len() > remaining
}
fn remember(error: &Error) -> Error {
    match error {
        Error::Closed => Error::Closed,
        Error::DeadlineExceeded => Error::DeadlineExceeded,
        Error::Status(s) => Error::Status(s.clone()),
        _ => Error::Protocol(error.to_string()),
    }
}
impl Stream for ExecSession {
    type Item = Result<ExecEvent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(output) = self.output.as_mut() else {
                return Poll::Ready(None);
            };
            match Pin::new(output).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let err =
                        Error::Protocol("execution stream closed without an exit event".into());
                    self.error = Some(remember(&err));
                    self.finish();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Err(err))) => {
                    self.error = Some(remember(&err));
                    self.finish();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(message))) => {
                    let limit = self.capture;
                    let event = match message.r#type {
                        Some(proto::exec_response::Type::Stdout(data)) => {
                            self.truncated |= capture(&mut self.stdout, &data.data, limit);
                            ExecEvent::Stdout(data.data)
                        }
                        Some(proto::exec_response::Type::Stderr(data)) => {
                            self.truncated |= capture(&mut self.stderr, &data.data, limit);
                            ExecEvent::Stderr(data.data)
                        }
                        Some(proto::exec_response::Type::Exit(e)) => {
                            let result = ExecResult {
                                exit_code: e.code,
                                stdout: self.stdout.split().freeze(),
                                stderr: self.stderr.split().freeze(),
                                truncated: self.truncated,
                                killed: self.killed.load(Ordering::Acquire),
                            };
                            self.result = Some(result);
                            self.finish();
                            ExecEvent::Exit(e.code)
                        }
                        None => continue,
                    };
                    return Poll::Ready(Some(Ok(event)));
                }
            }
        }
    }
}
impl Drop for ExecSession {
    fn drop(&mut self) {
        self.ended.cancel();
    }
}
