use crate::{Bytes, Error, ExecEvent, Result, Workspace, shell_quote};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Files transferred through the Workspace shell, with literal paths and bounded memory.
///
/// These helpers require POSIX `sh`, `head`, `base64`, `cat`, `mkdir`, and `rm`.
/// Remote writes can leave partial files after failure. Local downloads use a
/// private temporary file and replace the destination only after successful exit.
#[derive(Clone, Debug)]
pub struct Files {
    workspace: Workspace,
    root: bool,
    timeout: Option<std::time::Duration>,
    max_read: usize,
}
impl Files {
    pub(crate) fn new(workspace: Workspace) -> Self {
        Self {
            workspace,
            root: false,
            timeout: Some(std::time::Duration::from_secs(300)),
            max_read: 16 * 1024 * 1024,
        }
    }
    /// Runs transfer commands as root.
    pub fn as_root(mut self, value: bool) -> Self {
        self.root = value;
        self
    }
    /// Sets the total transfer deadline, default five minutes. `None` disables it.
    pub fn timeout(mut self, value: Option<std::time::Duration>) -> Self {
        self.timeout = value;
        self
    }
    /// Sets the in-memory read bound, default 16 MiB. Use downloads for larger files.
    pub fn max_read_bytes(mut self, value: usize) -> Self {
        self.max_read = value;
        self
    }
    /// Reads binary data, failing rather than silently truncating files above the read bound.
    pub async fn read(&self, path: &str) -> Result<Bytes> {
        self.validate()?;
        let path = remote_path(path)?;
        let bound = self
            .max_read
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("read bound is too large".into()))?;
        let result = self
            .workspace
            .exec(format!("head -c {bound} < {}", shell_quote(&path)?))
            .as_root(self.root)
            .timeout(self.timeout)
            .max_capture_bytes(bound)
            .await?;
        if result.stdout.len() > self.max_read || result.truncated {
            return Err(Error::LimitExceeded(format!(
                "file exceeds the {} byte read limit",
                self.max_read
            )));
        }
        Ok(result.stdout)
    }
    /// Reads strict UTF-8 text, returning an error for invalid bytes.
    pub async fn read_text(&self, path: &str) -> Result<String> {
        Ok(String::from_utf8(self.read(path).await?.to_vec())?)
    }
    /// Writes binary data, creating parent directories. Existing remote content is replaced.
    pub async fn write(&self, path: &str, data: impl AsRef<[u8]>) -> Result<()> {
        let data = data.as_ref();
        self.upload_reader(path, data.len() as u64, data).await
    }
    /// Writes UTF-8 text without shell interpolation.
    pub async fn write_text(&self, path: &str, text: &str) -> Result<()> {
        self.write(path, text.as_bytes()).await
    }
    /// Uploads a local regular file using bounded chunks. The file must not change during upload.
    pub async fn upload(&self, local: impl AsRef<Path>, remote: &str) -> Result<()> {
        self.validate()?;
        remote_path(remote)?;
        self.workspace
            .client
            .operation(self.timeout, async {
                let file = tokio::fs::File::open(local).await?;
                let md = file.metadata().await?;
                if !md.is_file() {
                    return Err(Error::InvalidArgument(
                        "upload source must be a regular file".into(),
                    ));
                }
                let size = md.len();
                self.upload_reader(remote, size, file).await
            })
            .await
    }
    /// Uploads exactly `size` bytes from an async reader; extra bytes are left unread.
    /// The protocol has no stdin EOF, so base64 framing communicates the exact length.
    pub async fn upload_reader<R>(&self, path: &str, size: u64, mut reader: R) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
    {
        self.validate()?;
        let path = remote_path(path)?;
        let encoded = size
            .checked_add(2)
            .and_then(|n| (n / 3).checked_mul(4))
            .ok_or_else(|| Error::InvalidArgument("upload size is too large".into()))?;
        let parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or(".");
        let command = format!(
            "mkdir -p {} && head -c {encoded} | base64 -d > {}",
            shell_quote(if parent.is_empty() { "/" } else { parent })?,
            shell_quote(&path)?
        );
        self.workspace
            .client
            .operation(self.timeout, async {
                let session = self
                    .workspace
                    .exec(command)
                    .as_root(self.root)
                    .timeout(self.timeout)
                    .max_capture_bytes(8192)
                    .stdin_enabled(true)
                    .stream()
                    .await?;
                let input = session.input();
                let sender = async {
                    let mut remaining = size;
                    let mut chunk = vec![0; 24 * 1024];
                    while remaining > 0 {
                        let n = usize::try_from(remaining.min(chunk.len() as u64))
                            .map_err(|_| Error::InvalidArgument("upload size overflow".into()))?;
                        reader.read_exact(&mut chunk[..n]).await?;
                        input.write(STANDARD.encode(&chunk[..n])).await?;
                        remaining -= n as u64;
                    }
                    Ok::<_, Error>(())
                };
                let (_, result) = tokio::try_join!(sender, session.wait())?;
                result.check()?;
                Ok(())
            })
            .await
    }
    /// Streams a remote file into an async writer, draining stderr and checking process exit.
    pub async fn download_to<W>(&self, remote: &str, mut writer: W) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        self.validate()?;
        let path = remote_path(remote)?;
        self.workspace
            .client
            .operation(self.timeout, async {
                let mut session = self
                    .workspace
                    .exec(format!("cat < {}", shell_quote(&path)?))
                    .as_root(self.root)
                    .timeout(self.timeout)
                    .max_capture_bytes(8192)
                    .stream()
                    .await?;
                while let Some(event) = session.next().await {
                    if let ExecEvent::Stdout(data) = event? {
                        writer.write_all(&data).await?;
                    }
                }
                session.wait().await?;
                writer.flush().await?;
                Ok(())
            })
            .await
    }
    /// Atomically replaces a local destination after successful download. Failure preserves existing content.
    /// The temporary file is created in the destination directory with private permissions.
    pub async fn download(&self, remote: &str, local: impl AsRef<Path>) -> Result<()> {
        self.validate()?;
        remote_path(remote)?;
        self.workspace
            .client
            .operation(self.timeout, async {
                let local = local.as_ref().to_path_buf();
                if local.as_os_str().is_empty() {
                    return Err(Error::InvalidArgument("local path is empty".into()));
                }
                let parent = local
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .to_path_buf();
                let temp = tokio::task::spawn_blocking(move || {
                    std::fs::create_dir_all(&parent)?;
                    tempfile::NamedTempFile::new_in(parent)
                })
                .await
                .map_err(|e| Error::Protocol(format!("temporary file task failed: {e}")))??;
                let mut file = tokio::fs::File::from_std(temp.reopen()?);
                self.download_to(remote, &mut file).await?;
                file.sync_all().await?;
                drop(file);
                // The short final rename is synchronous so cancellation cannot race a detached commit task.
                temp.persist(&local).map_err(|e| Error::Io(e.error))?;
                Ok(())
            })
            .await
    }
    /// Creates a remote directory and all parents using a literal path.
    pub async fn mkdir(&self, path: &str) -> Result<()> {
        self.validate()?;
        let path = remote_path(path)?;
        self.workspace
            .exec(format!("mkdir -p {}", shell_quote(&path)?))
            .as_root(self.root)
            .timeout(self.timeout)
            .await?;
        Ok(())
    }
    /// Removes a remote file, or a directory tree when `recursive` is true.
    pub async fn remove(&self, path: &str, recursive: bool) -> Result<()> {
        self.validate()?;
        let path = remote_path(path)?;
        self.workspace
            .exec(format!(
                "rm {} {}",
                if recursive { "-rf" } else { "-f" },
                shell_quote(&path)?
            ))
            .as_root(self.root)
            .timeout(self.timeout)
            .await?;
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        if self.timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::InvalidArgument(
                "file timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}
fn remote_path(path: &str) -> Result<String> {
    crate::error::nonempty(path, "remote path")?;
    Ok(if path.starts_with('/') || path.starts_with("./") {
        path.into()
    } else {
        format!("./{path}")
    })
}
