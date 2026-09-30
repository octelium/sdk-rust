use crate::{ExecResult, proto};

/// Result returned by Cordium SDK operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An SDK, transport, API, or command failure. New variants may be added.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A local argument failed validation before its RPC was sent.
    #[error("cordium: invalid argument: {0}")]
    InvalidArgument(String),
    /// Client shutdown canceled the operation.
    #[error("cordium: client is closed")]
    Closed,
    /// The total operation deadline expired.
    #[error("cordium: operation deadline exceeded")]
    DeadlineExceeded,
    /// A gRPC call returned a status, preserving its code, details and metadata.
    #[error("cordium: {0}")]
    Status(#[from] tonic::Status),
    /// Authentication, TLS, or authenticated HTTP transport failed.
    #[error(transparent)]
    Transport(#[from] octelium::Error),
    /// A checked command returned a nonzero exit status. Captured output remains available.
    #[error("cordium: command exited with code {code}", code = .0.exit_code)]
    CommandFailed(Box<ExecResult>),
    /// Workspace startup failed. The resource remains available for inspection.
    #[error("cordium: workspace did not reach the requested state: {message}")]
    WorkspaceFailed {
        /// Last resource reported by the server, including failure details.
        workspace: Box<proto::Workspace>,
        /// Description of the failure.
        message: String,
    },
    /// An asynchronous template build, snapshot or volume failed.
    #[error("cordium: {kind} failed: {message}")]
    ResourceFailed {
        /// Resource kind.
        kind: &'static str,
        /// Server failure description.
        message: String,
    },
    /// A response exceeded an explicit memory bound.
    #[error("cordium: {0}")]
    LimitExceeded(String),
    /// The server sent an inconsistent response, such as an exec stream without an exit.
    #[error("cordium: invalid server response: {0}")]
    Protocol(String),
    /// Local file I/O failed.
    #[error("cordium: file I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Text returned by a binary API was not UTF-8.
    #[error("cordium: invalid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
impl Error {
    /// Returns the gRPC status code, if this error originated from a status.
    pub fn code(&self) -> Option<tonic::Code> {
        match self {
            Self::Status(s) | Self::Transport(octelium::Error::Status(s)) => Some(s.code()),
            Self::Closed => Some(tonic::Code::Cancelled),
            Self::DeadlineExceeded => Some(tonic::Code::DeadlineExceeded),
            Self::InvalidArgument(_) => Some(tonic::Code::InvalidArgument),
            _ => None,
        }
    }
}
pub(crate) fn nonempty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(Error::InvalidArgument(format!(
            "{field} must be nonempty and contain no NUL"
        )));
    }
    Ok(())
}
