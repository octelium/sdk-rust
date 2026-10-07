// Copyright Octelium Labs, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::sync::Arc;

/// A type-erased error, used wherever an application supplies its own failure,
/// such as an [`Authenticator`](crate::Authenticator) or an
/// [`AccessTokenProvider`](crate::AccessTokenProvider).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A `Result` alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The errors returned by this crate.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The [`Client`](crate::Client) was closed.
    Closed,

    /// No authentication method was configured and the environment held no
    /// credentials.
    NoCredentials,

    /// The operation needs a Cluster Session, but the Client uses an
    /// externally managed access token.
    NoManagedSession,

    /// The Session expired and the configured authenticator cannot create a
    /// replacement. One-time authentication tokens are never reused.
    SessionExpired,

    #[doc = "The externally managed token was rejected and its provider cannot replace it."]
    AccessTokenRejected,

    #[doc = "The operation exceeded its deadline."]
    DeadlineExceeded,

    #[doc = "The Cluster returned an invalid token response."]
    Protocol(String),

    #[doc = "The local Session changed while an exchange was in flight."]
    SessionChanged,

    /// The Client was configured with conflicting or invalid options.
    Config(String),

    /// A Cluster Session could not be created.
    Authentication(Arc<dyn std::error::Error + Send + Sync>),

    /// An existing Cluster Session could not be refreshed.
    Refresh(Arc<dyn std::error::Error + Send + Sync>),

    /// The SDK refused to attach the access token to an HTTP destination.
    HttpAuthorization {
        /// The rejected URL's origin, without userinfo, path, query or fragment.
        url: String,
        /// Why the destination was rejected.
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    /// The gRPC transport could not be created or connected.
    Transport(Arc<tonic::transport::Error>),

    /// A gRPC call failed.
    Status(tonic::Status),

    /// An HTTP call failed.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    Http(Arc<reqwest::Error>),

    /// A credential could not be read.
    Io(Arc<std::io::Error>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("octelium: the client is closed"),
            Self::NoCredentials => f.write_str("octelium: no credentials were provided: use ClientBuilder::authenticator, ClientBuilder::access_token_provider, OCTELIUM_ACCESS_TOKEN, OCTELIUM_ASSERTION_FILE, OCTELIUM_ASSERTION or OCTELIUM_AUTH_TOKEN"),
            Self::NoManagedSession => f.write_str("octelium: the client does not own a Cluster Session"),
            Self::SessionExpired => f.write_str("octelium: the Session expired and the configured authenticator cannot be reused"),
            Self::AccessTokenRejected => f.write_str("octelium: the fixed access token was rejected; create a client with fresh credentials"),
            Self::DeadlineExceeded => f.write_str("octelium: the operation exceeded its deadline"),
            Self::Protocol(message) => write!(f, "octelium: invalid token response: {message}"),
            Self::SessionChanged => f.write_str("octelium: the local Session changed during authentication"),
            Self::Config(message) => write!(f, "octelium: {message}"),
            Self::Authentication(source) => write!(f, "octelium: could not authenticate: {source}"),
            Self::Refresh(source) => write!(f, "octelium: could not refresh the Session: {source}"),
            Self::HttpAuthorization { url, source } => write!(f, "octelium: HTTP destination is not authorized to receive the access token: {url}: {source}"),
            Self::Transport(source) => write!(f, "octelium: gRPC transport error: {source}"),
            Self::Status(source) => write!(f, "octelium: gRPC error: {source}"),
            #[cfg(feature = "http")]
            Self::Http(source) => write!(f, "octelium: HTTP error: {source}"),
            Self::Io(source) => write!(f, "octelium: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Authentication(source)
            | Self::Refresh(source)
            | Self::HttpAuthorization { source, .. } => Some(source.as_ref()),
            Self::Transport(source) => Some(source.as_ref()),
            Self::Status(source) => Some(source),
            #[cfg(feature = "http")]
            Self::Http(source) => Some(source.as_ref()),
            Self::Io(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Self::Status(status)
    }
}

impl Error {
    pub(crate) fn config(msg: impl fmt::Display) -> Self {
        Self::Config(msg.to_string())
    }

    pub(crate) fn authentication(err: impl Into<BoxError>) -> Self {
        Self::Authentication(Arc::from(err.into()))
    }

    /// Reports whether the error means the Client holds no usable credentials
    /// and will not obtain any without being reconfigured.
    pub fn is_credentials_error(&self) -> bool {
        matches!(
            self,
            Self::NoCredentials | Self::SessionExpired | Self::AccessTokenRejected
        )
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::Transport(Arc::new(err))
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Http(Arc::new(err.without_url()))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(Arc::new(err))
    }
}

/// A simple error carrying only a message, used for the SDK's own failures
/// that an application may see through [`Error::Authentication`] and friends.
#[derive(Debug)]
pub(crate) struct Message(pub(crate) String);

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Message {}

pub(crate) fn message(msg: impl fmt::Display) -> BoxError {
    Box::new(Message(msg.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_authentication_errors_preserve_their_concrete_source() {
        let error = Error::authentication(tonic::Status::permission_denied("original status"));
        let cloned = error.clone();
        let source = std::error::Error::source(&cloned).unwrap();
        assert_eq!(
            source.downcast_ref::<tonic::Status>().unwrap().code(),
            tonic::Code::PermissionDenied
        );
    }
}
