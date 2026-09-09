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

/// A type-erased error, used wherever an application supplies its own failure,
/// such as an [`Authenticator`](crate::Authenticator) or an
/// [`AccessTokenProvider`](crate::AccessTokenProvider).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A `Result` alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The errors returned by this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The [`Client`](crate::Client) was closed.
    #[error("octelium: the client is closed")]
    Closed,

    /// No authentication method was configured and the environment held no
    /// credentials.
    #[error(
        "octelium: no credentials were provided: use ClientBuilder::authenticator, \
         ClientBuilder::access_token_provider, OCTELIUM_ACCESS_TOKEN, OCTELIUM_ASSERTION_FILE, \
         OCTELIUM_ASSERTION or OCTELIUM_AUTH_TOKEN"
    )]
    NoCredentials,

    /// The operation needs a Cluster Session, but the Client uses an
    /// externally managed access token.
    #[error("octelium: the client does not own a Cluster Session")]
    NoManagedSession,

    /// The Session expired and the configured authenticator cannot create a
    /// replacement. One-time authentication tokens are never reused.
    #[error("octelium: the Session expired and the configured authenticator cannot be reused")]
    SessionExpired,

    /// The Client was configured with conflicting or invalid options.
    #[error("octelium: {0}")]
    Config(String),

    /// A Cluster Session could not be created.
    #[error("octelium: could not authenticate: {0}")]
    Authentication(#[source] BoxError),

    /// An existing Cluster Session could not be refreshed. The Client keeps
    /// its Session state so a later call can retry.
    #[error("octelium: could not refresh the Session: {0}")]
    Refresh(#[source] BoxError),

    /// The SDK refused to attach the access token to an HTTP destination.
    #[error(
        "octelium: HTTP destination is not authorized to receive the access token: {url}: {source}"
    )]
    HttpAuthorization {
        /// The rejected URL, with any userinfo redacted.
        url: String,
        /// Why the destination was rejected.
        #[source]
        source: BoxError,
    },

    /// The gRPC transport could not be created or connected.
    #[error("octelium: gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// A gRPC call failed.
    #[error("octelium: gRPC error: {0}")]
    Status(#[from] tonic::Status),

    /// An HTTP call failed.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    #[error("octelium: HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// A credential could not be read.
    #[error("octelium: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn config(msg: impl fmt::Display) -> Self {
        Self::Config(msg.to_string())
    }

    pub(crate) fn authentication(err: impl Into<BoxError>) -> Self {
        Self::Authentication(err.into())
    }

    /// Reports whether the error means the Client holds no usable credentials
    /// and will not obtain any without being reconfigured.
    pub fn is_credentials_error(&self) -> bool {
        matches!(self, Self::NoCredentials | Self::SessionExpired)
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
