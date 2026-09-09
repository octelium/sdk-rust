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

//! The official Rust SDK for [Octelium](https://octelium.com).
//!
//! A [`Client`] authenticates to an Octelium Cluster, keeps the resulting
//! Session fresh, and hands out ready-to-use gRPC and HTTP clients:
//!
//! ```no_run
//! use octelium::Client;
//! use octelium_apis::corev1;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .domain("example.com")
//!     .authenticator(octelium::AuthenticationToken::new("<authentication token>"))
//!     .build()
//!     .await?;
//!
//! let users = client
//!     .core_v1()
//!     .list_user(corev1::ListUserOptions::default())
//!     .await?
//!     .into_inner();
//!
//! for user in users.items {
//!     println!("{}", user.metadata.unwrap_or_default().name);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Credentials
//!
//! With no explicit credentials the Client reads them from the environment, in
//! this order:
//!
//! | Variable | Meaning |
//! | --- | --- |
//! | `OCTELIUM_ACCESS_TOKEN` | An externally managed access token, used as-is. |
//! | `OCTELIUM_ASSERTION_FILE` | A file holding a signed assertion, re-read on every authentication. |
//! | `OCTELIUM_ASSERTION` | A signed assertion. |
//! | `OCTELIUM_AUTH_TOKEN` | A Credential authentication token. |
//!
//! `OCTELIUM_DOMAIN`, `OCTELIUM_API_ENDPOINT` and `OCTELIUM_TLS_SERVER_NAME`
//! configure the Cluster and its endpoint. An application can supply its own
//! [`Authenticator`] or [`AccessTokenProvider`] instead, which is how
//! authentication methods added after this release are used.
//!
//! # Sessions
//!
//! The Client obtains an access token on the first call that needs one, and
//! replaces it shortly before it expires. Concurrent callers share one
//! in-flight authentication. A Session created from a reusable credential,
//! such as an assertion, is re-created after it expires; one created from a
//! one-time authentication token is not, and the Client then returns
//! [`Error::SessionExpired`].
//!
//! # Cloning and shutdown
//!
//! [`Client`] is cheap to clone and safe to share across tasks: every clone
//! shares one Session, one token cache and one HTTP/2 connection. Create one
//! per Cluster for the lifetime of the process. The connection is released
//! when the last clone is dropped; [`Client::close`] additionally drops the
//! tokens and makes later calls fail with [`Error::Closed`].
//!
//! # Features
//!
//! | Feature | Default | Description |
//! | --- | --- | --- |
//! | `http` | yes | [`Client::http`], an HTTP client for Octelium Services. |
//! | `tls-native-roots` | yes | Verify Cluster certificates against the host's certificate store. |
//! | `tls-webpki-roots` | no | Verify Cluster certificates against the bundled webpki roots. |

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(clippy::all)]

mod builder;
mod client;
mod config;
mod credentials;
mod domain;
mod env;
mod error;
mod grpc;
mod tls;
mod token;

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
pub mod http;

pub use crate::builder::ClientBuilder;
pub use crate::client::Client;
pub use crate::credentials::{
    AccessTokenProvider, AccessTokenProviderFn, Assertion, AuthenticationToken, Authenticator,
    StaticAccessToken,
};
pub use crate::error::{BoxError, Error, Result};
pub use crate::grpc::{
    AuthServiceClient, AuthenticatedChannel, SessionChannel, METADATA_KEY_AUTH,
    METADATA_KEY_REFRESH_TOKEN,
};
pub use crate::token::AccessToken;

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
pub use crate::http::{HttpAuthorizationPolicy, HttpClient, RequestBuilder};

/// The generated Octelium protobuf types and gRPC clients.
pub use octelium_apis as apis;

pub use tonic;

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
pub use reqwest;
