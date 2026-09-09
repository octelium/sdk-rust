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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ::http::header::{HeaderName, HeaderValue};
use octelium_apis::authv1::main_service_client::MainServiceClient;
use tonic::body::Body;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tower_service::Service;

use crate::client::Client;
use crate::error::BoxError;
use crate::token::TokenManager;

/// The gRPC metadata key carrying the Octelium access token.
pub const METADATA_KEY_AUTH: &str = "x-octelium-auth";

/// The gRPC metadata key carrying a Cluster Session's refresh token.
pub const METADATA_KEY_REFRESH_TOKEN: &str = "x-octelium-refresh-token";

const HEADER_AUTH: HeaderName = HeaderName::from_static(METADATA_KEY_AUTH);
const HEADER_REFRESH_TOKEN: HeaderName = HeaderName::from_static(METADATA_KEY_REFRESH_TOKEN);

/// A gRPC status is reported in the response headers when the server replies
/// with a trailers-only response, which is how an unauthenticated call is
/// rejected before any message is produced.
const HEADER_GRPC_STATUS: HeaderName = HeaderName::from_static("grpc-status");

type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<::http::Response<Body>, BoxError>> + Send>>;

/// The authentication client handed to an [`Authenticator`](crate::Authenticator).
pub type AuthServiceClient = MainServiceClient<SessionChannel>;

/// A [`Channel`] that attaches the Octelium access token to every call,
/// authenticating or refreshing the Cluster Session as needed.
///
/// It implements [`tower_service::Service`], so it can be passed to any
/// generated `octelium-apis` client.
#[derive(Clone, Debug)]
pub struct AuthenticatedChannel {
    inner: Channel,
    client: Client,
}

impl AuthenticatedChannel {
    pub(crate) fn new(inner: Channel, client: Client) -> Self {
        Self { inner, client }
    }
}

impl Service<::http::Request<Body>> for AuthenticatedChannel {
    type Response = ::http::Response<Body>;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: ::http::Request<Body>) -> Self::Future {
        // `poll_ready` reserved capacity on `self.inner`, so the readiness has
        // to move into the future with the request.
        let clone = self.inner.clone();
        let ready = std::mem::replace(&mut self.inner, clone);
        let client = self.client.clone();

        Box::pin(async move {
            let mut inner = ready;

            let token = match client.token().await {
                Ok(token) => token,
                Err(err) => return Err(Box::new(status_from_error(err)) as BoxError),
            };

            let value = HeaderValue::from_str(&token.value).map_err(|_| {
                Status::unauthenticated("octelium: the access token is not a valid header value")
            })?;
            req.headers_mut().insert(HEADER_AUTH, value);

            let resp = inner.call(req).await?;

            // A rejected call invalidates the cached token so the next one
            // authenticates again instead of replaying a stale token.
            if grpc_code(resp.headers()) == Some(Code::Unauthenticated) {
                client.invalidate_access_token();
            }

            Ok(resp)
        })
    }
}

/// A [`Channel`] that attaches the Cluster Session's refresh token, used for
/// the authentication API itself.
#[derive(Clone, Debug)]
pub struct SessionChannel {
    inner: Channel,
    tokens: Arc<TokenManager>,
}

impl SessionChannel {
    pub(crate) fn new(inner: Channel, tokens: Arc<TokenManager>) -> Self {
        Self { inner, tokens }
    }
}

impl Service<::http::Request<Body>> for SessionChannel {
    type Response = ::http::Response<Body>;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: ::http::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let ready = std::mem::replace(&mut self.inner, clone);
        let refresh_token = self.tokens.refresh_token();

        Box::pin(async move {
            let mut inner = ready;

            if let Some(refresh_token) = refresh_token {
                let value = HeaderValue::from_str(&refresh_token).map_err(|_| {
                    Status::unauthenticated(
                        "octelium: the refresh token is not a valid header value",
                    )
                })?;
                req.headers_mut().insert(HEADER_REFRESH_TOKEN, value);
            }

            Ok(inner.call(req).await?)
        })
    }
}

/// Reports the gRPC status of a trailers-only response.
fn grpc_code(headers: &::http::HeaderMap) -> Option<Code> {
    let value = headers.get(HEADER_GRPC_STATUS)?;
    let code = value.to_str().ok()?.parse::<i32>().ok()?;
    Some(Code::from(code))
}

/// Surfaces an SDK error to a caller of a generated gRPC client.
///
/// The error is carried as the [`Status`] source, so it can be recovered with
/// `Status::source` and downcast back to [`crate::Error`].
fn status_from_error(err: crate::Error) -> Status {
    let code = match &err {
        crate::Error::Closed => Code::Cancelled,
        crate::Error::Status(status) => status.code(),
        _ => Code::Unauthenticated,
    };

    let mut status = Status::new(code, err.to_string());
    status.set_source(Arc::new(err));
    status
}
