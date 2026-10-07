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
use std::time::Duration;

use ::http::header::{HeaderName, HeaderValue};
use http_body::{Body as HttpBody, Frame, SizeHint};
use octelium_apis::authv1::main_service_client::MainServiceClient;
use octelium_apis::bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tonic::body::Body;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tower_service::Service;

use crate::client::Client;
use crate::error::{BoxError, Error};
use crate::token::TokenManager;

/// The gRPC metadata key carrying the Octelium access token.
pub const METADATA_KEY_AUTH: &str = "x-octelium-auth";

/// The gRPC metadata key carrying a Cluster Session's refresh token.
pub const METADATA_KEY_REFRESH_TOKEN: &str = "x-octelium-refresh-token";

const HEADER_AUTH: HeaderName = HeaderName::from_static(METADATA_KEY_AUTH);
const HEADER_REFRESH_TOKEN: HeaderName = HeaderName::from_static(METADATA_KEY_REFRESH_TOKEN);
const HEADER_GRPC_STATUS: HeaderName = HeaderName::from_static("grpc-status");
const HEADER_GRPC_TIMEOUT: HeaderName = HeaderName::from_static("grpc-timeout");

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
    client: Client,
}

impl AuthenticatedChannel {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

impl Service<::http::Request<Body>> for AuthenticatedChannel {
    type Response = ::http::Response<Body>;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: ::http::Request<Body>) -> Self::Future {
        let client = self.client.clone();
        let timeout = request_timeout(req.headers(), client.request_timeout());
        Box::pin(async move {
            let timeout = timeout.map_err(status_from_error)?;
            let deadline =
                timeout.and_then(|duration| tokio::time::Instant::now().checked_add(duration));
            let cancel = client.cancellation();
            let operation = async {
                let lease = client.token_lease().await.map_err(status_from_error)?;
                let mut inner = client.api_channel().map_err(status_from_error)?;
                std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
                clear_credentials(req.headers_mut());
                let mut value = HeaderValue::from_str(&lease.token.value)
                    .map_err(|_| Status::unauthenticated("octelium: invalid access token"))?;
                value.set_sensitive(true);
                req.headers_mut().insert(HEADER_AUTH, value);
                if let Some(deadline) = deadline {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(Box::new(Status::deadline_exceeded(
                            "octelium: request deadline exceeded",
                        )) as BoxError);
                    }
                    let mut request = tonic::Request::new(());
                    request.set_timeout(remaining);
                    req.headers_mut().insert(
                        HEADER_GRPC_TIMEOUT,
                        request
                            .metadata()
                            .clone()
                            .into_headers()
                            .remove("grpc-timeout")
                            .unwrap(),
                    );
                }
                let response = inner.call(req).await?;
                if grpc_code(response.headers()) == Some(Code::Unauthenticated) {
                    client.invalidate_generation(lease.generation);
                }
                let (parts, body) = response.into_parts();
                let body = Body::new(ObservedBody {
                    inner: body,
                    client: client.clone(),
                    generation: lease.generation,
                    cancelled: Box::pin(cancel.clone().cancelled_owned()),
                    deadline: deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline))),
                    done: false,
                });
                Ok(::http::Response::from_parts(parts, body))
            };
            wait_operation(operation, cancel.clone(), deadline).await
        })
    }
}

struct ObservedBody {
    inner: Body,
    client: Client,
    generation: u64,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    done: bool,
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.done = true;
            self.inner = Body::empty();
            return Poll::Ready(Some(Err(Status::cancelled("octelium: client is closed"))));
        }
        if self
            .deadline
            .as_mut()
            .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
        {
            self.done = true;
            self.inner = Body::empty();
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "octelium: request deadline exceeded",
            ))));
        }
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if frame
                .trailers_ref()
                .is_some_and(|trailers| grpc_code(trailers) == Some(Code::Unauthenticated))
            {
                self.client.invalidate_generation(self.generation);
            }
        }
        if matches!(&result, Poll::Ready(Some(Err(status))) if status.code() == Code::Unauthenticated)
        {
            self.client.invalidate_generation(self.generation);
        }
        if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.done = true;
        }
        result
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.done || self.inner.is_end_stream()
    }
}

/// A [`Channel`] that attaches the Cluster Session's refresh token, used for
/// the authentication API itself.
#[derive(Clone, Debug)]
pub struct SessionChannel {
    inner: Channel,
    tokens: Arc<TokenManager>,
    cancel: CancellationToken,
    authentication: bool,
}

impl SessionChannel {
    pub(crate) fn new(
        inner: Channel,
        tokens: Arc<TokenManager>,
        cancel: CancellationToken,
        authentication: bool,
    ) -> Self {
        Self {
            inner,
            tokens,
            cancel,
            authentication,
        }
    }
}

impl Service<::http::Request<Body>> for SessionChannel {
    type Response = ::http::Response<Body>;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.cancel.is_cancelled() {
            return Poll::Ready(Ok(()));
        }
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: ::http::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut ready = std::mem::replace(&mut self.inner, clone);
        let tokens = self.tokens.clone();
        let cancel = self.cancel.clone();
        let authentication = self.authentication;
        Box::pin(async move {
            let supplied_refresh = req.headers().get(HEADER_REFRESH_TOKEN).cloned();
            clear_credentials(req.headers_mut());
            if let Some(refresh_token) = tokens.refresh_token() {
                let mut value = HeaderValue::from_str(&refresh_token)
                    .map_err(|_| Status::unauthenticated("octelium: invalid refresh token"))?;
                value.set_sensitive(true);
                req.headers_mut().insert(HEADER_REFRESH_TOKEN, value);
            } else if authentication {
                if let Some(mut value) = supplied_refresh {
                    crate::token::validate_value(value.to_str().map_err(|_| {
                        Status::invalid_argument("octelium: invalid refresh token")
                    })?)
                    .map_err(status_from_error)?;
                    value.set_sensitive(true);
                    req.headers_mut().insert(HEADER_REFRESH_TOKEN, value);
                }
            }
            tokens.mark_exchange(authentication);
            wait_operation(async move { Ok(ready.call(req).await?) }, cancel, None).await
        })
    }
}

async fn wait_operation<F, T>(
    operation: F,
    cancel: CancellationToken,
    deadline: Option<tokio::time::Instant>,
) -> Result<T, BoxError>
where
    F: Future<Output = Result<T, BoxError>>,
{
    let expired = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Box::new(Status::cancelled("octelium: client is closed"))),
        _ = expired => Err(Box::new(Status::deadline_exceeded("octelium: request deadline exceeded"))),
        result = operation => result,
    }
}

fn clear_credentials(headers: &mut ::http::HeaderMap) {
    for name in [
        "authorization",
        "cookie",
        METADATA_KEY_AUTH,
        METADATA_KEY_REFRESH_TOKEN,
    ] {
        headers.remove(name);
    }
}

fn request_timeout(
    headers: &::http::HeaderMap,
    default: Option<Duration>,
) -> crate::Result<Option<Duration>> {
    let Some(value) = headers.get(HEADER_GRPC_TIMEOUT) else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .map_err(|_| Error::config("invalid grpc-timeout"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Error::config("invalid grpc-timeout"));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::config("invalid grpc-timeout"));
    }
    let count: u64 = digits
        .parse()
        .map_err(|_| Error::config("invalid grpc-timeout"))?;
    let multiplier = match unit {
        "H" => 3_600_000_000_000,
        "M" => 60_000_000_000,
        "S" => 1_000_000_000,
        "m" => 1_000_000,
        "u" => 1_000,
        "n" => 1,
        _ => return Err(Error::config("invalid grpc-timeout")),
    };
    let nanos = count
        .checked_mul(multiplier)
        .ok_or_else(|| Error::config("grpc-timeout is too large"))?;
    Ok(Some(Duration::from_nanos(nanos)))
}

fn grpc_code(headers: &::http::HeaderMap) -> Option<Code> {
    let value = headers.get(HEADER_GRPC_STATUS)?;
    Some(Code::from(value.to_str().ok()?.parse::<i32>().ok()?))
}

pub(crate) fn error_code(err: &(dyn std::error::Error + 'static)) -> Option<Code> {
    if let Some(status) = err.downcast_ref::<Status>() {
        return Some(status.code());
    }
    if let Some(err) = err.downcast_ref::<Error>() {
        match err {
            Error::Authentication(source) | Error::Refresh(source) => {
                return error_code(source.as_ref())
            }
            Error::Closed | Error::SessionChanged => return Some(Code::Cancelled),
            Error::DeadlineExceeded => return Some(Code::DeadlineExceeded),
            Error::Config(_) => return Some(Code::InvalidArgument),
            Error::Protocol(_) => return Some(Code::DataLoss),
            Error::Transport(_) | Error::Io(_) => return Some(Code::Unavailable),
            #[cfg(feature = "http")]
            Error::Http(_) => return Some(Code::Unavailable),
            Error::NoCredentials
            | Error::NoManagedSession
            | Error::SessionExpired
            | Error::AccessTokenRejected => return Some(Code::Unauthenticated),
            _ => {}
        }
    }
    err.source().and_then(error_code)
}

fn status_from_error(err: Error) -> Status {
    let code = error_code(&err).unwrap_or(Code::Unavailable);
    let mut status = Status::new(
        code,
        match code {
            Code::Unauthenticated => "octelium: credentials were rejected or are unavailable",
            Code::DeadlineExceeded => "octelium: operation deadline exceeded",
            Code::Cancelled => "octelium: operation was cancelled",
            Code::InvalidArgument => "octelium: invalid configuration",
            Code::DataLoss => "octelium: invalid token response",
            _ => "octelium: authentication service or token provider is unavailable",
        },
    );
    status.set_source(Arc::new(err));
    status
}
