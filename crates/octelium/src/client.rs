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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use octelium_apis::{authv1, cordiumv1, corev1, userv1};
use tonic::transport::Channel;
use tonic::Code;

use crate::builder::ClientBuilder;
use crate::config::Config;
use crate::error::{message, Error, Result};
use crate::grpc::{AuthServiceClient, AuthenticatedChannel, SessionChannel};
use crate::token::{AccessToken, TokenManager, DEFAULT_REFRESH_BEFORE};

/// An authenticated Octelium Cluster client.
///
/// A Client owns the Cluster Session, the access token and the shared HTTP/2
/// connection. It is cheap to clone, and every clone shares that state, so an
/// application normally creates one Client per Cluster and shares it for the
/// lifetime of the process.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// use octelium::Client;
/// use octelium_apis::corev1;
///
/// let client = Client::builder().domain("example.com").build().await?;
///
/// let users = client
///     .core_v1()
///     .list_user(corev1::ListUserOptions::default())
///     .await?
///     .into_inner();
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    cfg: Config,
    tokens: Arc<TokenManager>,
    closed: AtomicBool,

    /// The shared connection to the Cluster API. tonic reconnects on its own,
    /// so it is created lazily and never dialed here.
    channel: Channel,

    /// The authentication client, which carries the Session's refresh token
    /// rather than the access token.
    ///
    /// It has a connection of its own so that a token refresh cannot be
    /// blocked by API calls that are themselves waiting for that token.
    auth: AuthServiceClient,

    #[cfg(feature = "http")]
    http: reqwest::Client,
}

impl Client {
    /// Returns a builder for a Client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Creates a Client for `domain`, taking its credentials from the
    /// environment.
    ///
    /// See [`ClientBuilder`] for the full set of options.
    pub async fn connect(domain: impl Into<String>) -> Result<Self> {
        Self::builder().domain(domain).build().await
    }

    /// Creates a Client entirely from the environment, including the Cluster
    /// domain in `OCTELIUM_DOMAIN`.
    pub async fn from_env() -> Result<Self> {
        Self::builder().build().await
    }

    pub(crate) fn from_parts(
        cfg: Config,
        channel: Channel,
        auth_channel: Channel,
        #[cfg(feature = "http")] http: reqwest::Client,
    ) -> Self {
        let tokens = Arc::new(TokenManager::new());
        let auth = AuthServiceClient::new(SessionChannel::new(auth_channel, tokens.clone()));

        Self {
            inner: Arc::new(Inner {
                cfg,
                tokens,
                closed: AtomicBool::new(false),
                channel,
                auth,
                #[cfg(feature = "http")]
                http,
            }),
        }
    }

    /// Returns the normalized Cluster domain.
    pub fn domain(&self) -> &str {
        &self.inner.cfg.domain
    }

    /// Returns the gRPC endpoint the Client dials.
    pub fn api_endpoint(&self) -> &str {
        &self.inner.cfg.api_endpoint
    }

    /// Returns the default Cluster API address for a domain.
    pub fn api_server_addr(domain: &str) -> String {
        format!("octelium-api.{domain}:443")
    }

    /// Returns a valid access token, authenticating or refreshing the Session
    /// as needed.
    ///
    /// Concurrent callers share one in-flight token operation.
    pub async fn token(&self) -> Result<AccessToken> {
        self.ensure_open()?;

        if let Some(token) = self.inner.tokens.current(Instant::now()) {
            return Ok(token);
        }

        let _guard = self.inner.tokens.refresh_lock.lock().await;

        self.ensure_open()?;
        if let Some(token) = self.inner.tokens.current(Instant::now()) {
            return Ok(token);
        }

        if self.inner.cfg.token_provider.is_some() {
            return self.obtain_external_token().await;
        }

        self.obtain_managed_token().await
    }

    /// Returns the value of a valid access token.
    pub async fn access_token(&self) -> Result<String> {
        Ok(self.token().await?.value)
    }

    /// Causes the next operation to obtain a new access token, without
    /// discarding a managed Session's refresh token.
    pub fn invalidate_access_token(&self) {
        self.inner.tokens.invalidate_access_token();
    }

    /// Terminates the managed Cluster Session.
    ///
    /// It returns [`Error::NoManagedSession`] when the Client uses an
    /// externally managed access token.
    pub async fn logout(&self) -> Result<()> {
        if self.inner.cfg.token_provider.is_some() {
            return Err(Error::NoManagedSession);
        }
        self.ensure_open()?;

        let _guard = self.inner.tokens.refresh_lock.lock().await;

        self.ensure_open()?;
        if self.inner.tokens.refresh_token().is_none() {
            self.inner.tokens.clear();
            return Ok(());
        }

        let mut auth = self.inner.auth.clone();
        let result = self
            .with_timeout(async move { auth.logout(authv1::LogoutRequest {}).await })
            .await;

        match result {
            Ok(_) => {
                self.inner.tokens.clear();
                Ok(())
            }
            Err(Error::Status(status)) if status.code() == Code::Unauthenticated => {
                // The Session is already gone on the Cluster side.
                self.inner.tokens.clear();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Clears the local token state without contacting the Cluster.
    pub fn forget_session(&self) {
        self.inner.tokens.clear();
    }

    /// Releases the Client.
    ///
    /// Later operations return [`Error::Closed`], and the tokens are dropped.
    /// The connection itself is closed once the last clone of this Client is
    /// dropped. It does not log out.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.tokens.clear();
    }

    /// Reports whether [`close`](Self::close) was called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Returns the authenticated gRPC channel.
    ///
    /// Pass it to any generated `octelium-apis` client. The channel attaches a
    /// valid access token to every call.
    pub fn channel(&self) -> AuthenticatedChannel {
        AuthenticatedChannel::new(self.inner.channel.clone(), self.clone())
    }

    /// Returns the underlying tonic [`Channel`], without authentication.
    ///
    /// This is an escape hatch for calls that must not carry the access token.
    pub fn raw_channel(&self) -> Channel {
        self.inner.channel.clone()
    }

    /// Returns an authenticated `octelium.api.main.core.v1.MainService`
    /// client, the Cluster management API.
    pub fn core_v1(&self) -> corev1::main_service_client::MainServiceClient<AuthenticatedChannel> {
        corev1::main_service_client::MainServiceClient::new(self.channel())
    }

    /// Returns an authenticated `octelium.api.main.user.v1.MainService`
    /// client, the API available to every Cluster User.
    pub fn user_v1(&self) -> userv1::main_service_client::MainServiceClient<AuthenticatedChannel> {
        userv1::main_service_client::MainServiceClient::new(self.channel())
    }

    /// Returns an authenticated `octelium.api.main.auth.v1.MainService`
    /// client, used to manage Authenticators and Devices.
    pub fn auth_v1(&self) -> authv1::main_service_client::MainServiceClient<AuthenticatedChannel> {
        authv1::main_service_client::MainServiceClient::new(self.channel())
    }

    /// Returns an authenticated `octelium.api.main.cordium.v1.MainService`
    /// client.
    pub fn cordium_v1(
        &self,
    ) -> cordiumv1::main_service_client::MainServiceClient<AuthenticatedChannel> {
        cordiumv1::main_service_client::MainServiceClient::new(self.channel())
    }

    /// Returns an authenticated `octelium.api.main.cordium.v1.WorkspaceService`
    /// client.
    pub fn cordium_v1_workspace(
        &self,
    ) -> cordiumv1::workspace_service_client::WorkspaceServiceClient<AuthenticatedChannel> {
        cordiumv1::workspace_service_client::WorkspaceServiceClient::new(self.channel())
    }

    /// Returns an authenticated `octelium.api.main.cordium.v1.ManagementService`
    /// client.
    pub fn cordium_v1_management(
        &self,
    ) -> cordiumv1::management_service_client::ManagementServiceClient<AuthenticatedChannel> {
        cordiumv1::management_service_client::ManagementServiceClient::new(self.channel())
    }

    /// Returns an HTTP client that attaches the access token to Octelium
    /// Services.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn http(&self) -> crate::http::HttpClient {
        crate::http::HttpClient::new(self.clone(), self.inner.http.clone())
    }

    #[cfg(feature = "http")]
    pub(crate) fn http_config(&self) -> &crate::http::HttpConfig {
        &self.inner.cfg.http
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        Ok(())
    }

    async fn obtain_external_token(&self) -> Result<AccessToken> {
        let provider = self
            .inner
            .cfg
            .token_provider
            .clone()
            .ok_or(Error::NoCredentials)?;

        let token = match self
            .with_timeout(async move { provider.token().await.map_err(Error::authentication) })
            .await
        {
            Ok(token) => token,
            Err(err) => {
                // A provider that is briefly unavailable must not invalidate a
                // token that is still good.
                if let Some(current) = self.inner.tokens.usable(Instant::now()) {
                    tracing::warn!(
                        error = %err,
                        "could not proactively replace the access token; using the still-valid token"
                    );
                    return Ok(current);
                }
                return Err(Error::authentication(err));
            }
        };

        let token = AccessToken {
            value: token.value.trim().to_string(),
            expires_at: token.expires_at,
        };

        if token.value.is_empty() {
            return Err(Error::authentication(message(
                "the access token provider returned an empty token",
            )));
        }
        if token
            .expires_at
            .is_some_and(|expires_at| expires_at <= std::time::SystemTime::now())
        {
            return Err(Error::authentication(message(
                "the access token provider returned an expired token",
            )));
        }

        let now = Instant::now();
        self.inner
            .tokens
            .set_external(&token, now, DEFAULT_REFRESH_BEFORE);
        self.ensure_open()?;

        self.inner.tokens.current(now).ok_or_else(|| {
            Error::authentication(message("the access token became stale immediately"))
        })
    }

    async fn obtain_managed_token(&self) -> Result<AccessToken> {
        let snapshot = self.inner.tokens.snapshot();

        if !snapshot.refresh_token.is_empty() {
            match self.refresh_session(&snapshot.refresh_token).await {
                Ok(token) => return Ok(token),
                Err(err) => {
                    if !is_unauthenticated(&err) {
                        if let Some(current) = self.inner.tokens.usable(Instant::now()) {
                            tracing::warn!(
                                error = %err,
                                "could not proactively refresh the Session; using the still-valid access token"
                            );
                            return Ok(current);
                        }
                        return Err(Error::Refresh(Box::new(err)));
                    }

                    // The refresh token is gone, so only a full
                    // reauthentication can recover the Session.
                    self.inner.tokens.clear();
                    if !self.can_reauthenticate() {
                        return Err(Error::SessionExpired);
                    }
                }
            }
        }

        if self.inner.tokens.has_ever_authenticated() && !self.can_reauthenticate() {
            return Err(Error::SessionExpired);
        }

        self.authenticate().await
    }

    async fn refresh_session(&self, previous_refresh_token: &str) -> Result<AccessToken> {
        let mut auth = self.inner.auth.clone();

        let token = self
            .with_timeout(async move {
                auth.authenticate_with_refresh_token(authv1::AuthenticateWithRefreshTokenRequest {})
                    .await
            })
            .await?
            .into_inner();

        validate_session_token(&token)?;

        let now = Instant::now();
        self.inner
            .tokens
            .set_session(&token, now, DEFAULT_REFRESH_BEFORE, previous_refresh_token);
        self.ensure_open()?;

        self.inner.tokens.current(now).ok_or_else(|| {
            Error::Refresh(message(
                "the refreshed access token became stale immediately",
            ))
        })
    }

    async fn authenticate(&self) -> Result<AccessToken> {
        let authenticator = self
            .inner
            .cfg
            .authenticator
            .clone()
            .ok_or(Error::NoCredentials)?;

        let auth = self.inner.auth.clone();
        let scopes = self.inner.cfg.scopes.clone();

        let token = self
            .with_timeout(async move {
                authenticator
                    .authenticate(auth, &scopes)
                    .await
                    .map_err(Error::authentication)
            })
            .await?;

        validate_session_token(&token)?;

        let now = Instant::now();
        self.inner
            .tokens
            .set_session(&token, now, DEFAULT_REFRESH_BEFORE, "");
        self.ensure_open()?;

        self.inner.tokens.current(now).ok_or_else(|| {
            Error::authentication(message("the access token became stale immediately"))
        })
    }

    fn can_reauthenticate(&self) -> bool {
        self.inner
            .cfg
            .authenticator
            .as_ref()
            .is_some_and(|authenticator| authenticator.can_reauthenticate())
    }

    /// Bounds an authentication or refresh call by the configured timeout.
    async fn with_timeout<F, T, E>(&self, future: F) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, E>>,
        E: Into<Error>,
    {
        let Some(timeout) = self.inner.cfg.authentication_timeout else {
            return future.await.map_err(Into::into);
        };

        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err(Error::authentication(message(format!(
                "the operation did not complete within {timeout:?}"
            )))),
        }
    }
}

fn validate_session_token(token: &authv1::SessionToken) -> Result<()> {
    if token.access_token.trim().is_empty() {
        return Err(Error::authentication(message(
            "the Cluster returned an empty access token",
        )));
    }
    Ok(())
}

/// Reports whether the Cluster rejected the credential itself, rather than
/// failing for a transient reason.
fn is_unauthenticated(err: &Error) -> bool {
    match err {
        Error::Status(status) => status.code() == Code::Unauthenticated,
        _ => false,
    }
}
