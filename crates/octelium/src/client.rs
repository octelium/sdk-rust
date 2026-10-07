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
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use std::time::Instant;
use tokio::sync::watch;
use tokio_util::task::TaskTracker;

use octelium_apis::{authv1, cordiumv1, corev1, userv1};
use tonic::transport::Channel;
use tonic::Code;

use crate::builder::ClientBuilder;
use crate::config::Config;
use crate::error::{message, Error, Result};
use crate::grpc::{AuthServiceClient, AuthenticatedChannel, SessionChannel};
use crate::token::{AccessToken, TokenLease, TokenManager};

/// An authenticated Octelium Cluster client.
///
/// A Client owns the Cluster Session, the access token and separate shared API
/// and authentication channels. It is cheap to clone, and every clone shares that state, so an
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
    close_cancel: tokio_util::sync::CancellationToken,

    resources: RwLock<Option<Resources>>,
    pending: Mutex<Option<watch::Receiver<Option<Result<TokenLease>>>>>,
    tasks: TaskTracker,
}

struct Resources {
    channel: Channel,
    auth_channel: Option<Channel>,
    authenticator: Option<Arc<dyn crate::Authenticator>>,
    token_provider: Option<Arc<dyn crate::AccessTokenProvider>>,
    #[cfg(feature = "http")]
    http: reqwest::Client,
}

impl std::fmt::Debug for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resources")
            .field("managed_session", &self.authenticator.is_some())
            .field("external_tokens", &self.token_provider.is_some())
            .finish_non_exhaustive()
    }
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
        mut cfg: Config,
        channel: Channel,
        auth_channel: Option<Channel>,
        #[cfg(feature = "http")] http: reqwest::Client,
    ) -> Self {
        let tokens = Arc::new(TokenManager::new());
        let resources = Resources {
            channel,
            auth_channel,
            authenticator: cfg.authenticator.take(),
            token_provider: cfg.token_provider.take(),
            #[cfg(feature = "http")]
            http,
        };
        Self {
            inner: Arc::new(Inner {
                cfg,
                tokens,
                closed: AtomicBool::new(false),
                close_cancel: tokio_util::sync::CancellationToken::new(),
                resources: RwLock::new(Some(resources)),
                pending: Mutex::new(None),
                tasks: TaskTracker::new(),
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
        Ok((*self.token_lease().await?.token).clone())
    }

    pub(crate) async fn token_lease(&self) -> Result<TokenLease> {
        self.ensure_open()?;
        if let Some(token) = self.inner.tokens.current(Instant::now()) {
            self.ensure_open()?;
            return Ok(token);
        }
        let mut receiver = {
            let mut pending = self
                .inner
                .pending
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            self.ensure_open()?;
            if pending
                .as_ref()
                .is_some_and(|receiver| receiver.has_changed().is_err())
            {
                *pending = None;
            }
            if let Some(token) = self.inner.tokens.current(Instant::now()) {
                return Ok(token);
            }
            if let Some(receiver) = &*pending {
                receiver.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                *pending = Some(receiver.clone());
                let client = self.clone();
                self.inner.tasks.spawn(async move {
                    let result = tokio::select! {
                        biased;
                        _ = client.inner.close_cancel.cancelled() => Err(Error::Closed),
                        result = client.token_locked() => result,
                    };
                    let mut pending = client
                        .inner
                        .pending
                        .lock()
                        .unwrap_or_else(|err| err.into_inner());
                    sender.send_replace(Some(result));
                    *pending = None;
                });
                receiver
            }
        };
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                self.ensure_open()?;
                return result;
            }
            receiver.changed().await.map_err(|_| {
                Error::authentication(message("the token worker stopped before completing"))
            })?;
        }
    }

    async fn token_locked(&self) -> Result<TokenLease> {
        let _guard = self.inner.tokens.refresh_lock.lock().await;
        self.ensure_open()?;
        if let Some(token) = self.inner.tokens.current(Instant::now()) {
            return Ok(token);
        }
        if self.external_provider()?.is_some() {
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
        if self.external_provider()?.is_some() {
            return Err(Error::NoManagedSession);
        }
        self.ensure_open()?;

        let _guard = self.inner.tokens.refresh_lock.lock().await;

        self.ensure_open()?;
        if self.inner.tokens.refresh_token().is_none() {
            self.inner.tokens.clear();
            return Ok(());
        }

        let mut auth = self.authentication_client(false)?;
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
    /// Later operations return [`Error::Closed`]. Owned tokens, credentials and
    /// transports are released. Use [`shutdown`](Self::shutdown) to wait for
    /// authentication workers. It does not log out.
    pub fn close(&self) {
        let _pending = self
            .inner
            .pending
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.inner.closed.store(true, Ordering::Release);
        self.inner.close_cancel.cancel();
        self.inner.tokens.close();
        self.inner.tasks.close();
        self.inner
            .resources
            .write()
            .unwrap_or_else(|err| err.into_inner())
            .take();
    }

    #[doc = "Closes all clones, releases owned transports and credentials, and waits for authentication workers to stop. It does not log out."]
    pub async fn shutdown(&self) {
        self.close();
        self.inner.tasks.wait().await;
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
        AuthenticatedChannel::new(self.clone())
    }

    /// Returns the underlying tonic [`Channel`], without authentication.
    ///
    /// This is an escape hatch for calls that must not carry the access token.
    pub fn raw_channel(&self) -> Result<Channel> {
        self.api_channel()
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
        crate::http::HttpClient::new(self.clone())
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

    pub(crate) fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.inner.close_cancel.clone()
    }

    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        self.inner.cfg.request_timeout
    }

    pub(crate) fn invalidate_generation(&self, generation: u64) {
        self.inner.tokens.invalidate_generation(generation);
    }

    pub(crate) fn api_channel(&self) -> Result<Channel> {
        self.ensure_open()?;
        self.inner
            .resources
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(|resources| resources.channel.clone())
            .ok_or(Error::Closed)
    }

    fn authentication_client(&self, authentication: bool) -> Result<AuthServiceClient> {
        let channel = self
            .inner
            .resources
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .ok_or(Error::Closed)?
            .auth_channel
            .clone()
            .ok_or(Error::NoManagedSession)?;
        Ok(AuthServiceClient::new(SessionChannel::new(
            channel,
            self.inner.tokens.clone(),
            self.cancellation(),
            authentication,
        )))
    }

    fn external_provider(&self) -> Result<Option<Arc<dyn crate::AccessTokenProvider>>> {
        self.inner
            .resources
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(|resources| resources.token_provider.clone())
            .ok_or(Error::Closed)
    }

    fn authenticator(&self) -> Result<Arc<dyn crate::Authenticator>> {
        self.inner
            .resources
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .ok_or(Error::Closed)?
            .authenticator
            .clone()
            .ok_or(Error::NoCredentials)
    }

    #[cfg(feature = "http")]
    pub(crate) fn http_transport(&self) -> Result<reqwest::Client> {
        self.inner
            .resources
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(|resources| resources.http.clone())
            .ok_or(Error::Closed)
    }

    async fn obtain_external_token(&self) -> Result<TokenLease> {
        let provider = self.external_provider()?.ok_or(Error::NoCredentials)?;
        if self.inner.tokens.has_rejected_access_token() && !provider.can_replace_rejected_token() {
            return Err(Error::AccessTokenRejected);
        }
        let epoch = self.inner.tokens.snapshot().epoch;
        let token = match self
            .with_timeout(async move { provider.token().await.map_err(Error::authentication) })
            .await
        {
            Ok(token) => token,
            Err(err) => {
                if matches!(
                    crate::grpc::error_code(&err),
                    None | Some(
                        Code::Unavailable
                            | Code::Internal
                            | Code::Unknown
                            | Code::DeadlineExceeded
                            | Code::ResourceExhausted
                    )
                ) {
                    if let Some(current) = self.inner.tokens.backoff(epoch) {
                        return Ok(current);
                    }
                }
                return Err(err);
            }
        };
        self.inner.tokens.set_external(token, epoch)
    }

    async fn obtain_managed_token(&self) -> Result<TokenLease> {
        let snapshot = self.inner.tokens.snapshot();
        if !snapshot.refresh_token.is_empty()
            && snapshot
                .refresh_expires_at
                .is_some_and(|expiry| Instant::now() < expiry)
        {
            let started = Instant::now();
            let mut auth = self.authentication_client(false)?;
            let result = self
                .with_timeout(async move {
                    auth.authenticate_with_refresh_token(
                        authv1::AuthenticateWithRefreshTokenRequest {},
                    )
                    .await
                })
                .await
                .and_then(|response| {
                    self.inner.tokens.set_session(
                        &response.into_inner(),
                        self.inner.tokens.exchange_started_since(started),
                        snapshot.epoch,
                    )
                });
            match result {
                Ok(token) => return Ok(token),
                Err(err) => {
                    let code = crate::grpc::error_code(&err);
                    let safely_rejected = code == Some(Code::AlreadyExists);
                    if !safely_rejected {
                        self.inner.tokens.discard_refresh(snapshot.epoch);
                    }
                    if code == Some(Code::Unauthenticated) {
                        self.inner.tokens.invalidate_access_token();
                        if !self.can_reauthenticate() {
                            return Err(Error::SessionExpired);
                        }
                    } else {
                        if matches!(
                            code,
                            Some(
                                Code::AlreadyExists
                                    | Code::ResourceExhausted
                                    | Code::Unavailable
                                    | Code::Internal
                                    | Code::Unknown
                                    | Code::DeadlineExceeded
                            )
                        ) {
                            if let Some(current) = self.inner.tokens.backoff(snapshot.epoch) {
                                return Ok(current);
                            }
                        }
                        return Err(Error::Refresh(Arc::new(err)));
                    }
                }
            }
        } else {
            self.inner.tokens.discard_refresh(snapshot.epoch);
        }
        if self.inner.tokens.has_ever_authenticated() && !self.can_reauthenticate() {
            if let Some(current) = self.inner.tokens.backoff(snapshot.epoch) {
                return Ok(current);
            }
            return Err(Error::SessionExpired);
        }
        self.authenticate(snapshot.epoch).await
    }

    async fn authenticate(&self, epoch: u64) -> Result<TokenLease> {
        let authenticator = self.authenticator()?;
        let auth = self.authentication_client(true)?;
        let scopes = self.inner.cfg.scopes.clone();
        let started = Instant::now();
        let token = self
            .with_timeout(async move {
                authenticator
                    .authenticate(auth, &scopes)
                    .await
                    .map_err(Error::authentication)
            })
            .await?;
        self.inner.tokens.set_session(
            &token,
            self.inner.tokens.exchange_started_since(started),
            epoch,
        )
    }

    fn can_reauthenticate(&self) -> bool {
        self.authenticator()
            .is_ok_and(|authenticator| authenticator.can_reauthenticate())
    }

    async fn with_timeout<F, T, E>(&self, future: F) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, E>>,
        E: Into<Error>,
    {
        tokio::select! {
            biased;
            _ = self.inner.close_cancel.cancelled() => Err(Error::Closed),
            result = tokio::time::timeout(self.inner.cfg.authentication_timeout, future) => {
                result.map_err(|_| Error::DeadlineExceeded)?.map_err(Into::into)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn losing_refresh_credentials_preserves_an_unexpired_access_token() {
        let client = Client::builder()
            .domain("example.com")
            .authenticator(crate::AuthenticationToken::new("one-time"))
            .build()
            .await
            .unwrap();
        let token = authv1::SessionToken {
            access_token: "still-valid".into(),
            refresh_token: "refresh".into(),
            expires_in: 10,
            refresh_token_expires_in: 3600,
        };
        client
            .inner
            .tokens
            .set_session(&token, Instant::now() - Duration::from_secs(9), 0)
            .unwrap();
        client.inner.tokens.discard_refresh(0);
        assert_eq!(client.access_token().await.unwrap(), "still-valid");
        client.invalidate_access_token();
        assert!(matches!(client.token().await, Err(Error::SessionExpired)));
        client.shutdown().await;
    }
}
