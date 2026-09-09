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
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use octelium_apis::bytes::Bytes;
use octelium_apis::{authv1, metav1};

use crate::error::{message, BoxError};
use crate::grpc::AuthServiceClient;
use crate::token::AccessToken;

/// Creates a Cluster Session.
///
/// The trait is public so an application can add an authentication method
/// introduced after this SDK release without waiting for a new version. The
/// supplied [`AuthServiceClient`] already carries the Session's refresh token,
/// so an implementation only has to make the authentication call itself.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates and returns the resulting Session token.
    async fn authenticate(
        &self,
        client: AuthServiceClient,
        scopes: &[String],
    ) -> Result<authv1::SessionToken, BoxError>;

    /// Reports whether this authenticator can safely create a replacement
    /// Session after a refresh token has become invalid.
    ///
    /// One-time credentials, such as authentication tokens, deliberately
    /// return `false`.
    fn can_reauthenticate(&self) -> bool {
        false
    }
}

#[async_trait]
impl<T: Authenticator + ?Sized> Authenticator for Arc<T> {
    async fn authenticate(
        &self,
        client: AuthServiceClient,
        scopes: &[String],
    ) -> Result<authv1::SessionToken, BoxError> {
        (**self).authenticate(client, scopes).await
    }

    fn can_reauthenticate(&self) -> bool {
        (**self).can_reauthenticate()
    }
}

/// Exchanges a Credential authentication token for a Cluster Session.
///
/// Authentication tokens are treated as one-time credentials and are never
/// reused automatically after the Session expires.
#[derive(Clone)]
pub struct AuthenticationToken {
    token: String,
    code_verifier: Bytes,
}

impl AuthenticationToken {
    /// Creates an authenticator for the given authentication token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into().trim().to_string(),
            code_verifier: Bytes::new(),
        }
    }

    /// Supplies the code verifier of a Credential that is bound to a code
    /// challenge. Supplying it for a Credential that is not bound is rejected
    /// by the Cluster.
    pub fn with_code_verifier(mut self, code_verifier: impl Into<Bytes>) -> Self {
        self.code_verifier = code_verifier.into();
        self
    }
}

impl std::fmt::Debug for AuthenticationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticationToken")
            .field("token", &"[redacted]")
            .finish()
    }
}

#[async_trait]
impl Authenticator for AuthenticationToken {
    async fn authenticate(
        &self,
        mut client: AuthServiceClient,
        scopes: &[String],
    ) -> Result<authv1::SessionToken, BoxError> {
        if self.token.is_empty() {
            return Err(message("empty authentication token"));
        }

        let resp = client
            .authenticate_with_authentication_token(
                authv1::AuthenticateWithAuthenticationTokenRequest {
                    authentication_token: self.token.clone(),
                    scopes: scopes.to_vec(),
                    code_verifier: self.code_verifier.clone(),
                },
            )
            .await?;

        Ok(resp.into_inner())
    }
}

type AssertionFuture = Pin<Box<dyn Future<Output = Result<String, BoxError>> + Send>>;

/// Authenticates with a signed assertion, such as an OIDC ID token.
#[derive(Clone)]
pub struct Assertion {
    provider: Arc<dyn Fn() -> AssertionFuture + Send + Sync>,
    identity_provider: Option<metav1::ObjectReference>,
    reusable: bool,
}

impl Assertion {
    /// Authenticates with a freshly obtained assertion. The closure is called
    /// for every authentication attempt, so this authenticator can replace an
    /// expired Session.
    pub fn from_fn<F, Fut>(provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, BoxError>> + Send + 'static,
    {
        Self {
            provider: Arc::new(move || Box::pin(provider())),
            identity_provider: None,
            reusable: true,
        }
    }

    /// Reads the assertion from a file on every authentication.
    ///
    /// This suits Kubernetes projected ServiceAccount tokens and similar files
    /// that the surrounding platform rotates atomically.
    pub fn from_file(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::from_fn(move || {
            let path = path.clone();
            async move { read_credential_file(&path).await }
        })
    }

    /// Authenticates with one fixed assertion.
    ///
    /// It is not reused automatically, because assertions are commonly short
    /// lived or replay protected.
    pub fn new_static(assertion: impl Into<String>) -> Self {
        let assertion = assertion.into().trim().to_string();
        Self {
            provider: Arc::new(move || {
                let assertion = assertion.clone();
                Box::pin(async move { Ok(assertion) })
            }),
            identity_provider: None,
            reusable: false,
        }
    }

    /// Authenticates against a specific IdentityProvider. By default the
    /// Cluster picks the IdentityProvider that matches the assertion.
    pub fn with_identity_provider(mut self, name: impl Into<String>) -> Self {
        self.identity_provider = Some(metav1::ObjectReference {
            name: name.into(),
            ..Default::default()
        });
        self
    }

    /// Authenticates against the referenced IdentityProvider.
    pub fn with_identity_provider_ref(mut self, reference: metav1::ObjectReference) -> Self {
        self.identity_provider = Some(reference);
        self
    }
}

impl std::fmt::Debug for Assertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assertion")
            .field("reusable", &self.reusable)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Authenticator for Assertion {
    async fn authenticate(
        &self,
        mut client: AuthServiceClient,
        scopes: &[String],
    ) -> Result<authv1::SessionToken, BoxError> {
        let assertion = (self.provider)().await?;
        let assertion = assertion.trim();
        if assertion.is_empty() {
            return Err(message("empty assertion"));
        }

        let resp = client
            .authenticate_with_assertion(authv1::AuthenticateWithAssertionRequest {
                assertion: assertion.to_string(),
                scopes: scopes.to_vec(),
                identity_provider_ref: self.identity_provider.clone(),
            })
            .await?;

        Ok(resp.into_inner())
    }

    fn can_reauthenticate(&self) -> bool {
        self.reusable
    }
}

/// Supplies externally managed Octelium access tokens.
///
/// Implementations must be safe for concurrent use. Use this to plug in an
/// OAuth2 token source, a workload identity provider or a secret store.
#[async_trait]
pub trait AccessTokenProvider: Send + Sync + 'static {
    /// Returns a currently valid access token.
    async fn token(&self) -> Result<AccessToken, BoxError>;
}

#[async_trait]
impl<T: AccessTokenProvider + ?Sized> AccessTokenProvider for Arc<T> {
    async fn token(&self) -> Result<AccessToken, BoxError> {
        (**self).token().await
    }
}

/// One externally managed access token with no known expiration time.
#[derive(Clone)]
pub struct StaticAccessToken {
    token: String,
}

impl StaticAccessToken {
    /// Creates a provider for the given access token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into().trim().to_string(),
        }
    }
}

impl std::fmt::Debug for StaticAccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticAccessToken")
            .field("token", &"[redacted]")
            .finish()
    }
}

#[async_trait]
impl AccessTokenProvider for StaticAccessToken {
    async fn token(&self) -> Result<AccessToken, BoxError> {
        if self.token.is_empty() {
            return Err(message("empty access token"));
        }
        Ok(AccessToken::new(self.token.clone()))
    }
}

type TokenFuture = Pin<Box<dyn Future<Output = Result<AccessToken, BoxError>> + Send>>;

/// An [`AccessTokenProvider`] backed by a closure.
#[derive(Clone)]
pub struct AccessTokenProviderFn {
    provider: Arc<dyn Fn() -> TokenFuture + Send + Sync>,
}

impl AccessTokenProviderFn {
    /// Creates a provider that calls `provider` whenever a token is needed.
    ///
    /// The SDK caches the returned token until it is due for replacement, so
    /// the closure is not called on every request.
    pub fn new<F, Fut>(provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AccessToken, BoxError>> + Send + 'static,
    {
        Self {
            provider: Arc::new(move || Box::pin(provider())),
        }
    }
}

impl std::fmt::Debug for AccessTokenProviderFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessTokenProviderFn")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AccessTokenProvider for AccessTokenProviderFn {
    async fn token(&self) -> Result<AccessToken, BoxError> {
        (self.provider)().await
    }
}

pub(crate) async fn read_credential_file(path: &Path) -> Result<String, BoxError> {
    let content = tokio::fs::read_to_string(path).await.map_err(|err| {
        message(format!(
            "could not read the credential file {}: {err}",
            path.display()
        ))
    })?;
    Ok(content.trim().to_string())
}
