use crate::{Error, Result, proto};
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// Shared authenticated client. Cloning shares connections, credentials and shutdown.
///
/// The client uses Tokio. Dropping the last client/handle releases its transport;
/// it never stops or deletes remote workspaces. Use [`Self::close`] to cancel
/// high-level calls and streams across all clones.
#[derive(Clone, Debug)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}
#[derive(Debug)]
pub(crate) struct Inner {
    transport: octelium::Client,
    pub(crate) closed: CancellationToken,
    timeout: Option<Duration>,
}
impl Client {
    /// Creates a builder with environment credentials and verified TLS.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
    /// Builds a client for a cluster domain, using environment credentials.
    pub async fn connect(domain: impl Into<String>) -> Result<Self> {
        Self::builder().domain(domain).build().await
    }
    /// Builds from `CORDIUM_DOMAIN` or `OCTELIUM_DOMAIN` and Octelium credential variables.
    pub async fn from_env() -> Result<Self> {
        Self::builder().build().await
    }
    /// Reuses an existing authenticated Octelium client. Shutdown closes that shared client too.
    pub fn from_transport(transport: octelium::Client) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                closed: CancellationToken::new(),
                timeout: Some(Duration::from_secs(30)),
            }),
        }
    }
    /// Returns the cluster domain.
    pub fn domain(&self) -> &str {
        self.inner.transport.domain()
    }
    /// Returns the gRPC endpoint.
    pub fn api_endpoint(&self) -> &str {
        self.inner.transport.api_endpoint()
    }
    /// Returns the underlying authenticated client for extension APIs and credentials.
    pub fn transport(&self) -> &octelium::Client {
        &self.inner.transport
    }
    /// Obtains a valid bearer token. Token values are sensitive; avoid logging them.
    pub async fn access_token(&self) -> Result<String> {
        self.operation(self.inner.timeout, async {
            Ok(self.inner.transport.access_token().await?)
        })
        .await
    }
    /// Cancels active high-level operations and prevents future calls on every clone.
    /// This does not delete resources or log out the server Session.
    pub fn close(&self) {
        self.inner.closed.cancel();
        self.inner.transport.close();
    }
    /// Returns whether this client or its shared transport has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.is_cancelled() || self.inner.transport.is_closed()
    }
    /// Workspace collection and sandbox lifecycle.
    pub fn workspaces(&self) -> crate::Workspaces {
        crate::Workspaces::new(self.clone())
    }
    /// Space management.
    pub fn spaces(&self) -> crate::Spaces {
        crate::Spaces::new(self.clone())
    }
    /// Template configuration and pre-builds.
    pub fn templates(&self) -> crate::Templates {
        crate::Templates::new(self.clone())
    }
    /// Persistent workspace snapshots.
    pub fn snapshots(&self) -> crate::Snapshots {
        crate::Snapshots::new(self.clone())
    }
    /// Space-owned persistent storage.
    pub fn volumes(&self) -> crate::Volumes {
        crate::Volumes::new(self.clone())
    }
    /// Write-only Space secrets.
    pub fn secrets(&self) -> crate::Secrets {
        crate::Secrets::new(self.clone())
    }
    /// Personal secrets and generated SSH keys.
    pub fn user_secrets(&self) -> crate::UserSecrets {
        crate::UserSecrets::new(self.clone())
    }
    /// Git OAuth provider configuration.
    pub fn git_providers(&self) -> crate::GitProviders {
        crate::GitProviders::new(self.clone())
    }
    /// Space membership and roles.
    pub fn memberships(&self) -> crate::Memberships {
        crate::Memberships::new(self.clone())
    }
    /// Available workspace hosting Regions.
    pub fn regions(&self) -> crate::Regions {
        crate::Regions::new(self.clone())
    }
    /// Authenticated user preferences.
    pub fn user_config(&self) -> crate::UserConfig {
        crate::UserConfig::new(self.clone())
    }
    /// Administrator cluster configuration.
    pub fn management(&self) -> crate::Management {
        crate::Management::new(self.clone())
    }

    /// Authenticated generated MainService escape hatch. SDK deadlines are not applied.
    pub fn main_service(
        &self,
    ) -> proto::main_service_client::MainServiceClient<octelium::AuthenticatedChannel> {
        proto::main_service_client::MainServiceClient::new(self.inner.transport.channel())
    }
    /// Authenticated generated WorkspaceService escape hatch. SDK deadlines are not applied.
    pub fn workspace_service(
        &self,
    ) -> proto::workspace_service_client::WorkspaceServiceClient<octelium::AuthenticatedChannel>
    {
        proto::workspace_service_client::WorkspaceServiceClient::new(self.inner.transport.channel())
    }
    /// Authenticated generated ManagementService escape hatch. SDK deadlines are not applied.
    pub fn management_service(
        &self,
    ) -> proto::management_service_client::ManagementServiceClient<octelium::AuthenticatedChannel>
    {
        proto::management_service_client::ManagementServiceClient::new(
            self.inner.transport.channel(),
        )
    }
    #[cfg(feature = "http")]
    /// Authenticated HTTP for workspace applications. Destination scoping is enforced by Octelium.
    /// Use request timeouts and consume or stream the response body through reqwest.
    pub fn http(&self) -> octelium::HttpClient {
        self.inner.transport.http()
    }
    pub(crate) fn default_timeout(&self) -> Option<Duration> {
        self.inner.timeout
    }
    // Boxing at this boundary prevents the large generated protobuf futures
    // from inflating every lifecycle/wait future and overflowing task stacks.
    pub(crate) fn rpc<'a, T: Send + 'a>(
        &'a self,
        f: impl Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>> + Send + 'a,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>> {
        Box::pin(self.operation(self.inner.timeout, async move { Ok(f.await?.into_inner()) }))
    }
    pub(crate) async fn operation<T>(
        &self,
        timeout: Option<Duration>,
        f: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let deadline = match timeout {
            Some(t) => Some(
                tokio::time::Instant::now()
                    .checked_add(t)
                    .ok_or_else(|| Error::InvalidArgument("timeout is too large".into()))?,
            ),
            None => None,
        };
        self.operation_until(deadline, f).await
    }
    pub(crate) async fn operation_until<T>(
        &self,
        deadline: Option<tokio::time::Instant>,
        f: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let result = tokio::select! {
            biased;
            _ = self.inner.closed.cancelled() => Err(Error::Closed),
            result = async {
                match deadline {
                    Some(d) => tokio::time::timeout_at(d,f).await.map_err(|_| Error::DeadlineExceeded)?,
                    None => f.await,
                }
            } => result,
        };
        if self.is_closed() {
            Err(Error::Closed)
        } else {
            result
        }
    }
}

/// Configures authentication, transport, and default unary RPC deadlines.
#[derive(Debug)]
#[must_use = "call build to create the client"]
pub struct ClientBuilder {
    inner: octelium::ClientBuilder,
    domain: Option<String>,
    timeout: Option<Duration>,
}
impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            inner: octelium::ClientBuilder::new()
                .user_agent(concat!("cordium-rust/", env!("CARGO_PKG_VERSION"))),
            domain: None,
            timeout: Some(Duration::from_secs(30)),
        }
    }
}
impl ClientBuilder {
    /// Creates a builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the cluster domain, e.g. `example.com`.
    pub fn domain(mut self, v: impl Into<String>) -> Self {
        self.domain = Some(v.into());
        self
    }
    /// Sets a shared per-call deadline including authentication. `None` disables it.
    pub fn timeout(mut self, v: Option<Duration>) -> Self {
        self.timeout = v;
        self
    }
    /// Configures advanced Octelium transport settings, credentials, and HTTP policy.
    ///
    /// ```no_run
    /// # async fn run() -> cordium::Result<()> {
    /// let client = cordium::Client::builder().domain("example.com")
    ///     .configure_transport(|b| b.add_root_ca_pem(b"PEM certificate".to_vec()))
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    pub fn configure_transport(
        mut self,
        f: impl FnOnce(octelium::ClientBuilder) -> octelium::ClientBuilder,
    ) -> Self {
        self.inner = f(self.inner);
        self
    }
    /// Overrides the gRPC API endpoint. Plain HTTP endpoints are for local development.
    pub fn api_endpoint(mut self, v: impl Into<String>) -> Self {
        self.inner = self.inner.api_endpoint(v);
        self
    }
    /// Supplies an externally managed access token.
    pub fn access_token(mut self, v: impl Into<String>) -> Self {
        self.inner = self.inner.access_token(v);
        self
    }
    /// Supplies a rotating access token provider.
    pub fn access_token_provider(mut self, v: impl octelium::AccessTokenProvider) -> Self {
        self.inner = self.inner.access_token_provider(v);
        self
    }
    /// Supplies a Session authenticator such as an authentication token or assertion.
    pub fn authenticator(mut self, v: impl octelium::Authenticator) -> Self {
        self.inner = self.inner.authenticator(v);
        self
    }
    /// Disables the environment credential chain.
    pub fn without_environment_credentials(mut self) -> Self {
        self.inner = self.inner.without_environment_credentials();
        self
    }
    /// Limits the permissions of a Session.
    pub fn scopes<I, S>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.inner = self.inner.scopes(v);
        self
    }
    /// Validates credentials during build instead of authenticating lazily.
    pub fn authenticate_on_build(mut self, v: bool) -> Self {
        self.inner = self.inner.authenticate_on_build(v);
        self
    }
    /// Creates the client. Requires a Tokio runtime; network dialing is lazy by default.
    pub async fn build(mut self) -> Result<Client> {
        if self.timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::InvalidArgument("timeout must be positive".into()));
        }
        if let Some(domain) = self.domain.or_else(|| {
            std::env::var("CORDIUM_DOMAIN")
                .ok()
                .filter(|s| !s.trim().is_empty())
        }) {
            self.inner = self.inner.domain(domain);
        }
        let transport = self.inner.build().await?;
        Ok(Client {
            inner: Arc::new(Inner {
                transport,
                closed: CancellationToken::new(),
                timeout: self.timeout,
            }),
        })
    }
}
