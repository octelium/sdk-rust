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

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use crate::config::{
    install_crypto_provider, Config, DEFAULT_AUTHENTICATION_TIMEOUT, DEFAULT_USER_AGENT,
};
use crate::credentials::{AccessTokenProvider, Authenticator, StaticAccessToken};
use crate::env;
use crate::error::{Error, Result};
use crate::{domain, tls, Client};

/// Builds a [`Client`].
///
/// Every option is optional: with no configuration at all the Client takes its
/// Cluster domain and credentials from the environment.
///
/// ```no_run
/// # async fn run() -> Result<(), octelium::Error> {
/// let client = octelium::Client::builder()
///     .domain("example.com")
///     .authenticator(octelium::AuthenticationToken::new("..."))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ClientBuilder {
    domain: Option<String>,
    api_endpoint: Option<String>,
    tls_server_name: Option<String>,
    scopes: Vec<String>,
    authenticator: Option<Arc<dyn Authenticator>>,
    token_provider: Option<Arc<dyn AccessTokenProvider>>,
    use_environment: bool,
    user_agent: String,
    root_ca_pems: Vec<Vec<u8>>,
    accept_invalid_certs: bool,
    authentication_timeout: Option<Duration>,
    authenticate_on_build: bool,
    #[allow(clippy::type_complexity)]
    configure_endpoint: Option<Arc<dyn Fn(Endpoint) -> Endpoint + Send + Sync>>,

    #[cfg(feature = "http")]
    http: crate::http::HttpConfig,
    #[cfg(feature = "http")]
    http_client: Option<reqwest::Client>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("domain", &self.domain)
            .field("api_endpoint", &self.api_endpoint)
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

impl ClientBuilder {
    /// Creates a builder with the default configuration.
    pub fn new() -> Self {
        Self {
            domain: None,
            api_endpoint: None,
            tls_server_name: None,
            scopes: Vec::new(),
            authenticator: None,
            token_provider: None,
            use_environment: true,
            user_agent: DEFAULT_USER_AGENT.to_string(),
            root_ca_pems: Vec::new(),
            accept_invalid_certs: false,
            authentication_timeout: Some(DEFAULT_AUTHENTICATION_TIMEOUT),
            authenticate_on_build: false,
            configure_endpoint: None,

            #[cfg(feature = "http")]
            http: crate::http::HttpConfig::default(),
            #[cfg(feature = "http")]
            http_client: None,
        }
    }

    /// Sets the Cluster domain, for example `example.com` or
    /// `octelium.example.com`.
    ///
    /// It defaults to the `OCTELIUM_DOMAIN` environment variable.
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Overrides the Cluster API endpoint.
    ///
    /// It accepts a full URI, such as `https://octelium-api.example.com:443`,
    /// or a `host:port` pair, which is assumed to be TLS. It defaults to the
    /// `OCTELIUM_API_ENDPOINT` environment variable, and otherwise to
    /// `octelium-api.<domain>:443`.
    pub fn api_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.api_endpoint = Some(endpoint.into());
        self
    }

    /// Overrides the name verified in the Cluster certificate.
    ///
    /// This is useful when [`api_endpoint`](Self::api_endpoint) points at an
    /// IP address, a port forward or a local proxy while the certificate
    /// remains valid for `octelium-api.<domain>`. It defaults to the
    /// `OCTELIUM_TLS_SERVER_NAME` environment variable.
    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.tls_server_name = Some(name.into());
        self
    }

    /// Limits the access permissions of the resulting Session.
    pub fn scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the Cluster Session authentication method.
    ///
    /// It is mutually exclusive with
    /// [`access_token_provider`](Self::access_token_provider).
    pub fn authenticator(mut self, authenticator: impl Authenticator) -> Self {
        self.authenticator = Some(Arc::new(authenticator));
        self
    }

    /// Uses externally managed access tokens instead of a Cluster Session.
    ///
    /// It is mutually exclusive with [`authenticator`](Self::authenticator).
    pub fn access_token_provider(mut self, provider: impl AccessTokenProvider) -> Self {
        self.token_provider = Some(Arc::new(provider));
        self
    }

    /// Uses one externally managed access token.
    pub fn access_token(self, token: impl Into<String>) -> Self {
        self.access_token_provider(StaticAccessToken::new(token))
    }

    /// Disables the default environment credential chain.
    pub fn without_environment_credentials(mut self) -> Self {
        self.use_environment = false;
        self
    }

    /// Overrides the user agent sent to the Cluster.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// Trusts an additional CA certificate, in PEM form, when verifying the
    /// Cluster.
    ///
    /// The host's certificate store remains trusted as well.
    pub fn add_root_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_ca_pems.push(pem.into());
        self
    }

    /// Disables Cluster certificate verification.
    ///
    /// This makes the connection vulnerable to interception and should be
    /// limited to local development against a Cluster with a self-signed
    /// certificate. Prefer [`add_root_ca_pem`](Self::add_root_ca_pem).
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Bounds authentication and refresh calls. `None` disables the SDK
    /// timeout, leaving the caller in charge.
    pub fn authentication_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.authentication_timeout = timeout;
        self
    }

    /// Validates the credentials while building the Client, instead of on the
    /// first call.
    pub fn authenticate_on_build(mut self, enable: bool) -> Self {
        self.authenticate_on_build = enable;
        self
    }

    /// Customizes the underlying gRPC endpoint, for advanced settings such as
    /// HTTP/2 keepalive, window sizes or connection timeouts.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # async fn run() -> Result<(), octelium::Error> {
    /// let client = octelium::Client::builder()
    ///     .configure_endpoint(|endpoint| {
    ///         endpoint
    ///             .connect_timeout(Duration::from_secs(10))
    ///             .http2_keep_alive_interval(Duration::from_secs(30))
    ///     })
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The SDK deliberately configures no gRPC retry policy, because the
    /// Cluster APIs include non-idempotent operations.
    pub fn configure_endpoint<F>(mut self, configure: F) -> Self
    where
        F: Fn(Endpoint) -> Endpoint + Send + Sync + 'static,
    {
        self.configure_endpoint = Some(Arc::new(configure));
        self
    }

    /// Permits the listed exact hostnames to receive the access token over
    /// HTTP, in addition to the Cluster domain and its subdomains.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn authorized_http_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.http
            .authorized_hosts
            .extend(hosts.into_iter().map(Into::into));
        self
    }

    /// Replaces the default HTTP destination policy.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn http_authorization_policy(
        mut self,
        policy: impl crate::http::HttpAuthorizationPolicy,
    ) -> Self {
        self.http.policy = Some(Arc::new(policy));
        self
    }

    /// Permits bearer tokens over plain HTTP to destinations accepted by the
    /// authorization policy. It should normally be limited to local
    /// development environments.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn allow_insecure_http(mut self, allow: bool) -> Self {
        self.http.allow_insecure = allow;
        self
    }

    /// Installs a caller-owned [`reqwest::Client`] as the HTTP transport.
    ///
    /// The supplied client is used as-is, so it also has to carry any TLS
    /// settings configured on this builder.
    #[cfg(feature = "http")]
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Builds the Client.
    pub async fn build(self) -> Result<Client> {
        install_crypto_provider();

        let domain = domain::normalize(
            self.domain
                .or_else(|| env::var("OCTELIUM_DOMAIN"))
                .unwrap_or_default(),
        )?;

        if self.authenticator.is_some() && self.token_provider.is_some() {
            return Err(Error::config(
                "ClientBuilder::authenticator and ClientBuilder::access_token_provider are mutually exclusive",
            ));
        }

        let (mut authenticator, mut token_provider) = (self.authenticator, self.token_provider);
        if authenticator.is_none() && token_provider.is_none() {
            if !self.use_environment {
                return Err(Error::NoCredentials);
            }

            let identity = env::identity()?;
            authenticator = identity.authenticator;
            token_provider = identity.token_provider;
        }

        let default_server_name = format!("octelium-api.{domain}");

        let tls_server_name = self
            .tls_server_name
            .or_else(|| env::var("OCTELIUM_TLS_SERVER_NAME"))
            .unwrap_or_else(|| default_server_name.clone());

        let api_endpoint = self
            .api_endpoint
            .or_else(|| env::var("OCTELIUM_API_ENDPOINT"))
            .unwrap_or_else(|| format!("{default_server_name}:443"));
        let api_endpoint = domain::normalize_endpoint(&api_endpoint)?;

        let mut endpoint = Endpoint::from_shared(api_endpoint.clone())
            .map_err(|err| Error::config(format!("invalid API endpoint: {err}")))?
            .user_agent(self.user_agent.clone())
            .map_err(|err| Error::config(format!("invalid user agent: {err}")))?;

        if api_endpoint.starts_with("https://") {
            let tls = ClientTlsConfig::new().domain_name(tls_server_name.clone());

            endpoint = if self.accept_invalid_certs {
                // A custom verifier replaces the root store, so no roots are
                // configured alongside it.
                endpoint.tls_config_with_verifier(tls, tls::no_verification())?
            } else {
                let tls = self
                    .root_ca_pems
                    .iter()
                    .fold(tls.with_enabled_roots(), |tls, pem| {
                        tls.ca_certificate(Certificate::from_pem(pem))
                    });
                endpoint.tls_config(tls)?
            };
        }

        if let Some(configure) = &self.configure_endpoint {
            endpoint = configure(endpoint);
        }

        #[cfg(feature = "http")]
        let http_client = match self.http_client {
            Some(client) => client,
            None => crate::http::build_client(
                &self.user_agent,
                &self.root_ca_pems,
                self.accept_invalid_certs,
            )?,
        };

        #[cfg(feature = "http")]
        let http = {
            let mut http = self.http;
            http.authorized_hosts = http
                .authorized_hosts
                .iter()
                .map(domain::normalize_host)
                .collect::<Result<Vec<_>>>()?;
            http
        };

        let config = Config {
            domain,
            api_endpoint,
            tls_server_name,
            scopes: self.scopes,
            authenticator,
            token_provider,
            authentication_timeout: self.authentication_timeout,

            #[cfg(feature = "http")]
            http,
        };

        let client = Client::from_parts(
            config,
            endpoint.connect_lazy(),
            endpoint.connect_lazy(),
            #[cfg(feature = "http")]
            http_client,
        );

        if self.authenticate_on_build {
            client.token().await?;
        }

        Ok(client)
    }
}
