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

//! An authenticated HTTP client for Octelium Services.
//!
//! [`Client::http`](crate::Client::http) returns an [`HttpClient`] that
//! attaches the Cluster access token to authorized destinations only.

use std::sync::Arc;
use std::time::Duration;

use ::http::header::{HeaderName, HeaderValue, COOKIE, HOST};
use reqwest::{Method, Request, Response};
use url::Url;

use crate::client::Client;
use crate::domain;
use crate::error::{message, BoxError, Error, Result};

/// Decides whether an HTTP request may receive the Octelium access token.
///
/// Returning an error rejects the request before any network operation. A
/// closure taking `&reqwest::Request` implements this trait.
pub trait HttpAuthorizationPolicy: Send + Sync + 'static {
    /// Authorizes a request, or explains why the destination was rejected.
    fn authorize(&self, request: &Request) -> std::result::Result<(), BoxError>;
}

impl<F> HttpAuthorizationPolicy for F
where
    F: Fn(&Request) -> std::result::Result<(), BoxError> + Send + Sync + 'static,
{
    fn authorize(&self, request: &Request) -> std::result::Result<(), BoxError> {
        self(request)
    }
}

#[derive(Default)]
pub(crate) struct HttpConfig {
    pub(crate) authorized_hosts: Vec<String>,
    pub(crate) authorized_origins: Vec<String>,
    pub(crate) policy: Option<Arc<dyn HttpAuthorizationPolicy>>,
    pub(crate) allow_insecure: bool,
}

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) fn build_client(
    user_agent: &str,
    root_ca_pems: &[Vec<u8>],
    accept_invalid_certs: bool,
) -> Result<reqwest::Client> {
    let mut roots = rustls::RootCertStore::empty();
    #[cfg(feature = "tls-native-roots")]
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    #[cfg(feature = "tls-webpki-roots")]
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for pem in root_ca_pems {
        let certs = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::config("invalid root CA certificate"))?;
        if certs.is_empty() {
            return Err(Error::config("root CA PEM contains no certificates"));
        }
        for cert in certs {
            roots
                .add(cert)
                .map_err(|_| Error::config("invalid root CA certificate"))?;
        }
    }
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    if accept_invalid_certs {
        tls.dangerous()
            .set_certificate_verifier(crate::tls::no_verification());
    }
    reqwest::Client::builder()
        .user_agent(user_agent)
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .pool_idle_timeout(DEFAULT_POOL_IDLE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .tls_backend_preconfigured(tls)
        .build()
        .map_err(|_| Error::config("could not build the HTTP client"))
}

/// An HTTP client that attaches the Octelium access token to authorized
/// destinations.
///
/// By default only the Cluster domain and its subdomains, over HTTPS, receive
/// the token. Everything else is rejected before a connection is made, so a
/// redirects are returned to the caller without being followed.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = octelium::Client::builder().domain("example.com").build().await?;
///
/// let resp = client
///     .http()
///     .get("https://api.example.com/v1/items")
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Starts a `GET` request.
    pub fn get(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::GET, url)
    }

    /// Starts a `POST` request.
    pub fn post(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::POST, url)
    }

    /// Starts a `PUT` request.
    pub fn put(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::PUT, url)
    }

    /// Starts a `PATCH` request.
    pub fn patch(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::PATCH, url)
    }

    /// Starts a `DELETE` request.
    pub fn delete(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::DELETE, url)
    }

    /// Starts a `HEAD` request.
    pub fn head(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(Method::HEAD, url)
    }

    /// Starts a request with any method.
    pub fn request(&self, method: Method, url: impl reqwest::IntoUrl) -> RequestBuilder {
        RequestBuilder {
            http: self.clone(),
            // Keep URL userinfo intact until destination authorization. The
            // normal reqwest constructor removes it and synthesizes Basic auth.
            inner: self.client.http_transport().and_then(|inner| {
                let url = url.into_url()?;
                Ok(reqwest::RequestBuilder::from_parts(
                    inner,
                    Request::new(method, url),
                ))
            }),
        }
    }

    /// Authorizes, authenticates and sends an already built request.
    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        self.client.ensure_open()?;
        self.authorize(&request)?;
        let timeout = request.timeout().copied().or(self.client.request_timeout());
        let deadline = timeout
            .map(|timeout| {
                tokio::time::Instant::now()
                    .checked_add(timeout)
                    .ok_or_else(|| Error::config("request timeout is too large"))
            })
            .transpose()?;
        let cancel = self.client.cancellation();
        let operation = async {
            let lease = self.client.token_lease().await?;
            let mut value = HeaderValue::from_str(&lease.token.value)
                .map_err(|_| Error::authentication(message("invalid access token")))?;
            value.set_sensitive(true);
            for name in [
                "authorization",
                "x-octelium-auth",
                "x-octelium-refresh-token",
            ] {
                request.headers_mut().remove(name);
            }
            if let Some(cookie) = request.headers().get(COOKIE) {
                let cookie = cookie
                    .to_str()
                    .map_err(|_| Error::config("invalid Cookie header"))?;
                let filtered = cookie
                    .split(';')
                    .map(str::trim)
                    .filter(|cookie| {
                        !matches!(
                            cookie.split_once('=').map(|pair| pair.0.trim()),
                            Some("octelium_auth" | "octelium_rt")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                request.headers_mut().remove(COOKIE);
                if !filtered.is_empty() {
                    request.headers_mut().insert(
                        COOKIE,
                        HeaderValue::from_str(&filtered)
                            .map_err(|_| Error::config("invalid Cookie header"))?,
                    );
                }
            }
            request.headers_mut().insert("x-octelium-auth", value);
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(Error::DeadlineExceeded);
                }
                *request.timeout_mut() = Some(remaining);
            }
            let response = self.client.http_transport()?.execute(request).await?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                self.client.invalidate_generation(lease.generation);
            }
            Ok(response)
        };
        let expired = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Closed),
            _ = expired => Err(Error::DeadlineExceeded),
            result = operation => result,
        }
    }

    /// Sends a request without attaching the access token.
    ///
    /// The destination policy does not apply. Redirects remain disabled.
    pub async fn execute_unauthenticated(&self, mut request: Request) -> Result<Response> {
        self.client.ensure_open()?;
        if request.timeout().is_none() {
            *request.timeout_mut() = self.client.request_timeout();
        }
        let cancel = self.client.cancellation();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Closed),
            result = self.client.http_transport()?.execute(request) => Ok(result?),
        }
    }

    /// Returns a clone of the underlying [`reqwest::Client`], without authentication.
    /// The caller owns this clone independently of SDK shutdown.
    pub fn inner(&self) -> Result<reqwest::Client> {
        self.client.http_transport()
    }

    fn authorize(&self, request: &Request) -> Result<()> {
        let cfg = self.client.http_config();

        let url = request.url();

        if !url.username().is_empty() || url.password().is_some() {
            return Err(self.rejected(url, message("URL userinfo is not allowed")));
        }

        match url.scheme() {
            "https" => {}
            "http" if cfg.allow_insecure => {}
            "http" => return Err(self.rejected(url, message("plain HTTP is disabled"))),
            scheme => {
                return Err(
                    self.rejected(url, message(format!("unsupported URL scheme {scheme:?}")))
                )
            }
        }

        let host = match url.host_str() {
            Some(host) => domain::normalize_host(host)
                .map_err(|err| self.rejected(url, Box::new(err) as BoxError))?,
            None => return Err(self.rejected(url, message("the URL has no host"))),
        };

        if request.headers().get_all(HOST).iter().nth(1).is_some() {
            return Err(self.rejected(url, message("multiple Host headers are not allowed")));
        }
        if let Some(authority) = request.headers().get(HOST) {
            let authority = authority
                .to_str()
                .ok()
                .and_then(|value| value.parse::<::http::uri::Authority>().ok())
                .ok_or_else(|| self.rejected(url, message("invalid Host header")))?;
            let routed = domain::normalize_host(authority.host())
                .map_err(|_| self.rejected(url, message("invalid Host header")))?;
            if routed != host
                || authority
                    .port_u16()
                    .unwrap_or(if url.scheme() == "https" { 443 } else { 80 })
                    != url.port_or_known_default().unwrap_or(0)
            {
                return Err(self.rejected(url, message("Host header must match the URL authority")));
            }
        }
        if let Some(policy) = &cfg.policy {
            return policy
                .authorize(request)
                .map_err(|source| self.rejected(url, source));
        }
        if cfg
            .authorized_origins
            .contains(&url.origin().ascii_serialization())
        {
            return Ok(());
        }
        if url.port_or_known_default() != Some(if url.scheme() == "https" { 443 } else { 80 }) {
            return Err(self.rejected(
                url,
                message("nondefault ports require an explicitly authorized origin"),
            ));
        }
        if domain::is_within(&host, self.client.domain()) {
            return Ok(());
        }
        if cfg.authorized_hosts.contains(&host) {
            return Ok(());
        }

        Err(self.rejected(
            url,
            message(format!(
                "the host {host:?} is outside the Cluster domain {:?}",
                self.client.domain()
            )),
        ))
    }

    fn rejected(&self, url: &Url, source: BoxError) -> Error {
        Error::HttpAuthorization {
            url: redact(url),
            source: Arc::from(source),
        }
    }
}

/// Builds a request that carries the Octelium access token.
///
/// It mirrors [`reqwest::RequestBuilder`], and
/// [`with`](RequestBuilder::with) hands the inner builder over for anything
/// not covered here.
#[must_use]
pub struct RequestBuilder {
    http: HttpClient,
    inner: Result<reqwest::RequestBuilder>,
}
impl std::fmt::Debug for RequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // An unbuilt request may carry URL userinfo; never include it in Debug.
        f.debug_struct("RequestBuilder")
            .field("http", &self.http)
            .finish_non_exhaustive()
    }
}

impl RequestBuilder {
    /// Adds a header to the request.
    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<::http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<::http::Error>,
    {
        self.inner = self.inner.map(|builder| builder.header(key, value));
        self
    }

    /// Adds a set of headers to the request.
    pub fn headers(mut self, headers: ::http::HeaderMap) -> Self {
        self.inner = self.inner.map(|builder| builder.headers(headers));
        self
    }

    /// Appends the serialized parameters to the URL query string.
    pub fn query<T: serde::Serialize + ?Sized>(mut self, query: &T) -> Self {
        self.inner = self.inner.map(|builder| builder.query(query));
        self
    }

    /// Sets the JSON request body.
    pub fn json<T: serde::Serialize + ?Sized>(mut self, json: &T) -> Self {
        self.inner = self.inner.map(|builder| builder.json(json));
        self
    }

    /// Sets the request body.
    pub fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
        self.inner = self.inner.map(|builder| builder.body(body));
        self
    }

    /// Bounds this request, overriding the client's timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.map(|builder| builder.timeout(timeout));
        self
    }

    /// Applies any other [`reqwest::RequestBuilder`] method.
    ///
    /// ```no_run
    /// # async fn run(http: octelium::HttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let resp = http
    ///     .post("https://api.example.com/v1/items")
    ///     .with(|builder| builder.version(reqwest::Version::HTTP_11))
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with<F>(mut self, f: F) -> Self
    where
        F: FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    {
        self.inner = self.inner.map(f);
        self
    }

    /// Builds the request without sending it.
    ///
    /// The returned request carries no access token yet; pass it to
    /// [`HttpClient::execute`] to have one attached.
    pub fn build(self) -> Result<Request> {
        Ok(self.inner?.build()?)
    }

    /// Sends the request with a valid access token attached.
    pub async fn send(self) -> Result<Response> {
        let request = self.inner?.build()?;
        self.http.execute(request).await
    }
}

/// Returns the URL with any userinfo removed, so a credential embedded in it
/// never reaches an error message or a log.
fn redact(url: &Url) -> String {
    url.origin().ascii_serialization()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn client() -> Client {
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .without_environment_credentials()
            .build()
            .await
            .unwrap()
    }

    fn request(http: &HttpClient, url: &str) -> Request {
        http.get(url).build().unwrap()
    }

    #[tokio::test]
    async fn the_cluster_domain_and_its_subdomains_are_authorized() {
        let http = client().await.http();

        for url in [
            "https://example.com/path",
            "https://api.example.com/",
            "https://a.b.example.com/?q=1",
        ] {
            http.authorize(&request(&http, url))
                .unwrap_or_else(|err| panic!("expected {url} to be authorized: {err}"));
        }
    }

    #[tokio::test]
    async fn other_destinations_are_rejected() {
        let http = client().await.http();

        for url in [
            "https://evil.com/",
            "https://example.com.evil.com/",
            "https://notexample.com/",
            "http://example.com/",
        ] {
            let err = http
                .authorize(&request(&http, url))
                .expect_err("expected the destination to be rejected");
            assert!(matches!(err, Error::HttpAuthorization { .. }), "{err}");
        }
    }

    #[tokio::test]
    async fn additional_hosts_can_be_authorized() {
        let client = Client::builder()
            .domain("example.com")
            .access_token("token")
            .authorized_http_hosts(["Partner.Example.NET."])
            .build()
            .await
            .unwrap();
        let http = client.http();

        http.authorize(&request(&http, "https://partner.example.net/v1"))
            .unwrap();
        assert!(http
            .authorize(&request(&http, "https://other.example.net/v1"))
            .is_err());
    }

    #[tokio::test]
    async fn plain_http_can_be_allowed_explicitly() {
        let client = Client::builder()
            .domain("example.com")
            .access_token("token")
            .allow_insecure_http(true)
            .build()
            .await
            .unwrap();
        let http = client.http();

        http.authorize(&request(&http, "http://example.com/"))
            .unwrap();
        assert!(http.authorize(&request(&http, "http://evil.com/")).is_err());
    }

    #[tokio::test]
    async fn a_custom_policy_replaces_the_default() {
        let client = Client::builder()
            .domain("example.com")
            .access_token("token")
            .http_authorization_policy(|request: &Request| {
                if request.url().host_str() == Some("allowed.test") {
                    Ok(())
                } else {
                    Err(message("denied"))
                }
            })
            .build()
            .await
            .unwrap();
        let http = client.http();

        http.authorize(&request(&http, "https://allowed.test/"))
            .unwrap();
        assert!(http
            .authorize(&request(&http, "https://example.com/"))
            .is_err());
    }

    #[tokio::test]
    async fn url_credentials_are_rejected_and_redacted() {
        let http = client().await.http();

        let request = request(&http, "https://user:hunter2@example.com/");

        let err = http
            .authorize(&request)
            .expect_err("expected the destination to be rejected");

        assert!(matches!(err, Error::HttpAuthorization { .. }), "{err}");
        assert!(!err.to_string().contains("hunter2"), "{err}");
    }
}
