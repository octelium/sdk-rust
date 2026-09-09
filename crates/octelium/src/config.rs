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

use crate::credentials::{AccessTokenProvider, Authenticator};

/// The default user agent sent to the Cluster.
pub(crate) const DEFAULT_USER_AGENT: &str = concat!("octelium-rust/", env!("CARGO_PKG_VERSION"));

/// The default bound on authentication and refresh calls.
pub(crate) const DEFAULT_AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(20);

/// The resolved Client configuration.
pub(crate) struct Config {
    pub(crate) domain: String,
    pub(crate) api_endpoint: String,
    pub(crate) tls_server_name: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) authenticator: Option<Arc<dyn Authenticator>>,
    pub(crate) token_provider: Option<Arc<dyn AccessTokenProvider>>,
    pub(crate) authentication_timeout: Option<Duration>,

    #[cfg(feature = "http")]
    pub(crate) http: crate::http::HttpConfig,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("domain", &self.domain)
            .field("api_endpoint", &self.api_endpoint)
            .field("tls_server_name", &self.tls_server_name)
            .field("scopes", &self.scopes)
            .field("authenticator", &self.authenticator.is_some())
            .field("token_provider", &self.token_provider.is_some())
            .field("authentication_timeout", &self.authentication_timeout)
            .finish_non_exhaustive()
    }
}

/// Installs a process-wide rustls crypto provider if the application has not
/// installed one already.
///
/// The SDK builds its TLS configuration with `ring`, which needs no C
/// toolchain. An application that installed another provider keeps it, and
/// both the gRPC and the HTTP client then use that one.
pub(crate) fn install_crypto_provider() {
    use std::sync::Once;

    static ONCE: Once = Once::new();

    ONCE.call_once(|| {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
    });
}
