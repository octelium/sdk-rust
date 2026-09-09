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

use crate::credentials::{
    AccessTokenProvider, Assertion, AuthenticationToken, Authenticator, StaticAccessToken,
};
use crate::error::{Error, Result};

/// The credentials found in the environment.
pub(crate) struct Identity {
    pub(crate) authenticator: Option<Arc<dyn Authenticator>>,
    pub(crate) token_provider: Option<Arc<dyn AccessTokenProvider>>,
}

impl Identity {
    fn authenticator(authenticator: impl Authenticator) -> Self {
        Self {
            authenticator: Some(Arc::new(authenticator)),
            token_provider: None,
        }
    }

    fn token_provider(provider: impl AccessTokenProvider) -> Self {
        Self {
            authenticator: None,
            token_provider: Some(Arc::new(provider)),
        }
    }
}

/// Reads a non-empty environment variable.
pub(crate) fn var(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Resolves the credentials from the environment, in order of precedence.
pub(crate) fn identity() -> Result<Identity> {
    if let Some(token) = var("OCTELIUM_ACCESS_TOKEN") {
        return Ok(Identity::token_provider(StaticAccessToken::new(token)));
    }

    if let Some(path) = var("OCTELIUM_ASSERTION_FILE") {
        return Ok(Identity::authenticator(Assertion::from_file(path)));
    }

    if let Some(assertion) = var("OCTELIUM_ASSERTION") {
        return Ok(Identity::authenticator(Assertion::new_static(assertion)));
    }

    if let Some(token) = var("OCTELIUM_AUTH_TOKEN") {
        return Ok(Identity::authenticator(AuthenticationToken::new(token)));
    }

    Err(Error::NoCredentials)
}
