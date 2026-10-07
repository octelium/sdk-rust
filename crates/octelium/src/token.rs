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

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use octelium_apis::authv1;

use crate::error::{Error, Result};

pub(crate) const DEFAULT_REFRESH_BEFORE: Duration = Duration::from_secs(30);

/// A bearer token for the Octelium Cluster and its known expiration time.
///
/// An expiration time of `None` means the issuer did not report one.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken {
    /// The bearer token.
    pub value: String,
    /// When the token expires, if known.
    pub expires_at: Option<SystemTime>,
}

impl AccessToken {
    /// Creates a token with no known expiration time.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            expires_at: None,
        }
    }

    /// Sets the expiration time.
    #[must_use]
    pub fn with_expiry(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessToken")
            .field("value", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TokenLease {
    pub(crate) token: Arc<AccessToken>,
    pub(crate) generation: u64,
}

#[derive(Clone, Default)]
pub(crate) struct Snapshot {
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
    access_token: Option<Arc<AccessToken>>,
    pub(crate) refresh_token: String,
    pub(crate) refresh_expires_at: Option<Instant>,
    expires_at: Option<Instant>,
    refresh_at: Option<Instant>,
    retry_at: Option<Instant>,
    invalidated: bool,
    attempted: bool,
    exchange_started: Option<Instant>,
    closed: bool,
}

impl Snapshot {
    fn lease(&self) -> TokenLease {
        TokenLease {
            token: self.access_token.as_ref().unwrap().clone(),
            generation: self.generation,
        }
    }

    fn usable(&self, now: Instant) -> bool {
        !self.closed
            && !self.invalidated
            && self.access_token.is_some()
            && self.expires_at.is_none_or(|expires_at| now < expires_at)
    }
}

pub(crate) struct TokenManager {
    state: RwLock<Snapshot>,
    pub(crate) refresh_lock: tokio::sync::Mutex<()>,
}

impl fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.read();
        f.debug_struct("TokenManager")
            .field("has_access_token", &state.access_token.is_some())
            .field("has_refresh_token", &!state.refresh_token.is_empty())
            .field(
                "expires_at",
                &state
                    .access_token
                    .as_ref()
                    .and_then(|token| token.expires_at),
            )
            .field("invalidated", &state.invalidated)
            .field("closed", &state.closed)
            .finish()
    }
}

impl TokenManager {
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Snapshot> {
        self.state.read().unwrap_or_else(|err| err.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.state.write().unwrap_or_else(|err| err.into_inner())
    }

    pub(crate) fn current(&self, now: Instant) -> Option<TokenLease> {
        let state = self.read();
        if !state.usable(now) {
            return None;
        }
        if state.refresh_at.is_some_and(|refresh_at| now >= refresh_at)
            && state.retry_at.is_none_or(|retry_at| now >= retry_at)
        {
            return None;
        }
        Some(state.lease())
    }

    #[cfg(test)]
    pub(crate) fn usable(&self, now: Instant) -> Option<TokenLease> {
        let state = self.read();
        state.usable(now).then(|| state.lease())
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        self.read().clone()
    }

    pub(crate) fn refresh_token(&self) -> Option<String> {
        let state = self.read();
        (!state.closed
            && !state.refresh_token.is_empty()
            && state
                .refresh_expires_at
                .is_some_and(|expiry| Instant::now() < expiry))
        .then(|| state.refresh_token.clone())
    }

    pub(crate) fn has_ever_authenticated(&self) -> bool {
        self.read().attempted
    }

    pub(crate) fn has_rejected_access_token(&self) -> bool {
        let state = self.read();
        state.invalidated && state.access_token.is_some()
    }

    pub(crate) fn mark_exchange(&self, authentication: bool) {
        let mut state = self.write();
        state.exchange_started = Some(Instant::now());
        state.attempted |= authentication;
    }

    pub(crate) fn exchange_started_since(&self, started: Instant) -> Instant {
        self.read()
            .exchange_started
            .filter(|exchange| *exchange >= started)
            .unwrap_or(started)
    }

    pub(crate) fn set_session(
        &self,
        token: &authv1::SessionToken,
        started: Instant,
        epoch: u64,
    ) -> Result<TokenLease> {
        validate_value(&token.access_token)?;
        let access_lifetime = lifetime(token.expires_in)?;
        let expires_at = started
            .checked_add(access_lifetime)
            .ok_or_else(|| Error::Protocol("access token lifetime is too large".into()))?;
        let now = Instant::now();
        if expires_at <= now {
            return Err(Error::Protocol(
                "the access token expired during the exchange".into(),
            ));
        }
        let expires_at_wall = SystemTime::now()
            .checked_add(expires_at.duration_since(now))
            .ok_or_else(|| Error::Protocol("access token lifetime is too large".into()))?;
        let replacement = if token.refresh_token.is_empty() {
            None
        } else {
            validate_value(&token.refresh_token)?;
            let expiry = started
                .checked_add(lifetime(token.refresh_token_expires_in)?)
                .ok_or_else(|| Error::Protocol("refresh token lifetime is too large".into()))?;
            if expiry <= now {
                return Err(Error::Protocol(
                    "the refresh token expired during the exchange".into(),
                ));
            }
            Some((token.refresh_token.clone(), expiry))
        };
        let mut state = self.write();
        check_epoch(&state, epoch)?;
        if let Some((value, expiry)) = replacement {
            state.refresh_token = value;
            state.refresh_expires_at = Some(expiry);
        } else if state.refresh_token.is_empty()
            || state.refresh_expires_at.is_none_or(|expiry| expiry <= now)
        {
            return Err(Error::Protocol(
                "the Cluster returned no usable refresh token".into(),
            ));
        }
        state.access_token = Some(Arc::new(AccessToken {
            value: token.access_token.clone(),
            expires_at: Some(expires_at_wall),
        }));
        state.expires_at = Some(expires_at);
        state.refresh_at = Some(expires_at - DEFAULT_REFRESH_BEFORE.min(access_lifetime / 5));
        state.retry_at = None;
        state.invalidated = false;
        state.attempted = true;
        state.generation += 1;
        Ok(state.lease())
    }

    pub(crate) fn set_external(&self, token: AccessToken, epoch: u64) -> Result<TokenLease> {
        validate_value(&token.value)?;
        let now = Instant::now();
        let expiry = match token.expires_at {
            Some(wall) => {
                let remaining = wall.duration_since(SystemTime::now()).map_err(|_| {
                    Error::Protocol("the provider returned an expired access token".into())
                })?;
                if remaining.is_zero() {
                    return Err(Error::Protocol(
                        "the provider returned an expired access token".into(),
                    ));
                }
                let deadline = now
                    .checked_add(remaining)
                    .ok_or_else(|| Error::Protocol("access token lifetime is too large".into()))?;
                Some((
                    deadline,
                    deadline - DEFAULT_REFRESH_BEFORE.min(remaining / 5),
                ))
            }
            None => None,
        };
        let mut state = self.write();
        check_epoch(&state, epoch)?;
        state.access_token = Some(Arc::new(token));
        state.expires_at = expiry.map(|value| value.0);
        state.refresh_at = expiry.map(|value| value.1);
        state.retry_at = None;
        state.invalidated = false;
        state.generation += 1;
        Ok(state.lease())
    }

    pub(crate) fn discard_refresh(&self, epoch: u64) {
        let mut state = self.write();
        if state.epoch == epoch {
            state.refresh_token.clear();
            state.refresh_expires_at = None;
        }
    }

    pub(crate) fn backoff(&self, epoch: u64) -> Option<TokenLease> {
        let mut state = self.write();
        let now = Instant::now();
        if state.epoch != epoch || !state.usable(now) {
            return None;
        }
        state.retry_at = now.checked_add(Duration::from_secs(1));
        Some(state.lease())
    }

    pub(crate) fn invalidate_access_token(&self) {
        self.write().invalidated = true;
    }

    pub(crate) fn invalidate_generation(&self, generation: u64) {
        let mut state = self.write();
        if state.generation == generation {
            state.invalidated = true;
            state.retry_at = None;
        }
    }

    pub(crate) fn clear(&self) {
        let mut state = self.write();
        *state = Snapshot {
            epoch: state.epoch + 1,
            generation: state.generation,
            attempted: state.attempted,
            closed: state.closed,
            ..Default::default()
        };
    }

    pub(crate) fn close(&self) {
        let mut state = self.write();
        *state = Snapshot {
            epoch: state.epoch + 1,
            generation: state.generation,
            attempted: state.attempted,
            closed: true,
            ..Default::default()
        };
    }
}

fn check_epoch(state: &Snapshot, epoch: u64) -> Result<()> {
    if state.closed {
        return Err(Error::Closed);
    }
    if state.epoch != epoch {
        return Err(Error::SessionChanged);
    }
    Ok(())
}

fn lifetime(seconds: i64) -> Result<Duration> {
    if seconds <= 0 {
        return Err(Error::Protocol("token lifetimes must be positive".into()));
    }
    Ok(Duration::from_secs(seconds as u64))
}

pub(crate) fn validate_value(value: &str) -> Result<()> {
    if value.is_empty() || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(Error::Protocol(
            "token values must contain printable ASCII without whitespace".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(expires_in: i64, refresh_token_expires_in: i64) -> authv1::SessionToken {
        authv1::SessionToken {
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_in,
            refresh_token_expires_in,
        }
    }

    #[test]
    fn leeway_preserves_short_lived_tokens() {
        let manager = TokenManager::new();
        let started = Instant::now();
        manager.set_session(&session(10, 3600), started, 0).unwrap();
        assert!(manager.current(started + Duration::from_secs(7)).is_some());
        assert!(manager.current(started + Duration::from_secs(9)).is_none());
        assert!(manager.usable(started + Duration::from_secs(9)).is_some());
        assert!(manager.usable(started + Duration::from_secs(11)).is_none());
    }

    #[test]
    fn invalid_lifetimes_are_rejected_without_publication() {
        for token in [
            session(0, 3600),
            session(-1, 3600),
            session(i64::MAX, 3600),
            session(3600, 0),
            session(3600, -1),
            session(3600, i64::MAX),
        ] {
            let manager = TokenManager::new();
            assert!(manager.set_session(&token, Instant::now(), 0).is_err());
            assert!(manager.current(Instant::now()).is_none());
            assert!(manager.refresh_token().is_none());
        }
    }

    #[test]
    fn exchange_latency_is_subtracted_from_the_token_lifetime() {
        let manager = TokenManager::new();
        let started = Instant::now() - Duration::from_secs(4);
        manager.set_session(&session(10, 3600), started, 0).unwrap();
        assert!(manager.usable(started + Duration::from_secs(9)).is_some());
        assert!(manager.usable(started + Duration::from_secs(11)).is_none());
        assert!(manager.set_session(&session(1, 3600), started, 0).is_err());
    }

    #[test]
    fn omitted_refresh_tokens_keep_their_original_expiry() {
        let manager = TokenManager::new();
        let started = Instant::now();
        manager.set_session(&session(10, 60), started, 0).unwrap();
        let expiry = manager.snapshot().refresh_expires_at;
        let mut token = session(10, 3600);
        token.refresh_token.clear();
        manager.set_session(&token, started, 0).unwrap();
        assert_eq!(manager.snapshot().refresh_expires_at, expiry);
        assert_eq!(manager.refresh_token().as_deref(), Some("refresh"));
    }

    #[test]
    fn initial_sessions_require_a_refresh_token() {
        let manager = TokenManager::new();
        let mut token = session(10, 3600);
        token.refresh_token.clear();
        assert!(manager.set_session(&token, Instant::now(), 0).is_err());
    }

    #[test]
    fn older_rejections_do_not_invalidate_newer_tokens() {
        let manager = TokenManager::new();
        let started = Instant::now();
        let first = manager.set_session(&session(10, 3600), started, 0).unwrap();
        let second = manager.set_session(&session(10, 3600), started, 0).unwrap();
        manager.invalidate_generation(first.generation);
        assert_eq!(
            manager.current(started).unwrap().generation,
            second.generation
        );
        manager.invalidate_generation(second.generation);
        assert!(manager.usable(started).is_none());
    }

    #[test]
    fn clearing_and_closing_block_old_publications() {
        let manager = TokenManager::new();
        manager.clear();
        assert!(matches!(
            manager.set_session(&session(10, 3600), Instant::now(), 0),
            Err(Error::SessionChanged)
        ));
        manager.close();
        assert!(matches!(
            manager.set_session(&session(10, 3600), Instant::now(), 2),
            Err(Error::Closed)
        ));
        assert!(matches!(
            manager.set_external(AccessToken::new("access"), 2),
            Err(Error::Closed)
        ));
    }

    #[test]
    fn fallback_never_uses_rejected_or_expired_tokens() {
        let manager = TokenManager::new();
        manager
            .set_session(&session(10, 3600), Instant::now(), 0)
            .unwrap();
        assert!(manager.backoff(0).is_some());
        manager.invalidate_access_token();
        assert!(manager.backoff(0).is_none());
    }

    #[test]
    fn external_tokens_with_unknown_expiry_remain_valid() {
        let manager = TokenManager::new();
        manager.set_external(AccessToken::new("access"), 0).unwrap();
        assert!(manager
            .current(Instant::now() + Duration::from_secs(86400))
            .is_some());
        assert!(manager.refresh_token().is_none());
    }

    #[test]
    fn expired_external_tokens_and_unsafe_token_values_are_rejected() {
        let manager = TokenManager::new();
        assert!(manager
            .set_external(AccessToken::new("access").with_expiry(SystemTime::now()), 0)
            .is_err());
        for value in ["", "a b", "a\n", " a", "é", "a\t"] {
            assert!(manager.set_external(AccessToken::new(value), 0).is_err());
        }
    }

    #[test]
    fn token_debug_is_redacted() {
        let token = AccessToken::new("super-secret");
        assert!(!format!("{token:?}").contains("super-secret"));
    }
}
