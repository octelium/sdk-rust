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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime};

use octelium_apis::authv1;

/// How long before expiration a token is proactively replaced.
///
/// [`Snapshot::new`] caps it at 20% of a short token's lifetime, so the
/// effective leeway is always the smaller of the two.
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
    pub fn with_expiry(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }
}

// The token is a credential, so it never reaches logs through `Debug`.
impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessToken")
            .field("value", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// The token state at a point in time.
#[derive(Clone, Default)]
pub(crate) struct Snapshot {
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    /// The monotonic expiration deadline, immune to wall clock adjustments.
    expires_at: Option<Instant>,
    /// The wall clock expiration time, reported to the application.
    expires_at_wall: Option<SystemTime>,
    /// When the token should be replaced, always at or before `expires_at`.
    refresh_at: Option<Instant>,
    invalidated: bool,
}

impl Snapshot {
    fn new(
        access_token: String,
        refresh_token: String,
        now: Instant,
        lifetime: Option<Duration>,
        refresh_before: Duration,
    ) -> Self {
        let mut ret = Self {
            access_token,
            refresh_token,
            ..Default::default()
        };

        let Some(lifetime) = lifetime.filter(|lifetime| !lifetime.is_zero()) else {
            return ret;
        };

        ret.expires_at = Some(now + lifetime);
        ret.expires_at_wall = Some(SystemTime::now() + lifetime);

        // Never consume more than 20% of a short token's lifetime as refresh
        // leeway. This keeps the default useful for both minute-long and
        // hour-long tokens without refreshing the latter excessively early.
        let early = refresh_before.min(lifetime / 5);
        ret.refresh_at = Some(now + lifetime - early);

        ret
    }

    fn token(&self) -> AccessToken {
        AccessToken {
            value: self.access_token.clone(),
            expires_at: self.expires_at_wall,
        }
    }
}

/// The Client's token cache.
///
/// Reads take a shared lock, while [`TokenManager::refresh_lock`] serializes
/// the authentication and refresh calls so concurrent callers share one
/// in-flight token operation.
pub(crate) struct TokenManager {
    state: RwLock<Snapshot>,
    ever_authenticated: AtomicBool,
    pub(crate) refresh_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.read();
        f.debug_struct("TokenManager")
            .field("has_access_token", &!state.access_token.is_empty())
            .field("has_refresh_token", &!state.refresh_token.is_empty())
            .field("expires_at", &state.expires_at_wall)
            .field("invalidated", &state.invalidated)
            .finish()
    }
}

impl TokenManager {
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(Snapshot::default()),
            ever_authenticated: AtomicBool::new(false),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Snapshot> {
        self.state.read().unwrap_or_else(|err| err.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.state.write().unwrap_or_else(|err| err.into_inner())
    }

    /// Returns the token if it is valid and not yet due for replacement.
    pub(crate) fn current(&self, now: Instant) -> Option<AccessToken> {
        let state = self.read();

        if state.access_token.is_empty() || state.invalidated {
            return None;
        }
        if state.refresh_at.is_some_and(|refresh_at| now >= refresh_at) {
            return None;
        }

        Some(state.token())
    }

    /// Returns the token if it has not expired yet, even when it is already
    /// due for replacement. This keeps a Cluster that is briefly unreachable
    /// from breaking calls that would still succeed.
    pub(crate) fn usable(&self, now: Instant) -> Option<AccessToken> {
        let state = self.read();

        if state.access_token.is_empty() || state.invalidated {
            return None;
        }
        if state.expires_at.is_some_and(|expires_at| now >= expires_at) {
            return None;
        }

        Some(state.token())
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        self.read().clone()
    }

    pub(crate) fn refresh_token(&self) -> Option<String> {
        let state = self.read();
        if state.refresh_token.is_empty() {
            return None;
        }
        Some(state.refresh_token.clone())
    }

    pub(crate) fn has_ever_authenticated(&self) -> bool {
        self.ever_authenticated.load(Ordering::Relaxed)
    }

    /// Stores a Cluster Session. A Session token without a refresh token keeps
    /// the previous one, which the Cluster omits when it is unchanged.
    pub(crate) fn set_session(
        &self,
        token: &authv1::SessionToken,
        now: Instant,
        refresh_before: Duration,
        fallback_refresh_token: &str,
    ) {
        let refresh_token = match token.refresh_token.trim() {
            "" => fallback_refresh_token,
            value => value,
        };

        let lifetime = (token.expires_in > 0).then(|| Duration::from_secs(token.expires_in as u64));

        *self.write() = Snapshot::new(
            token.access_token.trim().to_string(),
            refresh_token.to_string(),
            now,
            lifetime,
            refresh_before,
        );
        self.ever_authenticated.store(true, Ordering::Relaxed);
    }

    /// Stores an externally managed access token.
    pub(crate) fn set_external(&self, token: &AccessToken, now: Instant, refresh_before: Duration) {
        let lifetime = token.expires_at.map(|expires_at| {
            expires_at
                .duration_since(SystemTime::now())
                .unwrap_or_default()
        });

        *self.write() = Snapshot::new(
            token.value.clone(),
            String::new(),
            now,
            lifetime,
            refresh_before,
        );
    }

    /// Causes the next operation to obtain a new access token without
    /// discarding a managed Session's refresh token.
    pub(crate) fn invalidate_access_token(&self) {
        let mut state = self.write();
        if state.access_token.is_empty() {
            return;
        }
        state.invalidated = true;
    }

    pub(crate) fn clear(&self) {
        *self.write() = Snapshot::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(access_token: &str, expires_in: i64, refresh_token: &str) -> authv1::SessionToken {
        authv1::SessionToken {
            access_token: access_token.to_string(),
            refresh_token: refresh_token.to_string(),
            expires_in,
            refresh_token_expires_in: 0,
        }
    }

    #[test]
    fn refresh_leeway_is_capped_at_a_fifth_of_the_lifetime() {
        let manager = TokenManager::new();
        let now = Instant::now();

        // A 60 second token would otherwise be replaced after 30 seconds.
        manager.set_session(
            &session("tkn", 60, "refresh"),
            now,
            DEFAULT_REFRESH_BEFORE,
            "",
        );

        assert!(manager.current(now + Duration::from_secs(47)).is_some());
        assert!(manager.current(now + Duration::from_secs(49)).is_none());
        assert!(manager.usable(now + Duration::from_secs(49)).is_some());
        assert!(manager.usable(now + Duration::from_secs(61)).is_none());
    }

    #[test]
    fn long_lived_tokens_use_the_default_leeway() {
        let manager = TokenManager::new();
        let now = Instant::now();

        manager.set_session(
            &session("tkn", 3600, "refresh"),
            now,
            DEFAULT_REFRESH_BEFORE,
            "",
        );

        assert!(manager.current(now + Duration::from_secs(3569)).is_some());
        assert!(manager.current(now + Duration::from_secs(3571)).is_none());
    }

    #[test]
    fn a_token_without_an_expiry_never_goes_stale() {
        let manager = TokenManager::new();
        let now = Instant::now();

        manager.set_session(
            &session("tkn", 0, "refresh"),
            now,
            DEFAULT_REFRESH_BEFORE,
            "",
        );

        let token = manager.current(now + Duration::from_secs(86_400)).unwrap();
        assert_eq!(token.value, "tkn");
        assert!(token.expires_at.is_none());
    }

    #[test]
    fn an_omitted_refresh_token_keeps_the_previous_one() {
        let manager = TokenManager::new();
        let now = Instant::now();

        manager.set_session(
            &session("first", 60, "refresh"),
            now,
            DEFAULT_REFRESH_BEFORE,
            "",
        );
        manager.set_session(
            &session("second", 60, ""),
            now,
            DEFAULT_REFRESH_BEFORE,
            "refresh",
        );

        assert_eq!(manager.refresh_token().as_deref(), Some("refresh"));
        assert_eq!(manager.current(now).unwrap().value, "second");
    }

    #[test]
    fn invalidation_keeps_the_refresh_token() {
        let manager = TokenManager::new();
        let now = Instant::now();

        manager.set_session(
            &session("tkn", 3600, "refresh"),
            now,
            DEFAULT_REFRESH_BEFORE,
            "",
        );
        manager.invalidate_access_token();

        assert!(manager.current(now).is_none());
        assert!(manager.usable(now).is_none());
        assert_eq!(manager.refresh_token().as_deref(), Some("refresh"));
        assert!(manager.has_ever_authenticated());

        manager.clear();
        assert!(manager.refresh_token().is_none());
        // Clearing the tokens does not un-authenticate the Client.
        assert!(manager.has_ever_authenticated());
    }

    #[test]
    fn external_tokens_expire_on_their_own_schedule() {
        let manager = TokenManager::new();
        let now = Instant::now();

        manager.set_external(
            &AccessToken::new("tkn").with_expiry(SystemTime::now() + Duration::from_secs(600)),
            now,
            DEFAULT_REFRESH_BEFORE,
        );

        assert!(manager.current(now + Duration::from_secs(560)).is_some());
        assert!(manager.current(now + Duration::from_secs(575)).is_none());
        assert!(manager.refresh_token().is_none());
    }

    #[test]
    fn the_token_value_is_redacted() {
        let token = AccessToken::new("super-secret");
        assert!(!format!("{token:?}").contains("super-secret"));
    }
}
