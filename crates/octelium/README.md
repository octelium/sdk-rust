# octelium

The official Rust SDK for [Octelium](https://octelium.com).

A `Client` authenticates to an Octelium Cluster, refreshes its Session on
demand, and provides authenticated gRPC and HTTP clients. It requires a Tokio
runtime and supports Rust 1.88 and later.

```sh
cargo add octelium octelium-apis
```

## Quickstart

```rust,no_run
use octelium::Client;
use octelium_apis::corev1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::from_env().await?;

    let users = client
        .core_v1()
        .list_user(corev1::ListUserOptions::default())
        .await?
        .into_inner();

    for user in users.items {
        println!("{}", user.metadata.unwrap_or_default().name);
    }

    client.shutdown().await;
    Ok(())
}
```

## Credentials

With no explicit credentials the Client reads them from the environment, in
this order:

| Variable | Meaning |
| --- | --- |
| `OCTELIUM_ACCESS_TOKEN` | An externally managed access token, used as-is. |
| `OCTELIUM_ASSERTION_FILE` | A file holding a signed assertion, re-read on every authentication. |
| `OCTELIUM_ASSERTION` | A signed assertion. |
| `OCTELIUM_AUTH_TOKEN` | A Credential authentication token. |

`OCTELIUM_DOMAIN`, `OCTELIUM_API_ENDPOINT` and `OCTELIUM_TLS_SERVER_NAME`
configure the Cluster and its endpoint.

Credentials can also be set explicitly:

```rust,no_run
use octelium::{AuthenticationToken, Client};

# async fn run() -> Result<(), octelium::Error> {
let client = Client::builder()
    .domain("example.com")
    .authenticator(AuthenticationToken::new("<authentication token>"))
    .scopes(["api:core.MainService/ListUser"])
    .build()
    .await?;
# Ok(())
# }
```

An application can implement the `Authenticator` or `AccessTokenProvider`
traits to plug in its own credential source, including authentication methods
added after this release.

An assertion's IdentityProvider reference is optional; the Cluster can infer
it from the issuer. `Assertion::from_fn` and `Assertion::from_file` obtain a
fresh assertion when a new Session is needed. Fixed assertions and
authentication tokens are not replayed by default. If a Credential is
configured for repeated exchanges on the Cluster, explicitly enable
`AuthenticationToken::with_reauthentication(true)`.

Externally managed providers should return `AccessToken::with_expiry` when
they know the expiry. Without an expiry the SDK caches the token until it is
invalidated. A fixed access token rejected by an API requires a new client
with fresh credentials. Provider futures are dropped when their owned
authentication deadline expires or the client closes.

## gRPC

`Client::channel()` returns an authenticated channel that can be passed to any
generated `octelium-apis` client, and the common Services have accessors:

```rust,no_run
use octelium_apis::userv1;

# async fn run(client: octelium::Client) -> Result<(), Box<dyn std::error::Error>> {
let status = client
    .user_v1()
    .get_status(userv1::GetStatusRequest::default())
    .await?;

// Any other generated client.
let mut svc = userv1::main_service_client::MainServiceClient::new(client.channel());
# Ok(())
# }
```

`core_v1`, `user_v1`, `cordium_v1`, `cordium_v1_workspace` and
`cordium_v1_management` use `x-octelium-auth`. Refresh and logout use a separate
auth channel and only `x-octelium-refresh-token`; SDK sessions require no
cookies. The auth API for Devices and Authenticators is not exposed as an
access-authenticated client. Custom `Authenticator` implementations still
receive an `AuthServiceClient` for their exchange.

API mutations are not retried. An `UNAUTHENTICATED` response invalidates only
the token generation used by that request, including errors delivered in
streaming response trailers. Infrastructure errors retain their gRPC status
codes and error sources.

## HTTP

`Client::http()` returns an HTTP client that attaches the access token to
Octelium Services. By default only the Cluster domain and its subdomains, over
HTTPS on port 443, receive the token. Redirects are returned without being
followed, including redirects within the same origin:

```rust,no_run
# async fn run(client: octelium::Client) -> Result<(), Box<dyn std::error::Error>> {
let resp = client
    .http()
    .get("https://my-service.example.com/api/items")
    .send()
    .await?;
# Ok(())
# }
```

`authorized_http_origins` permits exact additional origins with explicit
ports; `authorized_http_hosts` permits additional hosts on the default port.
A custom destination policy still enforces HTTPS, rejects URL userinfo, and
requires the Host header to match the URL authority. Plaintext service calls
require `allow_insecure_http(true)`; plaintext API endpoints separately
require `allow_insecure_api(true)`.

The HTTP client replaces alternate authentication headers and removes
Octelium auth cookies while preserving application cookies. Responses are
returned to the caller, which owns reading or dropping their bodies.
`HttpClient::inner()` returns a caller-owned unauthenticated reqwest clone;
its lifetime is independent of SDK shutdown. Arbitrary injected reqwest
clients are no longer supported because their redirect behavior cannot be
enforced by the SDK.

## Sessions

The Client obtains an access token on the first call that needs one, and
replaces it on a subsequent request shortly before it expires. There is no
idle refresh scheduler. Concurrent callers share one in-flight exchange and
its result, including failures. Cancelling one caller does not cancel the
shared exchange or lose a rotating refresh token.

A managed Session requires positive access and refresh lifetimes. Expiry
uses a monotonic clock and conservatively accounts for exchange latency.
Refresh-token expiry is tracked independently. An omitted refresh token keeps
the previous token and its original expiry.

An ambiguous refresh failure discards the potentially consumed refresh
token. A reusable authenticator can later create a replacement Session;
one-time credentials return `Error::SessionExpired` rather than being
replayed. Transient proactive-refresh failures can use an unexpired access
token with a short retry delay. Rejected or expired access tokens are never
used as fallback credentials.

`Client` is cheap to clone and safe to share across tasks: every clone shares
one Session, one token cache and an API channel. Managed Sessions use a
separate authentication channel; externally managed access tokens do not.
`forget_session()` clears local token state and prevents an in-flight exchange
from publishing its old result. It does not log out remotely.

`close()` synchronously closes every clone, cancels operations and releases
owned credentials and transports. `shutdown().await` also waits for owned
authentication workers to stop. Neither method logs out. Escaped raw tonic
channels, reqwest clones and caller-owned responses retain their own lifetime.

## Deadlines

Authentication and refresh have a positive client-owned deadline of 20 seconds
by default. `authentication_timeout(Duration)` changes it; it cannot be
disabled. `request_timeout(Some(duration))` sets a default API and HTTP deadline
that includes token acquisition. It defaults to `None`, leaving request
deadlines under caller control while preserving bounded authentication.

For an individual RPC, use `tonic::Request::set_timeout`; for HTTP, use
`RequestBuilder::timeout`. These override the client request deadline. gRPC
deadlines cover streaming response consumption too, so configure a suitable
deadline for long-lived streams.

## Examples

See the [Core API examples](examples/README.md) for paginated lists and CRUD
of Users, Services and Policies, Credential creation and secret rotation, and
ClusterConfig management. Their updates preserve fields from the current
resource rather than sending partial replacement specifications.

## Breaking changes

- `authentication_timeout` takes a positive `Duration` instead of
  `Option<Duration>`.
- Request deadlines include authentication and transport readiness waits.
- Plaintext API endpoints require `allow_insecure_api(true)`.
- `auth_v1()` and `ClientBuilder::http_client()` were removed.
- `raw_channel()` and `HttpClient::inner()` return `Result` and reject closed
  clients.
- HTTP attaches `x-octelium-auth`, replaces alternate auth carriers and returns
  redirects without following them. Nondefault ports require authorized origins
  or a custom host policy.
- `Error` is cloneable. Shared source errors are held in `Arc`, and deadline,
  protocol, Session-change and fixed-token rejection failures have distinct
  variants.

## Features

| Feature | Default | Description |
| --- | --- | --- |
| `http` | yes | `Client::http`, an HTTP client for Octelium Services. |
| `tls-native-roots` | yes | Verify Cluster certificates against the host's certificate store. |
| `tls-webpki-roots` | no | Verify Cluster certificates against the bundled webpki roots. |

## License

Apache-2.0
