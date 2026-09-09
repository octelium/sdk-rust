# octelium

The official Rust SDK for [Octelium](https://octelium.com).

A `Client` authenticates to an Octelium Cluster, keeps the resulting Session
fresh, and hands out ready-to-use gRPC and HTTP clients.

```sh
cargo add octelium octelium-apis
```

## Quickstart

```rust,no_run
use octelium::Client;
use octelium_apis::corev1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reads OCTELIUM_DOMAIN and the credentials from the environment.
    let client = Client::from_env().await?;

    let users = client
        .core_v1()
        .list_user(corev1::ListUserOptions::default())
        .await?
        .into_inner();

    for user in users.items {
        println!("{}", user.metadata.unwrap_or_default().name);
    }

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
    .scopes(["core.user.read"])
    .build()
    .await?;
# Ok(())
# }
```

An application can implement the `Authenticator` or `AccessTokenProvider`
traits to plug in its own credential source, including authentication methods
added after this release.

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

## HTTP

`Client::http()` returns an HTTP client that attaches the access token to
Octelium Services. By default only the Cluster domain and its subdomains, over
HTTPS, receive the token, so a redirect or a mistyped URL cannot leak the
credential:

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

## Sessions

The Client obtains an access token on the first call that needs one, and
replaces it shortly before it expires. Concurrent callers share one in-flight
authentication. A Session created from a reusable credential, such as an
assertion, is re-created after it expires; one created from a one-time
authentication token is not, and the Client then returns `Error::SessionExpired`.

`Client` is cheap to clone and safe to share across tasks: every clone shares
one Session, one token cache and one HTTP/2 connection.

## Features

| Feature | Default | Description |
| --- | --- | --- |
| `http` | yes | `Client::http`, an HTTP client for Octelium Services. |
| `tls-native-roots` | yes | Verify Cluster certificates against the host's certificate store. |
| `tls-webpki-roots` | no | Verify Cluster certificates against the bundled webpki roots. |

## License

Apache-2.0
