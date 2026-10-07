# Rust SDKs for Cordium and Octelium

The [Cordium SDK](crates/cordium/README.md) provides ergonomic async workspace
lifecycle, command execution, files, terminals, storage, and resource management.
Start with its [quickstart example](crates/cordium/examples/quickstart.rs).
The `octelium` crate supplies the shared authenticated transport, and
`octelium-apis` supplies generated protobuf models and clients.

## Octelium Rust SDK

The official Rust SDK for [Octelium](https://octelium.com), plus the generated
protobuf types and gRPC clients for the Cluster APIs.

| Crate | Description |
| --- | --- |
| [`cordium`](crates/cordium) | Async workspace lifecycle, execution, files, terminals, storage, and resources. |
| [`octelium`](crates/octelium) [![crates.io](https://img.shields.io/crates/v/octelium.svg)](https://crates.io/crates/octelium) [![docs.rs](https://img.shields.io/docsrs/octelium)](https://docs.rs/octelium) | Authenticated gRPC and HTTP clients for Octelium Clusters. |
| [`octelium-apis`](crates/octelium-apis) [![crates.io](https://img.shields.io/crates/v/octelium-apis.svg)](https://crates.io/crates/octelium-apis) [![docs.rs](https://img.shields.io/docsrs/octelium-apis)](https://docs.rs/octelium-apis) | Generated `prost` types and `tonic` clients for the Cluster APIs. |

```sh
cargo add octelium octelium-apis
```

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

    Ok(())
}
```

See [crates/octelium/README.md](crates/octelium/README.md) for credentials,
Session handling, the HTTP client and the feature flags, or the API
documentation on [docs.rs](https://docs.rs/octelium).

## Examples

```sh
export OCTELIUM_DOMAIN=example.com
export OCTELIUM_AUTH_TOKEN=...

cargo run --example list_users
cargo run -p octelium --example users -- --help
cargo run -p octelium --example services -- --help
cargo run -p octelium --example policies -- --help
cargo run -p octelium --example credentials -- --help
cargo run -p octelium --example cluster_config -- --help
cargo run --example http_service -- https://my-service.example.com/api
```

See the [Core API examples](crates/octelium/examples/README.md) for practical
resource workflows, pagination, credential rotation and ClusterConfig updates.

## Supported Rust version

The SDK builds on Rust 1.88 and later, which is the minimum required by
`tonic` 0.14.

## Development

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
cargo fmt --all --check
```

`crates/octelium-apis` is generated from the Octelium protobuf definitions and
synced into this repository by CI. Do not edit it by hand; open an issue for
anything that needs to change there.

## Other SDKs

- [Go](https://github.com/octelium/octelium/tree/main/octelium-go)
- [TypeScript](https://github.com/octelium/sdk-typescript)
- [Python](https://github.com/octelium/sdk-python)

## License

Apache-2.0. See [LICENSE](LICENSE).
