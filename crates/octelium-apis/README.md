# octelium-apis

Generated Rust protobuf types and [tonic](https://github.com/hyperium/tonic)
gRPC clients for the [Octelium](https://octelium.com) Cluster APIs.

This crate contains generated code only. For an authenticated client that
handles Cluster authentication, Session refresh and transport setup, use the
[`octelium`](https://docs.rs/octelium) crate.

## Installation

```sh
cargo add octelium-apis
```

## Layout

Every protobuf package is available under its full path, and the main Cluster
APIs are additionally re-exported under the short names used by the other
Octelium SDKs:

| Alias | Protobuf package |
| --- | --- |
| `metav1` | `octelium.api.main.meta.v1` |
| `corev1` | `octelium.api.main.core.v1` |
| `authv1` | `octelium.api.main.auth.v1` |
| `userv1` | `octelium.api.main.user.v1` |
| `cordiumv1` | `octelium.api.main.cordium.v1` |

```rust,no_run
use octelium_apis::corev1;

let opts = corev1::ListUserOptions::default();

// The same type through its protobuf package path.
let _: octelium_apis::octelium::api::main::core::v1::ListUserOptions = opts;
```

## Usage

Each Service exposes a generated gRPC client that accepts any
[`tonic::client::GrpcService`], including a plain
[`tonic::transport::Channel`] or the authenticated channel produced by the
`octelium` crate:

```rust,no_run
use octelium_apis::corev1;
use octelium_apis::corev1::main_service_client::MainServiceClient;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let channel = tonic::transport::Channel::from_static("https://octelium-api.example.com")
    .connect()
    .await?;

let mut client = MainServiceClient::new(channel);

let users = client
    .list_user(corev1::ListUserOptions::default())
    .await?
    .into_inner();

for user in users.items {
    println!("{}", user.metadata.unwrap_or_default().name);
}
# Ok(())
# }
```

## Features

| Feature | Default | Description |
| --- | --- | --- |
| `client` | yes | gRPC clients for the Cluster Services. |
| `channel` | yes | tonic's batteries-included HTTP/2 channel. Turn it off to supply another transport, such as `tonic-web-wasm-client`. |
| `server` | no | gRPC server stubs, for implementing a Cluster Service. |

## Notes

- `bytes` fields are generated as [`bytes::Bytes`] rather than `Vec<u8>`, which
  avoids a copy of every payload on decode. `bytes` is re-exported by this
  crate.
- Well-known types are the `prost-types` ones, also re-exported by this crate.
- Message type names are enabled, so `google.protobuf.Any` values can be packed
  and unpacked with [`prost::Name`].
- Clients have no generated `connect` constructor, because a Service may define
  an RPC of that name. Build a transport and pass it to `Client::new`.

## Generation

This crate is generated from the Octelium protobuf definitions and synced into
[octelium/sdk-rust](https://github.com/octelium/sdk-rust) by CI. Do not edit it
by hand.

## License

Apache-2.0
