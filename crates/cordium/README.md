# Cordium Rust SDK

The async Rust SDK for Cordium: create isolated workspaces, run commands, move
files, attach to persistent terminals, and manage the resources around them.
Built on Tokio, tonic, and the shared Octelium authentication client. Rust 2024
edition; minimum supported Rust version (MSRV) **1.88**.

## Installation

Add the crate from crates.io:

```toml
[dependencies]
cordium = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures-util = "0.3" # for StreamExt
```

For local development, use `cordium = { path = "/path/to/sdk-rust/crates/cordium" }`.
Generated protobuf files are included: applications need neither `protoc` nor the
protobuf source repository to compile.

## A usable workspace in one call

```no_run
use cordium::{Client, Command, WorkspaceOptions};

# async fn example() -> cordium::Result<()> {
let client = Client::builder().domain("example.com").build().await?;
let workspace = client.workspaces().run(
    WorkspaceOptions::new().image("python:3.14").ephemeral(true),
).await?;
let result = workspace.exec(Command::argv(["python", "--version"])).await?;
println!("{}", result.stdout_text());
workspace.files().write_text("/tmp/hello.txt", "Hello from Rust!\n").await?;
workspace.delete().await?;
# Ok(())
# }
```

`create` returns a stopped workspace with a cluster-assigned name. `run` creates,
starts, and waits for RUNNING under a five-minute total deadline. Use `run_with`
for start-time variable overrides, placement, or a different wait deadline.
Once the workspace exists, a failure of a later step is reported as
`Error::RunFailed`, which carries the workspace so that it can be inspected or
deleted; deleting it is explicit. `start` on a starting or running workspace is
a no-op, and `wait_stopped` reports a failed run as `Error::WorkspaceFailed`.

Dropping a client or workspace does **not** stop or delete remote workspaces.
For cleanup after a command error, store its result, call `delete`, then propagate
the result. The [quickstart example](examples/quickstart.rs) shows this pattern.

## Authentication and connection settings

Explicit credentials take precedence over the environment:

```no_run
use cordium::{Assertion, AuthenticationToken, Client};

# async fn example() -> cordium::Result<()> {
let client = Client::builder()
    .domain("example.com")
    .authenticator(AuthenticationToken::new("<one-time authentication token>"))
    .build().await?;

let workload_client = Client::builder()
    .domain("example.com")
    .authenticator(Assertion::from_file("/var/run/identity/token"))
    .build().await?;
# Ok(())
# }
```

`Client::from_env()` reads `CORDIUM_DOMAIN`, falling back to `OCTELIUM_DOMAIN`.
Credentials follow this order:

| Variable | Behavior |
| --- | --- |
| `OCTELIUM_ACCESS_TOKEN` | Externally managed bearer token, used directly. |
| `OCTELIUM_ASSERTION_FILE` | Assertion file re-read on every authentication. |
| `OCTELIUM_ASSERTION` | Fixed assertion, exchanged once. |
| `OCTELIUM_AUTH_TOKEN` | One-time authentication token. |

Use `access_token_provider` for rotating externally managed tokens, and
`Assertion::from_fn` for renewable signed assertions. `Authenticator` and
`AccessTokenProvider` remain extensible for new cluster authentication methods.

Authentication is lazy. Concurrent callers share token state and serialize
exchanges. Canceling an individual request does not discard an ongoing exchange;
it can complete for other callers. Authentication has its own default 30-second
bound, customizable through `configure_transport`. One-time exchanges are never
replayed after failure or expiry. Session refresh uses a separate connection and
starts before token expiry. Credentials have redacted `Debug` implementations;
`access_token()` deliberately returns a sensitive string.

The default endpoint is `https://octelium-api.<domain>:443`, with certificate
verification. `api_endpoint` overrides it, including a plain HTTP endpoint for
local testing. `OCTELIUM_API_ENDPOINT` and `OCTELIUM_TLS_SERVER_NAME` also apply.
Advanced transport settings are exposed through the shared builder:

```no_run
# async fn example() -> cordium::Result<()> {
let client = cordium::Client::builder().domain("example.com")
    .configure_transport(|builder| {
        builder
            .add_root_ca_pem(b"<PEM CA certificate>".to_vec())
            .configure_endpoint(|endpoint| {
                endpoint.connect_timeout(std::time::Duration::from_secs(10))
            })
    })
    .build().await?;
# Ok(())
# }
```

`Client::from_transport` reuses an existing `octelium::Client`. Both clients
share credentials and connections; the caller keeps ownership of that transport,
so Cordium shutdown leaves it open.

## Workspace configuration

Consuming builders provide common settings; full generated specs cover the rest:

```no_run
use cordium::{Application, EnvValue, Resources, Task, TaskPhase, WorkspaceOptions};

let options = WorkspaceOptions::new()
    .template("dev.team")
    .display_name("API development")
    .image("node:24")
    .repository("https://github.com/myorg/api.git")
    .branch("main")
    .env("NODE_ENV", "development")
    .env("API_TOKEN", EnvValue::secret("api-token.team"))
    .variable("VERSION", "v2")
    .resources(Resources::new().cpu_millicores(2000).memory_mb(4096))
    .application(Application::new("web", 3000).default_app())
    .task(Task::new("install", "npm ci"))
    .task(Task::new("serve", "npm run dev").phase(TaskPhase::Start).background(true))
    .mount("cache.team", "/cache", false);
```

Template and snapshot sources are mutually exclusive. Snapshot restoration needs
persistent storage. Environment values can resolve Space Secrets without placing
their content in the specification. Compute units are **millicores** and **MB**.
Variables use the cluster's `${{ vars.NAME }}` substitution syntax. Mount paths
must be absolute, canonical and non-overlapping; server policy checks reserved
paths and storage compatibility.

`WorkspaceOptions::from_spec`, `image_spec`, `repository_spec`, and the `proto`
module support private registries, repository credentials, devcontainers,
additional repositories, capabilities, networking, and future configuration.
Template creation accepts the same builder, while rejecting workspace-only
applications, ephemeral storage, and source references. Set `git_provider` on a
Template to obtain authenticated git operations in its workspaces.

Cloned Workspace handles share a cache. Accessors perform no network operations
and return owned values. `refresh`, lifecycle methods, and `modify` update that
cache; watch events are independent. `watch` yields typed `WorkspaceEvent`s and
`logs` yields `LogEntry` values (timestamp, stage, stream and bytes). `proto()`,
`spec()`, and `status()` return owned copies. Unknown enum values remain
accessible through their raw protobuf integer fields.

## Commands and streaming

String commands use remote shell syntax. Use `Command::argv` when arguments
should be literal; every argument is POSIX-quoted without interpolation.

```no_run
# async fn example(workspace: cordium::Workspace) -> cordium::Result<()> {
use cordium::{Command, Error};

let result = workspace.exec(Command::argv(["printf", "%s", "$(literal text)"]))
    .cwd("/workspace/repo")
    .env("MODE", "test")
    .timeout(Some(std::time::Duration::from_secs(60)))
    .await?;

match workspace.exec("exit 7").check(true).await {
    Err(Error::CommandFailed(result)) => {
        println!("exit {}: {}", result.exit_code, result.stderr_text());
    }
    other => { other?; }
}
# Ok(())
# }
```

Nonzero exit codes are normal results by default; `.check(true)`, or
`ExecResult::check` afterwards, turns them into `Error::CommandFailed`. Output is binary `Bytes`; text accessors use
UTF-8 replacement while retaining the original bytes. Capture defaults to
**1 MiB per stdout/stderr stream**. Overflow sets `truncated`; streaming events
still carry complete output. Zero capture size disables capture.

```no_run
# async fn example(workspace: cordium::Workspace) -> cordium::Result<()> {
use cordium::ExecEvent;
use futures_util::StreamExt;

let mut session = workspace.exec("printf 'hello\n'").stream().await?;
while let Some(event) = session.next().await {
    match event? {
        ExecEvent::Stdout(bytes) => print!("{}", String::from_utf8_lossy(&bytes)),
        ExecEvent::Stderr(bytes) => eprint!("{}", String::from_utf8_lossy(&bytes)),
        ExecEvent::Exit(code) => println!("exit: {code}"),
        _ => {},
    }
}
let result = session.wait().await?;
# Ok(())
# }
```

Streams implement `futures_core::Stream`, are `Send`, and use backpressure instead
of background output tasks or unbounded SDK queues. An Exit releases the RPC even
if the server leaves it open. Dropping a session cancels its RPC; use
`session.input().kill().await` for an explicit remote termination request. A
killed command that the cluster does not report as exited within 10 seconds
(`.kill_grace(...)`) ends the session with the exit code -1 and `killed` set.

For interactive stdin, call `.stdin_enabled(true).stream().await`, clone
`session.input()`, and write while another task consumes output. Writes are
serialized and chunked to 32 KiB through a bounded queue. For collected execution,
`.stdin(bytes)` sends initial input while draining output. **Cordium's exec
protocol has no stdin EOF message**: commands must read a known amount or finish
independently. Closing a write handle does not signal EOF. File upload handles
this with exact-length base64 framing.

## Files and terminals

`workspace.files()` provides binary/text reads and writes, uploads, downloads,
`mkdir`, and `remove`. Paths are literal: no `$HOME`, glob or tilde expansion.
Reads cap memory at 64 MiB and reject larger files. `max_read_bytes` adjusts that
bound; uploads, `upload_reader`, and `download_to` stream with bounded chunks.
Text reads use strict UTF-8. `as_root` and `timeout` customize transfer commands.

Transfers require POSIX `sh`, `head`, `base64`, `cat`, `mkdir`, and `rm` in the
image. Remote writes may leave partial content after failure. Local downloads
use a private temporary file in the destination directory and replace an existing
destination only after successful execution and flushing. Use repositories,
object storage, or SSH/SFTP for bulk data.

```no_run
# async fn example(workspace: cordium::Workspace) -> cordium::Result<()> {
use cordium::{TerminalOptions, TerminalEvent};
use futures_util::StreamExt;

let mut terminal = workspace.terminals().create(TerminalOptions::default().size(120, 40)).await?;
let input = terminal.input(); // can move to another task while consuming events
input.write("printf 'hello\n'\n").await?;
if let Some(Ok(TerminalEvent::Output(bytes))) = terminal.next().await {
    print!("{}", String::from_utf8_lossy(&bytes));
}
terminal.remove().await?; // terminate the persistent remote shell
# Ok(())
# }
```

Terminal Drop or `detach` ends only the local subscription. The shell survives
and can be found with `list` and reattached with `attach`. `remove` explicitly
terminates it. Several clients can attach to one terminal. Input and resize
operations are available on a clonable `TerminalInput`; local raw terminal mode
is the caller's responsibility.

## Resources and pagination

| Collection | Capabilities |
| --- | --- |
| `workspaces` | Create/run, get/update/delete, list/all, watch; handle lifecycle, logs, sharing, exec, files, terminals. |
| `spaces` | Create/update/delete, list/all, edit full specs, leave. |
| `templates` | Create/update/delete, list/all, build/cancel, wait for an explicit build ID. |
| `snapshots` | Create/get/delete, list/all, wait for READY. |
| `volumes` | Create/update/delete, list/all, monotonic capacity growth, wait for READY. |
| `secrets` | Write-only text/binary/JSON creation, metadata reads/list/all, delete. |
| `user_secrets` | Personal text/binary/JSON secrets, updates, cluster-generated SSH keys. |
| `git_providers` | GitHub/GitLab/generic OAuth2 creation, get/update/delete, list/all. |
| `memberships` | Add by User reference or email, read own membership, role edits, delete, list/all. |
| `regions` | List/all hosting Regions. |
| `user_config` | Get/update/modify preferences, preferred Region, dotfiles. |
| `management` | Administrator get/update/modify of the sole ClusterConfig. |

Collection operations return complete generated resource types, preserving
advanced server fields. `modify` fetches, invokes a synchronous edit callback,
and updates exactly once; errors in the callback abort the update. There are no
automatic mutation retries or conflict-resolution guesses. Full resource updates
can race other writers; applications should handle any server conflict response.

```no_run
# async fn example(client: cordium::Client) -> cordium::Result<()> {
use cordium::{ListOptions, Reference};
use futures_util::StreamExt;

let workspace = client.workspaces().get(Reference::uid("workspace-uid")).await?;
let mut all = client.workspaces().all(ListOptions::new().in_space("team.cordium").page_size(100));
while let Some(workspace) = all.next().await { println!("{}", workspace?.name()); }
client.spaces().modify("team.cordium", |space| {
    space.metadata.as_mut().expect("server metadata").display_name = "Team".into();
    Ok(())
}).await?;
# Ok(())
# }
```

Pages are zero-based, carry server pagination metadata, and `all` fetches lazily.
Unsupported filters and pagination without progress fail explicitly. References
encode exactly one name or UID. Organization Space creation qualifies short names
with `.cordium` through `SpaceOptions::organization`.

Snapshots are taken online without stopping a Workspace. Wait for READY, then
create with `WorkspaceOptions::snapshot`. A Volume is independent persistent
storage, belongs to one Space and Region, and may require its first mount before
provisioning. Shared volumes require a shared storage backend. Capacity cannot
shrink. Template pre-build waits take an explicit ID to avoid mistaking a prior
successful build for the requested one.

## Deadlines, cancellation, and errors

| Operation | Default deadline |
| --- | --- |
| Unary SDK call, including authentication | 30 seconds, configurable on `ClientBuilder`. |
| Authentication/refresh exchange | 30 seconds, configured on the transport builder. |
| Workspace run or readiness wait | 300 seconds total, configured with `WaitOptions`. |
| File transfer | 300 seconds total. |
| Execution and server streams | Unlimited; set their total deadline explicitly. |

Durations are `std::time::Duration`; `None` disables a deadline. Positive finite
representable durations are required. Composite waits and stream deadlines cover
all their work rather than restarting on each event. Polling is used for resources
that do not publish readiness events; workspace watch streams are also exposed.
Drop a pending future/stream to cancel, or use `tokio::select!` with your own
cancellation signal. Streams progress when polled.

`Client::close()` cancels high-level SDK calls and subscriptions across clones,
clears local credentials, and prevents new calls. Connections are released when
all owners drop. Existing generated RPC calls and HTTP responses obtained through
escape hatches have their own transport lifetimes and should be dropped directly.
No remote resource is implicitly deleted and closing does not log out a Session.

`Error` is a non-exhaustive enum. `Status` preserves tonic status codes, details,
and metadata; `code()` exposes known gRPC codes. `CommandFailed` keeps binary
output and exit status, and `WorkspaceFailed` includes the last full resource.
Caller cancellation is normal Rust future cancellation; no synthetic success or
silent output loss is substituted. Non-idempotent API calls are never replayed.

## HTTP and extension APIs

With the default `http` feature, `client.http()` authenticates requests to workspace
application URLs. The Octelium policy accepts HTTPS for the cluster domain and its
subdomains; alternate hosts require explicit authorization through
`configure_transport`. Use a request timeout and stream large response bodies.
HTTP error status codes remain available on the reqwest response.

`main_service`, `workspace_service`, `management_service`, and `transport` are
escape hatches. Generated clients authenticate automatically, but high-level SDK
operation deadlines/cancellation are not applied to those raw calls. The `proto`,
`meta`, and `prost_types` re-exports keep model and runtime versions aligned.
The generated API crate is pinned to its tested version because additive protobuf
fields can change Rust struct construction. Regenerate, update that pin, and run
the workspace checks together when upgrading the API schema.

| Feature | Default | Purpose |
| --- | --- | --- |
| `http` | yes | Authenticated reqwest HTTP client for applications. |
| `tls-native-roots` | yes | Host certificate store for gRPC TLS. |
| `tls-webpki-roots` | no | Bundled webpki roots for gRPC TLS. |

No blocking facade or hidden runtime is created. Async clients work with a Tokio
runtime, including a current-thread runtime. Reuse one client per cluster, then
clone it cheaply for concurrent tasks.

## Examples and development

The examples include [quickstart](examples/quickstart.rs),
[streaming commands](examples/stream_command.rs),
[lazy workspace listing](examples/list_workspaces.rs),
[template pre-builds](examples/prebuild.rs), and
[HTTP applications](examples/http_application.rs).

```sh
cargo run -p cordium --example quickstart
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
python3 scripts/generate-cordium.py ../pb --check
```

Public APIs are documented in rustdoc, and missing public documentation is a
compile error. Integration tests run real generated tonic servers and POSIX shell
processes on loopback, including binary transfers and authentication cancellation.
A real cluster smoke test remains a release check; local tests do not verify a
particular cluster's policies, storage backend, or deployment.

Release dependency order: `octelium-apis` 0.1.4, `octelium` 0.2.0, then `cordium`
0.3.0. Packaging all workspace crates together verifies the local dependency
chain. Nothing is committed or published by the development commands above.
