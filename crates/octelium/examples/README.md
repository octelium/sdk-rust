# Core API examples

These programs manage actual Octelium resources with the generated Core API.
Run them from the repository root with credentials authorized for the selected
operation:

```sh
export OCTELIUM_DOMAIN=example.com
export OCTELIUM_ACCESS_TOKEN='<administrator access token>'

cargo run -p octelium --example users -- --help
cargo run -p octelium --example services -- --help
cargo run -p octelium --example policies -- --help
cargo run -p octelium --example credentials -- --help
cargo run -p octelium --example cluster_config -- --help
```

Help runs without a domain or credentials. Each API call has a ten-second
deadline, including authentication. Every program shuts the client down after
success or failure. All list commands follow `list_response_meta.has_more`
starting at page zero.

Use an administrator access token for workflows spanning several commands.
`OCTELIUM_AUTH_TOKEN` can also authenticate a command, but a one-time Credential
cannot be exchanged again by the next process. A rotating assertion file can
instead be supplied with `OCTELIUM_ASSERTION_FILE`.

## Create a workload and a protected HTTP Service

This example creates a workload User for a CI job, grants that User access with
a CEL Policy, and publishes a protected HTTP Service backed by an existing
Kubernetes application. The `default` Namespace and the upstream must already
exist.

```sh
cargo run -p octelium --example users -- create ci-agent workload
cargo run -p octelium --example policies -- create orders-access \
  "ctx.user.metadata.name == 'ci-agent'"
cargo run -p octelium --example services -- create default.orders-api \
  http://orders.default.svc:8080 orders-access true

cargo run -p octelium --example users -- get ci-agent
cargo run -p octelium --example services -- get default.orders-api
cargo run -p octelium --example policies -- get orders-access
```

The Service is public and authenticated. Its Policy determines who may access
it. The upstream remains private to the Cluster; `true` enables public access
and does not enable anonymous access.

## List, update and delete Users

The Group names supplied here must already exist. Use a comma-separated list
for memberships. `-` clears memberships or the email field.

```sh
cargo run -p octelium --example users -- list
cargo run -p octelium --example users -- create alice human alice@example.com engineering
cargo run -p octelium --example users -- update alice email alice@new-company.example
cargo run -p octelium --example users -- update alice groups engineering,on-call
cargo run -p octelium --example users -- update alice groups -
cargo run -p octelium --example users -- update alice enabled false
cargo run -p octelium --example users -- delete alice
```

Workload Users have no human email. An email update for a workload is rejected
before any write.

## Update Services and Policy rules

```sh
cargo run -p octelium --example services -- list
cargo run -p octelium --example services -- update default.orders-api upstream https://orders.internal:8443
cargo run -p octelium --example services -- update default.orders-api policies orders-access
cargo run -p octelium --example services -- update default.orders-api enabled false

cargo run -p octelium --example policies -- list
cargo run -p octelium --example policies -- update orders-access allow-access \
  "ctx.user.metadata.name == 'ci-agent'"
cargo run -p octelium --example policies -- set-enabled orders-access true
```

A Policy created by the example has one `ALLOW` rule named `allow-access`.
Updates find the named rule and preserve the other rules, effects, priorities,
enforcement rules and attributes. A missing rule is an error.

## Create, issue, rotate and disable Credentials

Creating a Credential resource does not issue its secret. The `token` command
issues a secret, or rotates the existing secret and invalidates its old value.
Only this command prints credential secrets.

```sh
cargo run -p octelium --example credentials -- create ci-bootstrap ci-agent auth-token 24
cargo run -p octelium --example credentials -- token ci-bootstrap

cargo run -p octelium --example credentials -- create ci-oauth ci-agent oauth2 720
cargo run -p octelium --example credentials -- token ci-oauth

cargo run -p octelium --example credentials -- create ci-access ci-agent access-token 24
cargo run -p octelium --example credentials -- token ci-access
cargo run -p octelium --example credentials -- token ci-access
cargo run -p octelium --example credentials -- list ci-agent
cargo run -p octelium --example credentials -- get ci-access
cargo run -p octelium --example credentials -- set-enabled ci-access false
```

The examples create clientless Credentials. Authentication tokens permit one
exchange by default; OAuth2 and access-token Credentials require workload
Users. Expiry is specified in hours, from 1 through 17,520. OAuth2 secrets are
used with the Cluster's client-credentials endpoint; access tokens can be
passed to `ClientBuilder::access_token`.

## Manage ClusterConfig

```sh
cargo run -p octelium --example cluster_config -- get
cargo run -p octelium --example cluster_config -- set-session-limit human 20
cargo run -p octelium --example cluster_config -- set-session-limit workload 100
```

The limit must be between 1 and 1,000. Each update reads the current
ClusterConfig and changes only the selected Session limit, preserving the
other Session type, durations, ingress, authorization and remaining settings.

## Remove the workflow resources

Delete dependent resources before removing their User:

```sh
cargo run -p octelium --example credentials -- delete ci-bootstrap
cargo run -p octelium --example credentials -- delete ci-oauth
cargo run -p octelium --example credentials -- delete ci-access
cargo run -p octelium --example services -- delete default.orders-api
cargo run -p octelium --example policies -- delete orders-access
cargo run -p octelium --example users -- delete ci-agent
```

Updates use get, modify, then update because the Core API accepts complete
resource specifications. They preserve fields from the read, but are not
atomic patches and can overwrite concurrent changes. Errors propagate and
mutations are never automatically replayed.
