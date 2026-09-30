#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(missing_debug_implementations)]

mod client;
mod error;
mod exec;
mod files;
mod models;
mod resources;
mod spec;
mod stream;
mod terminal;
mod workspace;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use exec::{Command, ExecBuilder, ExecEvent, ExecInput, ExecResult, ExecSession, shell_quote};
pub use files::Files;
pub use models::{ListFilter, ListOptions, OrderBy, Page, Reference, SpaceMode, WaitOptions};
pub use resources::{
    GitProviders, Management, Member, Memberships, Regions, Role, Secrets, Snapshots, SpaceOptions,
    Spaces, Templates, UserConfig, UserSecrets, VolumeOptions, Volumes,
};
pub use spec::{Application, EnvValue, Resources, Task, TaskPhase, WorkspaceOptions};
pub use stream::{EventStream, StreamOptions};
pub use terminal::{Terminal, TerminalEvent, TerminalInput, TerminalOptions, Terminals};
pub use workspace::{SharingMode, StartOptions, State, Workspace, Workspaces};

/// Credential implementations and extension traits shared with the Octelium client.
pub use octelium::{
    AccessToken, AccessTokenProvider, AccessTokenProviderFn, Assertion, AuthenticationToken,
    Authenticator, StaticAccessToken,
};
#[cfg(feature = "http")]
/// HTTP request and response types used by the authenticated application client.
pub use octelium::{HttpAuthorizationPolicy, HttpClient, RequestBuilder, reqwest};
/// Reference-counted binary buffers used by streams and execution results.
pub use octelium_apis::bytes::{Bytes, BytesMut};
/// Complete generated Cordium v1 messages and gRPC clients for advanced uses.
pub use octelium_apis::cordiumv1 as proto;
/// Common protobuf metadata, references, and list options.
pub use octelium_apis::metav1 as meta;
/// Protobuf well-known types such as timestamps and structured values.
pub use octelium_apis::prost_types;
/// The transport library used by the generated clients.
pub use tonic;
