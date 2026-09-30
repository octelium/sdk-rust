use crate::{
    Client, Error, EventStream, ListFilter, ListOptions, Page, Reference, Result, StreamOptions,
    WaitOptions, WorkspaceOptions, meta, proto,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

/// Generated lifecycle state enum. Unknown future values remain available in the raw status.
pub use proto::workspace::status::State;
/// Audience allowed to access a shared named Application.
#[derive(Clone, Copy, Debug)]
pub enum SharingMode {
    /// Members of this Workspace's Space.
    Members,
    /// All authenticated users in the cluster.
    All,
}
/// Run-specific variables and placement; they do not alter the stored spec.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StartOptions {
    /// Variable overrides for this run.
    pub variables: BTreeMap<String, String>,
    /// Optional hosting Region.
    pub region: Option<Reference>,
}
impl StartOptions {
    /// Adds a variable override.
    pub fn variable(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.variables.insert(name.into(), value.into());
        self
    }
    /// Selects the hosting Region.
    pub fn region(mut self, v: impl Into<Reference>) -> Self {
        self.region = Some(v.into());
        self
    }
    pub(crate) fn config(&self) -> Result<proto::start_workspace_request::Config> {
        for name in self.variables.keys() {
            crate::error::nonempty(name, "run variable name")?;
        }
        Ok(proto::start_workspace_request::Config {
            vars: self
                .variables
                .iter()
                .map(|(name, value)| proto::workspace::spec::Var {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            region_ref: self.region.as_ref().map(Reference::object).transpose()?,
        })
    }
}
/// Workspace collection. Workspaces are created stopped with server-assigned names.
#[derive(Clone, Debug)]
pub struct Workspaces {
    client: Client,
}
impl Workspaces {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Creates a stopped Workspace. An omitted source uses the user's default Template.
    pub async fn create(&self, mut options: WorkspaceOptions) -> Result<Workspace> {
        options.validate()?;
        if !options.git_provider.is_empty() {
            return Err(Error::InvalidArgument(
                "GitProvider is configured on Templates".into(),
            ));
        }
        let resource = proto::Workspace {
            metadata: Some(meta::Metadata {
                display_name: options.display_name,
                ..Default::default()
            }),
            spec: Some(options.spec),
            status: Some(proto::workspace::Status {
                template_ref: options
                    .template
                    .as_ref()
                    .map(Reference::object)
                    .transpose()?,
                workspace_snapshot_ref: options
                    .snapshot
                    .as_ref()
                    .map(Reference::object)
                    .transpose()?,
                ..Default::default()
            }),
            ..Default::default()
        };
        let resource = self
            .client
            .rpc(self.client.main_service().create_workspace(resource))
            .await?;
        Workspace::new(self.client.clone(), resource)
    }
    /// Creates, starts, and waits for RUNNING under one five-minute deadline.
    /// A failure preserves the Workspace for inspection; deletion is always explicit.
    pub async fn run(&self, options: WorkspaceOptions) -> Result<Workspace> {
        self.run_with(options, StartOptions::default(), WaitOptions::default())
            .await
    }
    /// Creates and starts with placement, variable overrides, and a total deadline.
    pub async fn run_with(
        &self,
        mut options: WorkspaceOptions,
        start: StartOptions,
        wait: WaitOptions,
    ) -> Result<Workspace> {
        let wait = wait.validate()?;
        options.validate()?;
        start.config()?;
        self.client
            .operation(wait.timeout, async {
                let ws = self.create(options).await?;
                ws.start(start).await?;
                ws.wait_loop(WaitTarget::Running, wait).await?;
                Ok(ws)
            })
            .await
    }
    /// Fetches a Workspace by name or UID.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<Workspace> {
        let r = reference.into().get()?;
        Workspace::new(
            self.client.clone(),
            self.client
                .rpc(self.client.main_service().get_workspace(r))
                .await?,
        )
    }
    /// Replaces display name and spec using a full resource; other server fields are preserved.
    pub async fn update(&self, resource: proto::Workspace) -> Result<Workspace> {
        let r = self
            .client
            .rpc(self.client.main_service().update_workspace(resource))
            .await?;
        Workspace::new(self.client.clone(), r)
    }
    /// Deletes a Workspace and its private storage.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_workspace(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Lists one page, optionally filtering by Space or Template.
    pub async fn list(&self, options: ListOptions) -> Result<Page<Workspace>> {
        let filter = match &options.filter {
            None => None,
            Some(ListFilter::Space(r)) => {
                Some(proto::list_workspace_options::Filter::SpaceRef(r.object()?))
            }
            Some(ListFilter::Template(r)) => Some(
                proto::list_workspace_options::Filter::TemplateRef(r.object()?),
            ),
            _ => {
                return Err(Error::InvalidArgument(
                    "Workspaces support Space or Template filters".into(),
                ));
            }
        };
        let page = self
            .client
            .rpc(
                self.client
                    .main_service()
                    .list_workspace(proto::ListWorkspaceOptions {
                        common: Some(options.common()),
                        filter,
                    }),
            )
            .await?;
        Ok(Page::new(
            page.items
                .into_iter()
                .map(|r| Workspace::new(self.client.clone(), r))
                .collect::<Result<_>>()?,
            page.list_response_meta,
        ))
    }
    /// Lazily fetches pages. Dropping the stream prevents further requests.
    pub fn all(&self, mut options: ListOptions) -> EventStream<Workspace> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
    /// Watches all owned Workspaces, or one reference. Events do not update cached handles.
    pub async fn watch(
        &self,
        reference: Option<Reference>,
        options: StreamOptions,
    ) -> Result<EventStream<proto::WatchWorkspaceResponse>> {
        let deadline = options.deadline()?;
        let request = proto::WatchWorkspaceRequest {
            workspace_ref: reference.as_ref().map(Reference::object).transpose()?,
        };
        let rpc = self
            .client
            .operation_until(deadline, async {
                self.client
                    .rpc(self.client.main_service().watch_workspace(request))
                    .await
            })
            .await?;
        Ok(crate::stream::rpc_stream(
            self.client.clone(),
            rpc,
            deadline,
        ))
    }
}
/// A cheap-to-clone Workspace handle with a shared, locally cached resource snapshot.
///
/// Accessors perform no I/O. [`Self::refresh`] and lifecycle methods update the
/// cache; watch events do not. Dropping a handle leaves the remote resource intact.
#[derive(Clone, Debug)]
pub struct Workspace {
    pub(crate) client: Client,
    resource: Arc<RwLock<proto::Workspace>>,
    reference: Reference,
}
#[derive(Clone, Copy)]
enum WaitTarget {
    Ready,
    Running,
    Stopped,
}
impl Workspace {
    pub(crate) fn new(client: Client, resource: proto::Workspace) -> Result<Self> {
        let md = resource
            .metadata
            .as_ref()
            .ok_or_else(|| Error::Protocol("Workspace metadata is missing".into()))?;
        let reference = if !md.uid.is_empty() {
            Reference::uid(md.uid.clone())
        } else {
            Reference::name(md.name.clone())
        };
        reference.object()?;
        Ok(Self {
            client,
            resource: Arc::new(RwLock::new(resource)),
            reference,
        })
    }
    fn read<T>(&self, f: impl FnOnce(&proto::Workspace) -> T) -> T {
        f(&self.resource.read().unwrap_or_else(|e| e.into_inner()))
    }
    /// Returns an owned copy of the cached resource for advanced inspection or updates.
    pub fn proto(&self) -> proto::Workspace {
        self.resource
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// Cluster-assigned name.
    pub fn name(&self) -> String {
        self.read(|w| {
            w.metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default()
        })
    }
    /// Cluster-wide UID.
    pub fn uid(&self) -> String {
        self.read(|w| {
            w.metadata
                .as_ref()
                .map(|m| m.uid.clone())
                .unwrap_or_default()
        })
    }
    /// Human-readable display name.
    pub fn display_name(&self) -> String {
        self.read(|w| {
            w.metadata
                .as_ref()
                .map(|m| m.display_name.clone())
                .unwrap_or_default()
        })
    }
    /// Resource creation timestamp, if reported.
    pub fn created_at(&self) -> Option<crate::prost_types::Timestamp> {
        self.read(|w| w.metadata.as_ref().and_then(|m| m.created_at))
    }
    /// Last lifecycle state. Inspect `proto().status.state` to retain an unknown numeric value.
    pub fn state(&self) -> State {
        self.read(|w| {
            w.status
                .as_ref()
                .map(|s| s.state())
                .unwrap_or(State::Unknown)
        })
    }
    /// True from PREPARING through RUNNING, when execution and terminals are accepted.
    pub fn is_ready(&self) -> bool {
        matches!(self.state(), State::Preparing | State::Running)
    }
    /// True when initialization has fully completed.
    pub fn is_running(&self) -> bool {
        self.state() == State::Running
    }
    /// True while the container is being provisioned.
    pub fn is_starting(&self) -> bool {
        matches!(
            self.state(),
            State::InitRequest
                | State::Initializing
                | State::PullingImage
                | State::BuildingImage
                | State::StartingRuntime
        )
    }
    /// True while a graceful stop is in progress.
    pub fn is_stopping(&self) -> bool {
        matches!(self.state(), State::StoppingRequest | State::Stopping)
    }
    /// True when stopped.
    pub fn is_stopped(&self) -> bool {
        self.state() == State::Stopped
    }
    /// Returns the public hostname, if the Workspace is running.
    pub fn hostname(&self) -> Option<String> {
        let h = self.read(|w| {
            w.status
                .as_ref()
                .map(|s| s.hostname.clone())
                .unwrap_or_default()
        });
        (!h.is_empty()).then_some(h)
    }
    /// Returns the default Application URL, if running.
    pub fn url(&self) -> Option<String> {
        self.hostname().map(|h| format!("https://{h}"))
    }
    /// Returns a named Application URL. Authorization is still required.
    pub fn app_url(&self, name: &str) -> Result<Option<String>> {
        crate::spec::validate_label(name)?;
        let Some(h) = self.hostname() else {
            return Ok(None);
        };
        let default = self
            .applications()
            .iter()
            .any(|a| a.name == name && a.is_default);
        Ok(Some(if default {
            format!("https://{h}")
        } else {
            format!("https://{name}_{h}")
        }))
    }
    /// Returns a proxy URL for a nonzero TCP port.
    pub fn port_url(&self, port: u16) -> Result<Option<String>> {
        if port == 0 {
            return Err(Error::InvalidArgument("port must be nonzero".into()));
        }
        Ok(self.hostname().map(|h| format!("https://port_{port}_{h}")))
    }
    /// Returns an owned copy of the cached specification.
    pub fn spec(&self) -> proto::workspace::Spec {
        self.read(|w| w.spec.clone().unwrap_or_default())
    }
    /// Returns an owned copy of the last server status.
    pub fn status(&self) -> proto::workspace::Status {
        self.read(|w| w.status.clone().unwrap_or_default())
    }
    /// Returns configured named applications.
    pub fn applications(&self) -> Vec<proto::workspace::spec::Application> {
        self.read(|w| {
            w.spec
                .as_ref()
                .map(|s| s.applications.clone())
                .unwrap_or_default()
        })
    }
    /// Returns whether private storage is discarded on stop.
    pub fn is_ephemeral(&self) -> bool {
        self.read(|w| w.spec.as_ref().is_some_and(|s| s.is_ephemeral))
    }
    /// Returns the stable reference used by operations on this handle.
    pub fn reference(&self) -> Reference {
        self.reference.clone()
    }
    pub(crate) fn object(&self) -> Result<meta::ObjectReference> {
        self.reference.object()
    }
    fn replace(&self, resource: proto::Workspace) {
        *self.resource.write().unwrap_or_else(|e| e.into_inner()) = resource;
    }
    /// Fetches fresh state and replaces the cache shared by handle clones.
    pub async fn refresh(&self) -> Result<()> {
        let r = self
            .client
            .rpc(
                self.client
                    .main_service()
                    .get_workspace(self.reference.get()?),
            )
            .await?;
        self.replace(r);
        Ok(())
    }
    /// Requests start, then refreshes cached state; does not wait for readiness.
    pub async fn start(&self, options: StartOptions) -> Result<()> {
        self.client
            .operation(self.client.default_timeout(), async {
                let config = options.config()?;
                self.client
                    .rpc(
                        self.client
                            .main_service()
                            .start_workspace(proto::StartWorkspaceRequest {
                                workspace_ref: Some(self.object()?),
                                config: Some(config),
                            }),
                    )
                    .await?;
                self.refresh().await
            })
            .await
    }
    /// Requests a graceful stop and refreshes the cache; does not wait for STOPPED.
    pub async fn stop(&self) -> Result<()> {
        self.client
            .operation(self.client.default_timeout(), async {
                self.client
                    .rpc(
                        self.client
                            .main_service()
                            .stop_workspace(proto::StopWorkspaceRequest {
                                workspace_ref: Some(self.object()?),
                            }),
                    )
                    .await?;
                self.refresh().await
            })
            .await
    }
    /// Deletes the remote Workspace and its private storage. The local handle remains inspectable.
    pub async fn delete(&self) -> Result<()> {
        self.client.workspaces().delete(&self.reference).await
    }
    /// Fetches, edits, and updates once. The callback receives a complete owned resource.
    /// No retries are made; concurrent server changes may require application conflict handling.
    pub async fn modify<F>(&self, edit: F) -> Result<()>
    where
        F: FnOnce(&mut proto::Workspace) -> Result<()> + Send,
    {
        self.client
            .operation(self.client.default_timeout(), async {
                self.refresh().await?;
                let mut resource = self.proto();
                edit(&mut resource)?;
                let r = self
                    .client
                    .rpc(self.client.main_service().update_workspace(resource))
                    .await?;
                self.replace(r);
                Ok(())
            })
            .await
    }
    /// Waits for PREPARING or RUNNING. Startup or stop failures include the last resource.
    pub async fn wait_ready(&self, options: WaitOptions) -> Result<()> {
        self.wait(WaitTarget::Ready, options).await
    }
    /// Waits for RUNNING under a total deadline.
    pub async fn wait_running(&self, options: WaitOptions) -> Result<()> {
        self.wait(WaitTarget::Running, options).await
    }
    /// Waits for STOPPED, allowing a failed run to finish stopping.
    pub async fn wait_stopped(&self, options: WaitOptions) -> Result<()> {
        self.wait(WaitTarget::Stopped, options).await
    }
    async fn wait(&self, target: WaitTarget, options: WaitOptions) -> Result<()> {
        let options = options.validate()?;
        self.client
            .operation(options.timeout, self.wait_loop(target, options))
            .await
    }
    async fn wait_loop(&self, target: WaitTarget, options: WaitOptions) -> Result<()> {
        loop {
            self.refresh().await?;
            let ws = self.proto();
            let s = ws.status.clone().unwrap_or_default();
            let done = match target {
                WaitTarget::Ready => matches!(s.state(), State::Preparing | State::Running),
                WaitTarget::Running => s.state() == State::Running,
                WaitTarget::Stopped => s.state() == State::Stopped,
            };
            if done {
                return Ok(());
            }
            if !matches!(target, WaitTarget::Stopped)
                && (s.failure.is_some()
                    || matches!(
                        s.state(),
                        State::StoppingRequest | State::Stopping | State::Stopped
                    ))
            {
                return Err(Error::WorkspaceFailed {
                    message: s
                        .failure
                        .as_ref()
                        .map(|f| f.message.clone())
                        .unwrap_or_else(|| format!("workspace is {}", s.state().as_str_name())),
                    workspace: Box::new(ws),
                });
            }
            tokio::time::sleep(options.poll_interval).await;
        }
    }
    /// Creates an online or stopped-storage snapshot without restarting the Workspace.
    pub async fn snapshot(&self, name: impl Into<String>) -> Result<proto::WorkspaceSnapshot> {
        self.client.snapshots().create(name, &self.reference).await
    }
    /// Shares a named Application and refreshes sharing status.
    pub async fn share_port(&self, name: &str, mode: SharingMode) -> Result<()> {
        self.client
            .operation(self.client.default_timeout(), async {
                crate::spec::validate_label(name)?;
                self.client
                    .rpc(self.client.main_service().share_workspace_port(
                        proto::ShareWorkspacePortRequest {
                            workspace_ref: Some(self.object()?),
                            application_name: name.into(),
                            mode: match mode {
                                SharingMode::Members => 1,
                                SharingMode::All => 2,
                            },
                        },
                    ))
                    .await?;
                self.refresh().await
            })
            .await
    }
    /// Revokes access to a shared named Application and refreshes status.
    pub async fn unshare_port(&self, name: &str) -> Result<()> {
        self.client
            .operation(self.client.default_timeout(), async {
                crate::spec::validate_label(name)?;
                self.client
                    .rpc(self.client.main_service().unshare_workspace_port(
                        proto::UnshareWorkspacePortRequest {
                            workspace_ref: Some(self.object()?),
                            application_name: name.into(),
                        },
                    ))
                    .await?;
                self.refresh().await
            })
            .await
    }
    /// Watches this Workspace. Dropping the stream cancels the subscription.
    pub async fn watch(
        &self,
        options: StreamOptions,
    ) -> Result<EventStream<proto::WatchWorkspaceResponse>> {
        self.client
            .workspaces()
            .watch(Some(self.reference()), options)
            .await
    }
    /// Streams initialization logs as typed protocol messages.
    pub async fn logs(
        &self,
        options: StreamOptions,
    ) -> Result<EventStream<proto::ListenLogResponse>> {
        let deadline = options.deadline()?;
        let request = proto::ListenLogRequest {
            workspace_ref: Some(self.object()?),
        };
        let rpc = self
            .client
            .operation_until(deadline, async {
                self.client
                    .rpc(self.client.workspace_service().listen_log(request))
                    .await
            })
            .await?;
        Ok(crate::stream::rpc_stream(
            self.client.clone(),
            rpc,
            deadline,
        ))
    }
    /// Starts a command builder. Await it directly to collect output or call `stream`.
    pub fn exec(&self, command: impl Into<crate::Command>) -> crate::ExecBuilder {
        crate::ExecBuilder::new(self.clone(), command.into())
    }
    /// File operations using the Workspace's POSIX shell.
    pub fn files(&self) -> crate::Files {
        crate::Files::new(self.clone())
    }
    /// Persistent interactive PTY terminals.
    pub fn terminals(&self) -> crate::Terminals {
        crate::Terminals::new(self.clone())
    }
}
