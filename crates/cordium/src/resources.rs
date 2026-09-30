use crate::error::nonempty;
use crate::{
    Client, Error, EventStream, ListFilter, ListOptions, Page, Reference, Result, WaitOptions,
    WorkspaceOptions, meta, proto,
};

fn metadata(name: String) -> meta::Metadata {
    meta::Metadata {
        name,
        ..Default::default()
    }
}
fn valid_resource(md: Option<&meta::Metadata>) -> Result<()> {
    let md = md.ok_or_else(|| Error::InvalidArgument("resource metadata is required".into()))?;
    if md.name.is_empty() && md.uid.is_empty() {
        return Err(Error::InvalidArgument(
            "resource needs a name or UID".into(),
        ));
    }
    Ok(())
}

/// Space namespaces, limits, runtime configuration, and membership.
#[derive(Clone, Debug)]
pub struct Spaces {
    client: Client,
}
impl Spaces {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::Space> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_space(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_space(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::Space) -> Result<proto::Space> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_space(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(&self, reference: impl Into<Reference>, edit: F) -> Result<proto::Space>
    where
        F: FnOnce(&mut proto::Space) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Space>> {
        options.unfiltered()?;
        let request = proto::ListSpaceOptions {
            common: Some(options.common()),
            mode: match options.space_mode {
                crate::SpaceMode::CreatedBy => 1,
                crate::SpaceMode::Member => 2,
            },
            ..Default::default()
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_space(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Space> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Reusable workspace specifications and asynchronous pre-builds.
#[derive(Clone, Debug)]
pub struct Templates {
    client: Client,
}
impl Templates {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::Template> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_template(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_template(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::Template) -> Result<proto::Template> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_template(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(
        &self,
        reference: impl Into<Reference>,
        edit: F,
    ) -> Result<proto::Template>
    where
        F: FnOnce(&mut proto::Template) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Template>> {
        let request = proto::ListTemplateOptions {
            common: Some(options.common()),
            space_ref: options.space()?,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_template(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Template> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Point-in-time snapshots of private persistent workspace storage.
#[derive(Clone, Debug)]
pub struct Snapshots {
    client: Client,
}
impl Snapshots {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::WorkspaceSnapshot> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_workspace_snapshot(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_workspace_snapshot(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::WorkspaceSnapshot>> {
        let filter = match &options.filter {
            None => None,
            Some(ListFilter::Space(r)) => Some(
                proto::list_workspace_snapshot_options::Filter::SpaceRef(r.object()?),
            ),
            Some(ListFilter::Workspace(r)) => {
                Some(proto::list_workspace_snapshot_options::Filter::WorkspaceRef(r.object()?))
            }
            _ => {
                return Err(Error::InvalidArgument(
                    "snapshots support Space or Workspace filters".into(),
                ));
            }
        };
        let request = proto::ListWorkspaceSnapshotOptions {
            common: Some(options.common()),
            filter,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_workspace_snapshot(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::WorkspaceSnapshot> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Space-owned persistent volumes that outlive Workspaces.
#[derive(Clone, Debug)]
pub struct Volumes {
    client: Client,
}
impl Volumes {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::Volume> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_volume(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_volume(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::Volume) -> Result<proto::Volume> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_volume(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(&self, reference: impl Into<Reference>, edit: F) -> Result<proto::Volume>
    where
        F: FnOnce(&mut proto::Volume) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Volume>> {
        let request = proto::ListVolumeOptions {
            common: Some(options.common()),
            space_ref: options.space()?,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_volume(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Volume> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Write-only Space secrets. Reads return metadata without sensitive content.
#[derive(Clone, Debug)]
pub struct Secrets {
    client: Client,
}
impl Secrets {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::Secret> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_secret(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_secret(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Secret>> {
        let request = proto::ListSecretOptions {
            common: Some(options.common()),
            space_ref: options.space()?,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_secret(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Secret> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Personal write-only secrets and cluster-generated SSH key pairs.
#[derive(Clone, Debug)]
pub struct UserSecrets {
    client: Client,
}
impl UserSecrets {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::UserSecret> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_user_secret(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_user_secret(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::UserSecret) -> Result<proto::UserSecret> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_user_secret(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(
        &self,
        reference: impl Into<Reference>,
        edit: F,
    ) -> Result<proto::UserSecret>
    where
        F: FnOnce(&mut proto::UserSecret) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::UserSecret>> {
        options.unfiltered()?;
        let request = proto::ListUserSecretOptions {
            common: Some(options.common()),
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_user_secret(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::UserSecret> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Git hosting OAuth2 provider configuration.
#[derive(Clone, Debug)]
pub struct GitProviders {
    client: Client,
}
impl GitProviders {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::GitProvider> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_git_provider(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_git_provider(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::GitProvider) -> Result<proto::GitProvider> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_git_provider(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(
        &self,
        reference: impl Into<Reference>,
        edit: F,
    ) -> Result<proto::GitProvider>
    where
        F: FnOnce(&mut proto::GitProvider) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::GitProvider>> {
        let request = proto::ListGitProviderOptions {
            common: Some(options.common()),
            space_ref: options.space()?,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_git_provider(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::GitProvider> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Space users and access roles.
#[derive(Clone, Debug)]
pub struct Memberships {
    client: Client,
}
impl Memberships {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves a resource by name or UID. Secret values are never returned.
    pub async fn get(&self, reference: impl Into<Reference>) -> Result<proto::Membership> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_membership(reference.into().get()?),
            )
            .await
    }
    /// Deletes the resource. Server-side ownership and usage restrictions apply.
    pub async fn delete(&self, reference: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .delete_membership(reference.into().delete()?),
            )
            .await?;
        Ok(())
    }
    /// Replaces the resource using a complete owned protobuf message; no retries are made.
    pub async fn update(&self, resource: proto::Membership) -> Result<proto::Membership> {
        valid_resource(resource.metadata.as_ref())?;
        self.client
            .rpc(self.client.main_service().update_membership(resource))
            .await
    }
    /// Fetches, edits and updates exactly once, preserving fields outside the callback.
    /// A callback error aborts before update. Concurrent edits may require conflict handling.
    pub async fn modify<F>(
        &self,
        reference: impl Into<Reference>,
        edit: F,
    ) -> Result<proto::Membership>
    where
        F: FnOnce(&mut proto::Membership) -> Result<()> + Send,
    {
        let reference = reference.into();
        self.client
            .operation(self.client.default_timeout(), async {
                let mut resource = self.get(reference).await?;
                edit(&mut resource)?;
                self.update(resource).await
            })
            .await
    }
    /// Lists one page. Pass `ListOptions::default()` for server defaults.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Membership>> {
        let request = proto::ListMembershipOptions {
            common: Some(options.common()),
            space_ref: options.space()?,
        };
        let page = self
            .client
            .rpc(self.client.main_service().list_membership(request))
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily streams all pages, stopping on the first error or when dropped.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Membership> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}

/// Options for Space creation. All complete protobuf spec fields remain available.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SpaceOptions {
    /// Qualifies an unqualified name with `.cordium` to create a shared organization Space.
    pub organization: bool,
    /// Human-readable display name.
    pub display_name: String,
    /// Defaults, limits, tasks, environment and SSH configuration.
    pub spec: proto::space::Spec,
}
impl SpaceOptions {
    /// Creates a shared organization Space.
    pub fn organization(mut self) -> Self {
        self.organization = true;
        self
    }
    /// Sets the display name.
    pub fn display_name(mut self, v: impl Into<String>) -> Self {
        self.display_name = v.into();
        self
    }
    /// Sets the complete Space specification.
    pub fn spec(mut self, v: proto::space::Spec) -> Self {
        self.spec = v;
        self
    }
}
impl Spaces {
    /// Creates a Space. A default Template and owner Membership are created by the cluster.
    pub async fn create(
        &self,
        name: impl Into<String>,
        options: SpaceOptions,
    ) -> Result<proto::Space> {
        let mut name = name.into();
        nonempty(&name, "Space name")?;
        if options.organization && !name.contains('.') {
            name.push_str(".cordium");
        }
        let mut md = metadata(name);
        md.display_name = options.display_name;
        self.client
            .rpc(self.client.main_service().create_space(proto::Space {
                metadata: Some(md),
                spec: Some(options.spec),
                ..Default::default()
            }))
            .await
    }
    /// Leaves a Space. A Space creator cannot leave their own Space.
    pub async fn leave(&self, space: impl Into<Reference>) -> Result<()> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .leave_space(proto::LeaveSpaceRequest {
                        space_ref: Some(space.into().object()?),
                    }),
            )
            .await?;
        Ok(())
    }
}
impl Templates {
    /// Creates a Template using the same image, runtime, repository, and resource builders as Workspaces.
    /// Workspace-only applications, ephemeral storage, and source references are rejected.
    pub async fn create(
        &self,
        name: impl Into<String>,
        options: WorkspaceOptions,
    ) -> Result<proto::Template> {
        let name = name.into();
        nonempty(&name, "Template name")?;
        let mut md = metadata(name);
        md.display_name = options.display_name.clone();
        let spec = options.into_template()?;
        self.client
            .rpc(self.client.main_service().create_template(proto::Template {
                metadata: Some(md),
                spec: Some(spec),
                ..Default::default()
            }))
            .await
    }
    /// Starts an asynchronous pre-build. Empty tags select `latest`; a new build cancels the previous one.
    pub async fn build(
        &self,
        template: impl Into<Reference>,
        tags: Vec<String>,
    ) -> Result<proto::Template> {
        for t in &tags {
            nonempty(t, "build tag")?;
        }
        self.client
            .rpc(
                self.client
                    .main_service()
                    .build_template(proto::BuildTemplateRequest {
                        template_ref: Some(template.into().object()?),
                        tags,
                    }),
            )
            .await
    }
    /// Cancels the running pre-build, if any.
    pub async fn cancel_build(&self, template: impl Into<Reference>) -> Result<proto::Template> {
        self.client
            .rpc(self.client.main_service().cancel_build_template(
                proto::CancelBuildTemplateRequest {
                    template_ref: Some(template.into().object()?),
                },
            ))
            .await
    }
    /// Follows an explicit build ID, so an older successful build cannot satisfy this wait.
    pub async fn wait_for_build(
        &self,
        template: impl Into<Reference>,
        build_id: &str,
        options: WaitOptions,
    ) -> Result<proto::template::status::build_info::Build> {
        nonempty(build_id, "build ID")?;
        let options = options.validate()?;
        let template = template.into();
        template.object()?;
        self.client
            .operation(options.timeout, async {
                loop {
                    let resource = self.get(&template).await?;
                    let info = resource
                        .status
                        .and_then(|s| s.build_info)
                        .unwrap_or_default();
                    if let Some(build) = info.builds.into_iter().find(|b| b.id == build_id) {
                        if build.is_canceled || build.state == 3 || build.failure.is_some() {
                            return Err(Error::ResourceFailed {
                                kind: "template build",
                                message: build
                                    .failure
                                    .map(|f| f.message)
                                    .unwrap_or_else(|| "build canceled or failed".into()),
                            });
                        }
                        if build.state == 2 {
                            return Ok(build);
                        }
                    }
                    tokio::time::sleep(options.poll_interval).await;
                }
            })
            .await
    }
}
impl Snapshots {
    /// Snapshots a Workspace without stopping it. Wait for READY before restoring it.
    pub async fn create(
        &self,
        name: impl Into<String>,
        workspace: impl Into<Reference>,
    ) -> Result<proto::WorkspaceSnapshot> {
        let name = name.into();
        nonempty(&name, "snapshot name")?;
        self.client
            .rpc(
                self.client
                    .main_service()
                    .create_workspace_snapshot(proto::WorkspaceSnapshot {
                        metadata: Some(metadata(name)),
                        status: Some(proto::workspace_snapshot::Status {
                            workspace_ref: Some(workspace.into().object()?),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
            )
            .await
    }
    /// Waits until a snapshot is restorable, failing early if the storage backend reports failure.
    pub async fn wait_ready(
        &self,
        reference: impl Into<Reference>,
        options: WaitOptions,
    ) -> Result<proto::WorkspaceSnapshot> {
        let options = options.validate()?;
        let reference = reference.into();
        reference.object()?;
        self.client
            .operation(options.timeout, async {
                loop {
                    let resource = self.get(&reference).await?;
                    let status = resource.status.clone().unwrap_or_default();
                    if status.state == 2 {
                        return Ok(resource);
                    }
                    if status.state == 3 || status.failure.is_some() {
                        return Err(Error::ResourceFailed {
                            kind: "snapshot",
                            message: status
                                .failure
                                .map(|f| f.message)
                                .unwrap_or_else(|| "snapshot failed".into()),
                        });
                    }
                    tokio::time::sleep(options.poll_interval).await;
                }
            })
            .await
    }
}
/// Capacity, concurrency and placement options for a persistent Volume.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct VolumeOptions {
    /// Capacity in megabytes; `None` uses the cluster default.
    pub size_mb: Option<u32>,
    /// Allows concurrent mounts by several Workspaces; requires a shared storage backend.
    pub shared: bool,
    /// Hosting Region; mounted Workspaces must use the same Region.
    pub region: Option<Reference>,
}
impl VolumeOptions {
    /// Sets the requested capacity in megabytes.
    pub fn size_mb(mut self, v: u32) -> Self {
        self.size_mb = Some(v);
        self
    }
    /// Enables multi-writer storage.
    pub fn shared(mut self, v: bool) -> Self {
        self.shared = v;
        self
    }
    /// Sets the hosting Region.
    pub fn region(mut self, v: impl Into<Reference>) -> Self {
        self.region = Some(v.into());
        self
    }
}
impl Volumes {
    /// Creates a Volume. Provisioning may wait for the first Workspace mount.
    pub async fn create(
        &self,
        name: impl Into<String>,
        options: VolumeOptions,
    ) -> Result<proto::Volume> {
        let name = name.into();
        nonempty(&name, "Volume name")?;
        if options.size_mb == Some(0) {
            return Err(Error::InvalidArgument(
                "volume size must be positive".into(),
            ));
        }
        self.client
            .rpc(
                self.client.main_service().create_volume(proto::Volume {
                    metadata: Some(metadata(name)),
                    spec: Some(proto::volume::Spec {
                        size: options
                            .size_mb
                            .map(|megabytes| proto::volume::spec::Size { megabytes }),
                        access_mode: if options.shared { 2 } else { 1 },
                    }),
                    status: Some(proto::volume::Status {
                        region_ref: options.region.as_ref().map(Reference::object).transpose()?,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await
    }
    /// Grows capacity without shrinking requested or provisioned storage. No update is sent for a no-op.
    pub async fn grow(
        &self,
        reference: impl Into<Reference>,
        size_mb: u32,
    ) -> Result<proto::Volume> {
        self.client
            .operation(self.client.default_timeout(), async {
                if size_mb == 0 {
                    return Err(Error::InvalidArgument(
                        "volume size must be positive".into(),
                    ));
                }
                let mut r = self.get(reference).await?;
                let requested = r
                    .spec
                    .as_ref()
                    .and_then(|s| s.size)
                    .map(|s| s.megabytes)
                    .unwrap_or(0);
                let actual = r
                    .status
                    .as_ref()
                    .and_then(|s| s.capacity)
                    .map(|s| s.megabytes)
                    .unwrap_or(0);
                if size_mb < requested.max(actual) {
                    return Err(Error::InvalidArgument("volumes cannot shrink".into()));
                }
                if size_mb == requested {
                    return Ok(r);
                }
                r.spec.get_or_insert_default().size =
                    Some(proto::volume::spec::Size { megabytes: size_mb });
                self.update(r).await
            })
            .await
    }
    /// Waits for provisioned storage, or reports a storage failure. Pending volumes may need a mount first.
    pub async fn wait_ready(
        &self,
        reference: impl Into<Reference>,
        options: WaitOptions,
    ) -> Result<proto::Volume> {
        let options = options.validate()?;
        let reference = reference.into();
        reference.object()?;
        self.client
            .operation(options.timeout, async {
                loop {
                    let resource = self.get(&reference).await?;
                    let status = resource.status.clone().unwrap_or_default();
                    if status.state == 2 {
                        return Ok(resource);
                    }
                    if status.state == 3 || status.failure.is_some() {
                        return Err(Error::ResourceFailed {
                            kind: "volume",
                            message: status
                                .failure
                                .map(|f| f.message)
                                .unwrap_or_else(|| "volume failed".into()),
                        });
                    }
                    tokio::time::sleep(options.poll_interval).await;
                }
            })
            .await
    }
}
impl Secrets {
    /// Creates a write-only text Secret. A name can be qualified with its Space.
    pub async fn create_text(
        &self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<proto::Secret> {
        self.create_data(name, proto::secret::data::Type::Value(value.into()))
            .await
    }
    /// Creates a write-only binary Secret without UTF-8 conversion.
    pub async fn create_bytes(
        &self,
        name: impl Into<String>,
        value: impl Into<crate::Bytes>,
    ) -> Result<proto::Secret> {
        self.create_data(name, proto::secret::data::Type::ValueBytes(value.into()))
            .await
    }
    /// Creates structured Secret attributes from a JSON object; lossy integer conversion is rejected.
    pub async fn create_json(
        &self,
        name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<proto::Secret> {
        self.create_data(name, proto::secret::data::Type::Attrs(json_struct(value)?))
            .await
    }
    async fn create_data(
        &self,
        name: impl Into<String>,
        value: proto::secret::data::Type,
    ) -> Result<proto::Secret> {
        let name = name.into();
        nonempty(&name, "Secret name")?;
        self.client
            .rpc(self.client.main_service().create_secret(proto::Secret {
                metadata: Some(metadata(name)),
                data: Some(proto::secret::Data {
                    r#type: Some(value),
                }),
                ..Default::default()
            }))
            .await
    }
}
impl UserSecrets {
    /// Creates a personal write-only text Secret.
    pub async fn create_text(
        &self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<proto::UserSecret> {
        self.create_data(name, proto::user_secret::data::Type::Value(value.into()))
            .await
    }
    /// Creates a personal write-only binary Secret.
    pub async fn create_bytes(
        &self,
        name: impl Into<String>,
        value: impl Into<crate::Bytes>,
    ) -> Result<proto::UserSecret> {
        self.create_data(
            name,
            proto::user_secret::data::Type::ValueBytes(value.into()),
        )
        .await
    }
    /// Creates structured personal Secret attributes from a JSON object.
    pub async fn create_json(
        &self,
        name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<proto::UserSecret> {
        self.create_data(
            name,
            proto::user_secret::data::Type::Attrs(json_struct(value)?),
        )
        .await
    }
    async fn create_data(
        &self,
        name: impl Into<String>,
        value: proto::user_secret::data::Type,
    ) -> Result<proto::UserSecret> {
        let name = name.into();
        nonempty(&name, "UserSecret name")?;
        self.client
            .rpc(
                self.client
                    .main_service()
                    .create_user_secret(proto::UserSecret {
                        metadata: Some(metadata(name)),
                        data: Some(proto::user_secret::Data {
                            r#type: Some(value),
                        }),
                        ..Default::default()
                    }),
            )
            .await
    }
    /// Asks the cluster to generate an SSH key pair. Only the public key is returned in status.
    pub async fn create_ssh_key(&self, name: impl Into<String>) -> Result<proto::UserSecret> {
        let name = name.into();
        nonempty(&name, "UserSecret name")?;
        self.client
            .rpc(
                self.client
                    .main_service()
                    .create_user_secret(proto::UserSecret {
                        metadata: Some(metadata(name)),
                        spec: Some(proto::user_secret::Spec { r#type: 1 }),
                        ..Default::default()
                    }),
            )
            .await
    }
    /// Replaces the value of an existing personal Secret without changing its other fields.
    pub async fn set_text(
        &self,
        reference: impl Into<Reference>,
        value: impl Into<String>,
    ) -> Result<proto::UserSecret> {
        let value = value.into();
        self.modify(reference, move |r| {
            r.data = Some(proto::user_secret::Data {
                r#type: Some(proto::user_secret::data::Type::Value(value)),
            });
            Ok(())
        })
        .await
    }
}
/// Target User of a new Space membership.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Member {
    /// An existing cluster User's email address.
    Email(String),
    /// An existing cluster User's name or UID.
    User(Reference),
}
/// Generated Space membership role enum.
pub use proto::membership::spec::Role;
impl Memberships {
    /// Adds a User to an organization Space. The server enforces role-granting permissions.
    pub async fn add(
        &self,
        space: impl Into<Reference>,
        member: Member,
        role: Role,
    ) -> Result<proto::Membership> {
        if role == Role::Unknown {
            return Err(Error::InvalidArgument("membership role must be set".into()));
        }
        let user_type = match member {
            Member::Email(v) => {
                nonempty(&v, "member email")?;
                proto::create_membership_request::UserType::Email(v)
            }
            Member::User(v) => proto::create_membership_request::UserType::UserRef(v.object()?),
        };
        self.client
            .rpc(
                self.client
                    .main_service()
                    .create_membership(proto::CreateMembershipRequest {
                        space_ref: Some(space.into().object()?),
                        role: role as i32,
                        user_type: Some(user_type),
                    }),
            )
            .await
    }
    /// Returns the authenticated User's membership in a Space.
    pub async fn mine(&self, space: impl Into<Reference>) -> Result<proto::Membership> {
        self.client
            .rpc(self.client.main_service().get_space_membership(
                proto::GetSpaceMembershipRequest {
                    space_ref: Some(space.into().object()?),
                },
            ))
            .await
    }
    /// Changes a membership role, preserving the full resource.
    pub async fn set_role(
        &self,
        reference: impl Into<Reference>,
        role: Role,
    ) -> Result<proto::Membership> {
        if role == Role::Unknown {
            return Err(Error::InvalidArgument("membership role must be set".into()));
        }
        self.modify(reference, move |r| {
            r.spec.get_or_insert_default().role = role as i32;
            Ok(())
        })
        .await
    }
}
impl GitProviders {
    /// Creates a provider from its complete specification. Client secrets reference Space Secrets.
    pub async fn create(
        &self,
        name: impl Into<String>,
        spec: proto::git_provider::Spec,
    ) -> Result<proto::GitProvider> {
        let name = name.into();
        nonempty(&name, "GitProvider name")?;
        if spec.r#type.is_none() {
            return Err(Error::InvalidArgument(
                "git provider type must be set".into(),
            ));
        }
        self.client
            .rpc(
                self.client
                    .main_service()
                    .create_git_provider(proto::GitProvider {
                        metadata: Some(metadata(name)),
                        spec: Some(spec),
                        ..Default::default()
                    }),
            )
            .await
    }
    /// Creates a Github OAuth provider using a client secret stored in a Space Secret.
    pub async fn create_github(
        &self,
        name: impl Into<String>,
        client_id: impl Into<String>,
        secret_name: impl Into<String>,
        scopes: Vec<String>,
    ) -> Result<proto::GitProvider> {
        let client_id = client_id.into();
        let secret_name = secret_name.into();
        nonempty(&client_id, "OAuth client ID")?;
        nonempty(&secret_name, "OAuth client Secret name")?;
        self.create(
            name,
            proto::git_provider::Spec {
                r#type: Some(proto::git_provider::spec::Type::Github(
                    proto::git_provider::spec::Github {
                        client_id,
                        client_secret: Some(proto::git_provider::spec::github::ClientSecret {
                            r#type: Some(
                                proto::git_provider::spec::github::client_secret::Type::FromSecret(
                                    secret_name,
                                ),
                            ),
                        }),
                        scopes,
                    },
                )),
            },
        )
        .await
    }
    /// Creates a Gitlab OAuth provider using a client secret stored in a Space Secret.
    pub async fn create_gitlab(
        &self,
        name: impl Into<String>,
        client_id: impl Into<String>,
        secret_name: impl Into<String>,
        scopes: Vec<String>,
    ) -> Result<proto::GitProvider> {
        let client_id = client_id.into();
        let secret_name = secret_name.into();
        nonempty(&client_id, "OAuth client ID")?;
        nonempty(&secret_name, "OAuth client Secret name")?;
        self.create(
            name,
            proto::git_provider::Spec {
                r#type: Some(proto::git_provider::spec::Type::Gitlab(
                    proto::git_provider::spec::Gitlab {
                        client_id,
                        client_secret: Some(proto::git_provider::spec::gitlab::ClientSecret {
                            r#type: Some(
                                proto::git_provider::spec::gitlab::client_secret::Type::FromSecret(
                                    secret_name,
                                ),
                            ),
                        }),
                        scopes,
                    },
                )),
            },
        )
        .await
    }
    /// Creates a generic OAuth2 git provider. Both endpoint URLs must be HTTPS and scopes nonempty.
    pub async fn create_oauth2(
        &self,
        name: impl Into<String>,
        provider: proto::git_provider::spec::OAuth2,
    ) -> Result<proto::GitProvider> {
        nonempty(&provider.client_id, "OAuth client ID")?;
        if !provider.auth_url.starts_with("https://")
            || !provider.token_url.starts_with("https://")
            || provider.scopes.is_empty()
        {
            return Err(Error::InvalidArgument(
                "OAuth2 requires HTTPS endpoints and at least one scope".into(),
            ));
        }
        let secret = provider
            .client_secret
            .as_ref()
            .and_then(|s| s.r#type.as_ref())
            .ok_or_else(|| {
                Error::InvalidArgument("OAuth client Secret reference is required".into())
            })?;
        let proto::git_provider::spec::o_auth2::client_secret::Type::FromSecret(s) = secret;
        nonempty(s, "OAuth client Secret name")?;
        self.create(
            name,
            proto::git_provider::Spec {
                r#type: Some(proto::git_provider::spec::Type::Oauth2(provider)),
            },
        )
        .await
    }
}
/// Available cluster Regions that can host Workspaces.
#[derive(Clone, Debug)]
pub struct Regions {
    client: Client,
}
impl Regions {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Lists a page of available Regions.
    pub async fn list(&self, options: ListOptions) -> Result<Page<proto::Region>> {
        options.unfiltered()?;
        let page = self
            .client
            .rpc(
                self.client
                    .main_service()
                    .list_region(proto::ListRegionOptions {
                        common: Some(options.common()),
                    }),
            )
            .await?;
        Ok(Page::new(page.items, page.list_response_meta))
    }
    /// Lazily iterates all available Regions.
    pub fn all(&self, mut options: ListOptions) -> EventStream<proto::Region> {
        let this = self.clone();
        EventStream::new(
            async_stream::try_stream! {loop{let page=this.list(options.clone()).await?;let empty=page.items.is_empty();for item in page.items{yield item;}if !options.next(page.info,empty)?{break;}}},
        )
    }
}
/// User preferences, dotfiles, and runtime defaults.
#[derive(Clone, Debug)]
pub struct UserConfig {
    client: Client,
}
impl UserConfig {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Fetches the caller's configuration, creating it automatically if necessary.
    pub async fn get(&self) -> Result<proto::UserConfig> {
        self.client
            .rpc(
                self.client
                    .main_service()
                    .get_user_config(proto::GetUserConfigRequest {}),
            )
            .await
    }
    /// Replaces the full UserConfig resource. No retries are made.
    pub async fn update(&self, resource: proto::UserConfig) -> Result<proto::UserConfig> {
        self.client
            .rpc(self.client.main_service().update_user_config(resource))
            .await
    }
    /// Fetches, edits and updates once; callback errors abort before update.
    pub async fn modify<F>(&self, edit: F) -> Result<proto::UserConfig>
    where
        F: FnOnce(&mut proto::UserConfig) -> Result<()> + Send,
    {
        self.client
            .operation(self.client.default_timeout(), async {
                let mut r = self.get().await?;
                edit(&mut r)?;
                self.update(r).await
            })
            .await
    }
    /// Sets the preferred Region name, or clears the preference with an empty string.
    pub async fn set_preferred_region(&self, name: impl Into<String>) -> Result<proto::UserConfig> {
        let name = name.into();
        self.modify(move |r| {
            r.spec.get_or_insert_default().preferred_region = name;
            Ok(())
        })
        .await
    }
    /// Replaces the dotfiles configuration, preserving all other preferences.
    pub async fn set_dotfiles(
        &self,
        config: proto::user_config::spec::Dotfiles,
    ) -> Result<proto::UserConfig> {
        if !config.url.starts_with("https://") {
            return Err(Error::InvalidArgument("dotfiles URL must use HTTPS".into()));
        }
        self.modify(move |r| {
            r.spec.get_or_insert_default().dotfiles = Some(config);
            Ok(())
        })
        .await
    }
}
/// Cluster-wide administrator API.
#[derive(Clone, Debug)]
pub struct Management {
    client: Client,
}
impl Management {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Retrieves the sole cluster configuration resource. Requires administrator authorization.
    pub async fn get_cluster_config(&self) -> Result<proto::ClusterConfig> {
        self.client
            .rpc(
                self.client
                    .management_service()
                    .get_cluster_config(proto::GetClusterConfigRequest {}),
            )
            .await
    }
    /// Replaces the complete cluster configuration. Requires administrator authorization.
    pub async fn update_cluster_config(
        &self,
        r: proto::ClusterConfig,
    ) -> Result<proto::ClusterConfig> {
        self.client
            .rpc(self.client.management_service().update_cluster_config(r))
            .await
    }
    /// Fetches, edits and updates the cluster configuration once.
    pub async fn modify_cluster_config<F>(&self, edit: F) -> Result<proto::ClusterConfig>
    where
        F: FnOnce(&mut proto::ClusterConfig) -> Result<()> + Send,
    {
        self.client
            .operation(self.client.default_timeout(), async {
                let mut r = self.get_cluster_config().await?;
                edit(&mut r)?;
                self.update_cluster_config(r).await
            })
            .await
    }
}
fn json_struct(value: serde_json::Value) -> Result<crate::prost_types::Struct> {
    match value {
        serde_json::Value::Object(map) => Ok(crate::prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| Ok((k, json_value(v)?)))
                .collect::<Result<_>>()?,
        }),
        _ => Err(Error::InvalidArgument(
            "structured Secret data must be a JSON object".into(),
        )),
    }
}
fn json_value(value: serde_json::Value) -> Result<crate::prost_types::Value> {
    use crate::prost_types::{ListValue, Value, value::Kind};
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(v) => Kind::BoolValue(v),
        serde_json::Value::String(v) => Kind::StringValue(v),
        serde_json::Value::Array(v) => Kind::ListValue(ListValue {
            values: v.into_iter().map(json_value).collect::<Result<_>>()?,
        }),
        serde_json::Value::Object(v) => {
            Kind::StructValue(json_struct(serde_json::Value::Object(v))?)
        }
        serde_json::Value::Number(v) => {
            if v.as_u64().is_some_and(|n| n > 9_007_199_254_740_992)
                || v.as_i64()
                    .is_some_and(|n| n.unsigned_abs() > 9_007_199_254_740_992)
            {
                return Err(Error::InvalidArgument(
                    "protobuf JSON cannot represent this integer without loss".into(),
                ));
            }
            let n = v
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or_else(|| Error::InvalidArgument("JSON number must be finite".into()))?;
            Kind::NumberValue(n)
        }
    };
    Ok(Value { kind: Some(kind) })
}
