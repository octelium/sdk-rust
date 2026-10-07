use crate::{Error, Reference, Result, meta, proto};
use proto::workspace::spec::{self, runtime};
use std::collections::HashSet;

/// An environment value supplied literally or resolved from a Space Secret.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum EnvValue {
    /// Literal environment content.
    Literal(String),
    /// Name of a write-only Secret in the Workspace's Space.
    Secret(String),
}
impl From<&str> for EnvValue {
    fn from(v: &str) -> Self {
        Self::Literal(v.into())
    }
}
impl From<String> for EnvValue {
    fn from(v: String) -> Self {
        Self::Literal(v)
    }
}
impl EnvValue {
    /// Resolves a Secret by name at workspace initialization time.
    pub fn secret(name: impl Into<String>) -> Self {
        Self::Secret(name.into())
    }
}
/// Compute allocations. Unset dimensions inherit platform defaults.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct Resources {
    /// CPU allocation in millicores; one core is 1000.
    pub cpu_millicores: Option<u32>,
    /// Memory allocation in megabytes.
    pub memory_mb: Option<u32>,
    /// Private persistent storage allocation in megabytes.
    pub storage_mb: Option<u32>,
}
impl Resources {
    /// Starts with inherited allocations.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets CPU in millicores. Zero is rejected when building the resource.
    pub fn cpu_millicores(mut self, v: u32) -> Self {
        self.cpu_millicores = Some(v);
        self
    }
    /// Sets memory in megabytes.
    pub fn memory_mb(mut self, v: u32) -> Self {
        self.memory_mb = Some(v);
        self
    }
    /// Sets storage in megabytes.
    pub fn storage_mb(mut self, v: u32) -> Self {
        self.storage_mb = Some(v);
        self
    }
    pub(crate) fn limit(self) -> Result<spec::Limit> {
        if [self.cpu_millicores, self.memory_mb, self.storage_mb].contains(&Some(0)) {
            return Err(Error::InvalidArgument(
                "resource allocations must be positive".into(),
            ));
        }
        Ok(spec::Limit {
            cpu: self
                .cpu_millicores
                .map(|millicores| spec::limit::Cpu { millicores }),
            memory: self
                .memory_mb
                .map(|megabytes| spec::limit::Memory { megabytes }),
            storage: self
                .storage_mb
                .map(|megabytes| spec::limit::Storage { megabytes }),
        })
    }
}
/// A named TCP application exposed through the Cordium HTTPS proxy.
#[derive(Clone, Debug)]
pub struct Application {
    inner: spec::Application,
}
impl Application {
    /// Declares a named port. Names must be lowercase hostname labels; the port must be nonzero.
    pub fn new(name: impl Into<String>, port: u16) -> Self {
        Self {
            inner: spec::Application {
                name: name.into(),
                port: i32::from(port),
                ..Default::default()
            },
        }
    }
    /// Serves this application at the Workspace root hostname.
    pub fn default_app(mut self) -> Self {
        self.inner.is_default = true;
        self
    }
    /// Sets a display name.
    pub fn display_name(mut self, v: impl Into<String>) -> Self {
        self.inner.display_name = v.into();
        self
    }
}
/// When a lifecycle task runs.
#[derive(Clone, Copy, Debug)]
pub enum TaskPhase {
    /// Once for persistent storage, or on each fresh ephemeral run.
    Create,
    /// After every start.
    Start,
    /// Before each graceful stop.
    Stop,
}
/// A lifecycle shell task, configured using consuming builder methods.
#[derive(Clone, Debug)]
pub struct Task {
    inner: runtime::Task,
}
impl Task {
    /// Creates a one-time setup task. Name and command are validated on resource creation.
    pub fn new(name: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            inner: runtime::Task {
                name: name.into(),
                run: command.into(),
                r#type: runtime::task::Type::OnCreate as i32,
                ..Default::default()
            },
        }
    }
    /// Selects the lifecycle phase.
    pub fn phase(mut self, v: TaskPhase) -> Self {
        self.inner.r#type = match v {
            TaskPhase::Create => 1,
            TaskPhase::Start => 2,
            TaskPhase::Stop => 3,
        };
        self
    }
    /// Starts without waiting for completion.
    pub fn background(mut self, v: bool) -> Self {
        self.inner.is_background = v;
        self
    }
    /// Runs as root instead of the Workspace user.
    pub fn as_root(mut self, v: bool) -> Self {
        self.inner.run_as_root = v;
        self
    }
    /// Continues initialization if this task fails. False explicitly aborts initialization.
    pub fn continue_on_failure(mut self, v: bool) -> Self {
        self.inner.on_failure = if v { 2 } else { 1 };
        self
    }
    /// Sets a task working directory.
    pub fn cwd(mut self, v: impl Into<String>) -> Self {
        self.inner.working_dir = v.into();
        self
    }
    /// Adds or replaces a task environment variable.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        self.inner.env_vars.retain(|v| v.key != key);
        self.inner.env_vars.push(runtime::task::EnvVar {
            key,
            value: value.into(),
        });
        self
    }
}
/// Workspace or Template configuration. Options override the matching fields of a base spec.
///
/// Full generated specifications are accepted through [`Self::from_spec`]. Template
/// creation rejects workspace-only fields (source references, applications, ephemeral).
#[derive(Clone, Debug, Default)]
#[must_use = "pass options to a workspace or template operation"]
pub struct WorkspaceOptions {
    pub(crate) spec: proto::workspace::Spec,
    pub(crate) display_name: String,
    pub(crate) template: Option<Reference>,
    pub(crate) snapshot: Option<Reference>,
    pub(crate) git_provider: String,
    resources: Option<Resources>,
}
impl WorkspaceOptions {
    /// Starts with inherited server defaults.
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts from an owned, complete specification, preserving less common fields.
    pub fn from_spec(spec: proto::workspace::Spec) -> Self {
        Self {
            spec,
            ..Self::default()
        }
    }
    /// Sets a human-readable name; the server assigns the actual Workspace name.
    pub fn display_name(mut self, v: impl Into<String>) -> Self {
        self.display_name = v.into();
        self
    }
    /// Creates from a Template. With a snapshot, it defaults to the snapshot's Template and must share its Space.
    pub fn template(mut self, v: impl Into<Reference>) -> Self {
        self.template = Some(v.into());
        self
    }
    /// Restores storage from a snapshot. An ephemeral Workspace restores it on every run.
    pub fn snapshot(mut self, v: impl Into<Reference>) -> Self {
        self.snapshot = Some(v.into());
        self
    }
    /// Pulls an image from a container registry.
    pub fn image(mut self, v: impl Into<String>) -> Self {
        self.spec.image = Some(spec::Image {
            r#type: Some(spec::image::Type::Registry(spec::image::Registry {
                url: v.into(),
                ..Default::default()
            })),
        });
        self
    }
    /// Sets a complete image source, such as a private registry or devcontainer.
    pub fn image_spec(mut self, v: spec::Image) -> Self {
        self.spec.image = Some(v);
        self
    }
    /// Builds an inline Dockerfile.
    pub fn dockerfile(mut self, v: impl Into<String>) -> Self {
        self.spec.image = Some(spec::Image {
            r#type: Some(spec::image::Type::Dockerfile(spec::image::Dockerfile {
                r#type: Some(spec::image::dockerfile::Type::Inline(v.into())),
            })),
        });
        self
    }
    /// Clones a primary HTTPS git repository into `/workspace/repo`.
    pub fn repository(mut self, v: impl Into<String>) -> Self {
        self.spec.repository = Some(spec::Repository {
            url: v.into(),
            ..Default::default()
        });
        self
    }
    /// Sets full repository options, including authentication and shallow clones.
    pub fn repository_spec(mut self, v: spec::Repository) -> Self {
        self.spec.repository = Some(v);
        self
    }
    /// Chooses a branch for the primary repository.
    pub fn branch(mut self, v: impl Into<String>) -> Self {
        self.spec
            .repository
            .get_or_insert_default()
            .clone_options
            .get_or_insert_default()
            .branch = v.into();
        self
    }
    /// Adds a secondary repository with an absolute clone path.
    pub fn additional_repository(mut self, v: spec::AdditionalRepository) -> Self {
        self.spec.additional_repositories.push(v);
        self
    }
    /// Adds or replaces a literal or Secret-backed runtime environment variable.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<EnvValue>) -> Self {
        let key = key.into();
        let value = match value.into() {
            EnvValue::Literal(v) => runtime::env_var::Type::Value(v),
            EnvValue::Secret(v) => runtime::env_var::Type::FromSecret(v),
        };
        let vars = &mut self.spec.runtime.get_or_insert_default().env_vars;
        vars.retain(|e| e.key != key);
        vars.push(runtime::EnvVar {
            key,
            r#type: Some(value),
        });
        self
    }
    /// Adds or replaces a `${{ vars.NAME }}` substitution variable.
    pub fn variable(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        self.spec.vars.retain(|v| v.name != name);
        self.spec.vars.push(spec::Var {
            name,
            value: value.into(),
        });
        self
    }
    /// Allocates CPU, memory, and storage dimensions. Unset dimensions retain the base spec.
    pub fn resources(mut self, v: Resources) -> Self {
        self.resources = Some(v);
        self
    }
    /// Discards private storage on stop, requiring fresh initialization on the next start.
    pub fn ephemeral(mut self, v: bool) -> Self {
        self.spec.is_ephemeral = v;
        self
    }
    /// Adds a named exposed port. Application names and default selection must be unique.
    pub fn application(mut self, v: Application) -> Self {
        self.spec.applications.push(v.inner);
        self
    }
    /// Adds a lifecycle task with a unique name.
    pub fn task(mut self, v: Task) -> Self {
        self.spec
            .runtime
            .get_or_insert_default()
            .tasks
            .push(v.inner);
        self
    }
    /// Mounts a Space-owned Volume at an absolute canonical path.
    pub fn mount(
        mut self,
        volume: impl Into<Reference>,
        path: impl Into<String>,
        read_only: bool,
    ) -> Self {
        // Keep the reference fields; semantic validation happens before any RPC.
        let r = match volume.into() {
            Reference::Name(name) => meta::ObjectReference {
                name,
                ..Default::default()
            },
            Reference::Uid(uid) => meta::ObjectReference {
                uid,
                ..Default::default()
            },
        };
        self.spec
            .runtime
            .get_or_insert_default()
            .volume_mounts
            .push(runtime::VolumeMount {
                volume_ref: Some(r),
                mount_path: path.into(),
                read_only,
            });
        self
    }
    /// Disables the inactivity timeout.
    pub fn disable_timeout(mut self, v: bool) -> Self {
        self.spec
            .runtime
            .get_or_insert_default()
            .timeout
            .get_or_insert_default()
            .mode = if v { 2 } else { 1 };
        self
    }
    /// Stops automatically after foreground lifecycle tasks complete.
    pub fn auto_stop(mut self, v: bool) -> Self {
        self.spec.runtime.get_or_insert_default().auto_stop = v;
        self
    }
    /// Associates a Template with a GitProvider. Only Template operations accept this field.
    pub fn git_provider(mut self, v: impl Into<String>) -> Self {
        self.git_provider = v.into();
        self
    }
    /// Validates and returns the resulting full Workspace specification.
    pub fn build_spec(mut self) -> Result<proto::workspace::Spec> {
        self.validate()?;
        Ok(self.spec)
    }
    pub(crate) fn validate(&mut self) -> Result<()> {
        for r in [&self.template, &self.snapshot].into_iter().flatten() {
            r.object()?;
        }
        if let Some(r) = self.resources.take() {
            let new = r.limit()?;
            let old = self.spec.limit.get_or_insert_default();
            if new.cpu.is_some() {
                old.cpu = new.cpu;
            }
            if new.memory.is_some() {
                old.memory = new.memory;
            }
            if new.storage.is_some() {
                old.storage = new.storage;
            }
        }
        if let Some(image) = &self.spec.image {
            match &image.r#type {
                Some(spec::image::Type::Registry(r)) => {
                    crate::error::nonempty(&r.url, "registry image")?
                }
                Some(spec::image::Type::Dockerfile(r)) if r.r#type.is_none() => {
                    return Err(Error::InvalidArgument(
                        "Dockerfile source is missing".into(),
                    ));
                }
                None => return Err(Error::InvalidArgument("image source is missing".into())),
                _ => {}
            }
        }
        if let Some(repo) = &self.spec.repository
            && !repo.url.starts_with("https://")
        {
            return Err(Error::InvalidArgument(
                "repository URL must use HTTPS".into(),
            ));
        }
        let mut apps = HashSet::new();
        let mut defaults = 0;
        for app in &self.spec.applications {
            validate_label(&app.name)?;
            if !(1..=65535).contains(&app.port) || !apps.insert(&app.name) {
                return Err(Error::InvalidArgument(
                    "application ports must be positive and names unique".into(),
                ));
            }
            defaults += u8::from(app.is_default);
        }
        if defaults > 1 {
            return Err(Error::InvalidArgument(
                "only one application may be the default".into(),
            ));
        }
        for var in &self.spec.vars {
            crate::error::nonempty(&var.name, "variable name")?;
        }
        if let Some(rt) = &self.spec.runtime {
            for e in &rt.env_vars {
                validate_env_key(&e.key)?;
                match &e.r#type {
                    Some(runtime::env_var::Type::FromSecret(v)) => {
                        crate::error::nonempty(v, "Secret name")?
                    }
                    Some(runtime::env_var::Type::Value(v)) if v.is_empty() => {
                        return Err(Error::InvalidArgument(format!(
                            "environment variable {} has an empty value",
                            e.key
                        )));
                    }
                    _ => {}
                }
            }
            let mut tasks = HashSet::new();
            for task in &rt.tasks {
                crate::error::nonempty(&task.name, "task name")?;
                crate::error::nonempty(&task.run, "task command")?;
                if !tasks.insert(&task.name) || !(1..=3).contains(&task.r#type) {
                    return Err(Error::InvalidArgument(
                        "task names must be unique and lifecycle phase set".into(),
                    ));
                }
                for e in &task.env_vars {
                    validate_env_key(&e.key)?;
                    if e.value.is_empty() {
                        return Err(Error::InvalidArgument(format!(
                            "task environment variable {} has an empty value",
                            e.key
                        )));
                    }
                }
            }
            let mut mounts = Vec::<&str>::new();
            for mount in &rt.volume_mounts {
                let path = mount.mount_path.as_str();
                if !path.starts_with('/')
                    || path == "/"
                    || path.ends_with('/')
                    || path
                        .split('/')
                        .skip(1)
                        .any(|s| s.is_empty() || s == "." || s == "..")
                    || path.contains('\0')
                {
                    return Err(Error::InvalidArgument(
                        "mount paths must be absolute and canonical, excluding root".into(),
                    ));
                }
                let r = mount.volume_ref.as_ref().ok_or_else(|| {
                    Error::InvalidArgument("mount volume reference is missing".into())
                })?;
                if r.name.is_empty() == r.uid.is_empty() {
                    return Err(Error::InvalidArgument(
                        "mount needs exactly one volume name or UID".into(),
                    ));
                }
                for prior in &mounts {
                    if path == *prior
                        || path.starts_with(&format!("{prior}/"))
                        || prior.starts_with(&format!("{path}/"))
                    {
                        return Err(Error::InvalidArgument("volume mount paths overlap".into()));
                    }
                }
                mounts.push(path);
            }
        }
        Ok(())
    }
    pub(crate) fn into_template(mut self) -> Result<proto::template::Spec> {
        self.validate()?;
        if self.template.is_some()
            || self.snapshot.is_some()
            || self.spec.is_ephemeral
            || !self.spec.applications.is_empty()
        {
            return Err(Error::InvalidArgument(
                "Template does not accept workspace source, applications, or ephemeral storage"
                    .into(),
            ));
        }
        Ok(proto::template::Spec {
            image: self.spec.image,
            runtime: self.spec.runtime,
            repository: self.spec.repository,
            additional_repositories: self.spec.additional_repositories,
            limit: self.spec.limit,
            vars: self.spec.vars,
            git_provider: self.git_provider,
        })
    }
}
pub(crate) fn validate_label(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 63
        || s.starts_with('-')
        || s.ends_with('-')
        || !s
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(Error::InvalidArgument(
            "application name must be a lowercase hostname label".into(),
        ));
    }
    Ok(())
}
pub(crate) fn validate_env_key(s: &str) -> Result<()> {
    if s.is_empty() || s.contains(['=', '\0']) {
        return Err(Error::InvalidArgument(
            "environment keys must be nonempty and contain neither '=' nor NUL".into(),
        ));
    }
    Ok(())
}
