//! Wire-level integration tests against generated tonic services, plus local shell transfers.
use cordium::{
    Bytes, Client, Command, Error, ExecEvent, ListOptions, Reference, StartOptions, StreamOptions,
    TerminalOptions, WaitOptions, WorkspaceOptions, meta, proto,
};
use futures_core::Stream;
use futures_util::StreamExt;
use octelium_apis::authv1;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tonic::{Request, Response, Status};
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Default)]
struct Data {
    workspaces: HashMap<String, proto::Workspace>,
    spaces: HashMap<String, proto::Space>,
    templates: HashMap<String, proto::Template>,
    snapshots: HashMap<String, proto::WorkspaceSnapshot>,
    volumes: HashMap<String, proto::Volume>,
    secrets: HashMap<String, proto::Secret>,
    user_secrets: HashMap<String, proto::UserSecret>,
    git_providers: HashMap<String, proto::GitProvider>,
    memberships: HashMap<String, proto::Membership>,
    created: Vec<proto::Workspace>,
    starts: Vec<proto::StartWorkspaceRequest>,
    commands: Vec<proto::exec_request::Request>,
    terminals: Vec<String>,
    terminal_writes: Vec<Bytes>,
    terminal_sizes: Vec<(u32, u32)>,
    list_pages: Vec<u32>,
    updates: usize,
    config: proto::UserConfig,
    cluster: proto::ClusterConfig,
}
#[derive(Default)]
struct Shared {
    data: Mutex<Data>,
    auth_calls: AtomicUsize,
    refresh_calls: AtomicUsize,
    auth_delay: AtomicUsize,
    watch_delay: AtomicUsize,
    rpc_delay: AtomicUsize,
    auth_fail: AtomicBool,
    stall: AtomicBool,
    bad_pagination: AtomicBool,
    active_exec: AtomicUsize,
    closed_exec: AtomicUsize,
}
#[derive(Clone)]
struct Service {
    shared: Arc<Shared>,
    dir: std::path::PathBuf,
}
fn check<T>(r: &Request<T>) -> Result<(), Status> {
    match r
        .metadata()
        .get("x-octelium-auth")
        .and_then(|v| v.to_str().ok())
    {
        Some("token" | "session-token" | "refreshed-token") => Ok(()),
        _ => Err(Status::unauthenticated("missing access token")),
    }
}
fn md(name: &str) -> meta::Metadata {
    meta::Metadata {
        name: name.into(),
        uid: format!("uid-{name}"),
        ..Default::default()
    }
}
fn ref_matches(r: &meta::GetOptions, m: Option<&meta::Metadata>) -> bool {
    m.is_some_and(|m| {
        (!r.uid.is_empty() && r.uid == m.uid) || (!r.name.is_empty() && r.name == m.name)
    })
}
fn page<T: Clone>(
    values: impl Iterator<Item = T>,
    common: Option<meta::CommonListOptions>,
) -> (Vec<T>, meta::ListResponseMeta) {
    let c = common.unwrap_or_default();
    let mut all = values.collect::<Vec<_>>();
    let total = all.len() as u32;
    let size = if c.items_per_page == 0 {
        2
    } else {
        c.items_per_page
    };
    let start = (u64::from(c.page) * u64::from(size)) as usize;
    let end = start.saturating_add(size as usize).min(all.len());
    let items = if start < all.len() {
        all.drain(start..end).collect()
    } else {
        vec![]
    };
    (
        items,
        meta::ListResponseMeta {
            page: c.page,
            items_per_page: size,
            total_count: total,
            has_more: end < total as usize,
        },
    )
}
#[tonic::async_trait]
impl proto::main_service_server::MainService for Service {
    async fn create_workspace(
        &self,
        r: Request<proto::Workspace>,
    ) -> Result<Response<proto::Workspace>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let mut ws = r.into_inner();
        d.created.push(ws.clone());
        ws.metadata = Some(md(&format!("ws{}", d.workspaces.len() + 1)));
        ws.status.get_or_insert_default().state = cordium::State::Stopped as i32;
        d.workspaces
            .insert(ws.metadata.as_ref().unwrap().name.clone(), ws.clone());
        Ok(Response::new(ws))
    }
    async fn get_workspace(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Workspace>, Status> {
        check(&r)?;
        tokio::time::sleep(Duration::from_millis(
            self.shared.rpc_delay.load(Ordering::SeqCst) as u64,
        ))
        .await;
        if self.shared.stall.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        let d = self.shared.data.lock().unwrap();
        let ws = d
            .workspaces
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .ok_or_else(|| Status::not_found("Workspace"))?;
        Ok(Response::new(ws.clone()))
    }
    async fn update_workspace(
        &self,
        r: Request<proto::Workspace>,
    ) -> Result<Response<proto::Workspace>, Status> {
        check(&r)?;
        let ws = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.workspaces
            .insert(ws.metadata.as_ref().unwrap().name.clone(), ws.clone());
        Ok(Response::new(ws))
    }
    async fn delete_workspace(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .workspaces
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Workspace"))?;
        d.workspaces.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn start_workspace(
        &self,
        r: Request<proto::StartWorkspaceRequest>,
    ) -> Result<Response<proto::StartWorkspaceResponse>, Status> {
        check(&r)?;
        tokio::time::sleep(Duration::from_millis(
            self.shared.rpc_delay.load(Ordering::SeqCst) as u64,
        ))
        .await;
        let mut d = self.shared.data.lock().unwrap();
        d.starts.push(r.get_ref().clone());
        let ref_ = r.get_ref().workspace_ref.as_ref().unwrap();
        let ws = d
            .workspaces
            .values_mut()
            .find(|w| {
                w.metadata
                    .as_ref()
                    .is_some_and(|m| m.name == ref_.name || m.uid == ref_.uid)
            })
            .ok_or_else(|| Status::not_found("Workspace"))?;
        let status = ws.status.get_or_insert_default();
        status.state = cordium::State::Running as i32;
        status.hostname = "abc.cordium.example.com".into();
        Ok(Response::new(proto::StartWorkspaceResponse {}))
    }
    async fn stop_workspace(
        &self,
        r: Request<proto::StopWorkspaceRequest>,
    ) -> Result<Response<proto::StopWorkspaceResponse>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let ref_ = r.get_ref().workspace_ref.as_ref().unwrap();
        let ws = d
            .workspaces
            .values_mut()
            .find(|w| {
                w.metadata
                    .as_ref()
                    .is_some_and(|m| m.name == ref_.name || m.uid == ref_.uid)
            })
            .ok_or_else(|| Status::not_found("Workspace"))?;
        let status = ws.status.get_or_insert_default();
        status.state = cordium::State::Stopped as i32;
        status.hostname.clear();
        Ok(Response::new(proto::StopWorkspaceResponse {}))
    }
    async fn list_workspace(
        &self,
        r: Request<proto::ListWorkspaceOptions>,
    ) -> Result<Response<proto::WorkspaceList>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let c = r.get_ref().common;
        d.list_pages.push(c.unwrap_or_default().page);
        let mut values = d.workspaces.values().cloned().collect::<Vec<_>>();
        values.sort_by_key(|w| w.metadata.as_ref().unwrap().name.clone());
        let (items, mut info) = page(values.into_iter(), c);
        if self.shared.bad_pagination.load(Ordering::SeqCst) {
            info.page = 0;
            info.has_more = true;
        }
        Ok(Response::new(proto::WorkspaceList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn watch_workspace(
        &self,
        r: Request<proto::WatchWorkspaceRequest>,
    ) -> Result<Response<BoxStream<proto::WatchWorkspaceResponse>>, Status> {
        check(&r)?;
        tokio::time::sleep(Duration::from_millis(
            self.shared.watch_delay.load(Ordering::SeqCst) as u64,
        ))
        .await;
        Ok(Response::new(Box::pin(futures_util::stream::pending())))
    }
    async fn create_space(
        &self,
        r: Request<proto::Space>,
    ) -> Result<Response<proto::Space>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.spaces.insert(name, resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_space(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Space>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .spaces
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("Space"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_space(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .spaces
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Space"))?;
        d.spaces.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_space(
        &self,
        r: Request<proto::ListSpaceOptions>,
    ) -> Result<Response<proto::SpaceList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.spaces.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::SpaceList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_space(
        &self,
        r: Request<proto::Space>,
    ) -> Result<Response<proto::Space>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.spaces.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn create_template(
        &self,
        r: Request<proto::Template>,
    ) -> Result<Response<proto::Template>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.templates.insert(name, resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_template(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Template>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .templates
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("Template"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_template(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .templates
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Template"))?;
        d.templates.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_template(
        &self,
        r: Request<proto::ListTemplateOptions>,
    ) -> Result<Response<proto::TemplateList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.templates.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::TemplateList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_template(
        &self,
        r: Request<proto::Template>,
    ) -> Result<Response<proto::Template>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.templates.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn create_workspace_snapshot(
        &self,
        r: Request<proto::WorkspaceSnapshot>,
    ) -> Result<Response<proto::WorkspaceSnapshot>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.snapshots.insert(name, resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_workspace_snapshot(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::WorkspaceSnapshot>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .snapshots
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("WorkspaceSnapshot"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_workspace_snapshot(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .snapshots
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("WorkspaceSnapshot"))?;
        d.snapshots.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_workspace_snapshot(
        &self,
        r: Request<proto::ListWorkspaceSnapshotOptions>,
    ) -> Result<Response<proto::WorkspaceSnapshotList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.snapshots.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::WorkspaceSnapshotList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn create_volume(
        &self,
        r: Request<proto::Volume>,
    ) -> Result<Response<proto::Volume>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.volumes.insert(name, resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_volume(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Volume>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .volumes
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("Volume"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_volume(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .volumes
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Volume"))?;
        d.volumes.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_volume(
        &self,
        r: Request<proto::ListVolumeOptions>,
    ) -> Result<Response<proto::VolumeList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.volumes.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::VolumeList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_volume(
        &self,
        r: Request<proto::Volume>,
    ) -> Result<Response<proto::Volume>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.volumes.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn create_secret(
        &self,
        r: Request<proto::Secret>,
    ) -> Result<Response<proto::Secret>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.secrets.insert(name, resource.clone());
        resource.data = None;
        Ok(Response::new(resource))
    }
    async fn get_secret(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Secret>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .secrets
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("Secret"))?;
        resource.data = None;
        Ok(Response::new(resource))
    }
    async fn delete_secret(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .secrets
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Secret"))?;
        d.secrets.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_secret(
        &self,
        r: Request<proto::ListSecretOptions>,
    ) -> Result<Response<proto::SecretList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.secrets.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::SecretList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn create_user_secret(
        &self,
        r: Request<proto::UserSecret>,
    ) -> Result<Response<proto::UserSecret>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.user_secrets.insert(name, resource.clone());
        resource.data = None;
        Ok(Response::new(resource))
    }
    async fn get_user_secret(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::UserSecret>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .user_secrets
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("UserSecret"))?;
        resource.data = None;
        Ok(Response::new(resource))
    }
    async fn delete_user_secret(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .user_secrets
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("UserSecret"))?;
        d.user_secrets.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_user_secret(
        &self,
        r: Request<proto::ListUserSecretOptions>,
    ) -> Result<Response<proto::UserSecretList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.user_secrets.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::UserSecretList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_user_secret(
        &self,
        r: Request<proto::UserSecret>,
    ) -> Result<Response<proto::UserSecret>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.user_secrets.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn create_git_provider(
        &self,
        r: Request<proto::GitProvider>,
    ) -> Result<Response<proto::GitProvider>, Status> {
        check(&r)?;
        let mut resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        let name = resource.metadata.as_ref().unwrap().name.clone();
        resource.metadata = Some(md(&name));
        d.git_providers.insert(name, resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_git_provider(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::GitProvider>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .git_providers
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("GitProvider"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_git_provider(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .git_providers
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("GitProvider"))?;
        d.git_providers.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_git_provider(
        &self,
        r: Request<proto::ListGitProviderOptions>,
    ) -> Result<Response<proto::GitProviderList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.git_providers.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::GitProviderList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_git_provider(
        &self,
        r: Request<proto::GitProvider>,
    ) -> Result<Response<proto::GitProvider>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.git_providers.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn get_membership(
        &self,
        r: Request<meta::GetOptions>,
    ) -> Result<Response<proto::Membership>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let mut resource = d
            .memberships
            .values()
            .find(|w| ref_matches(r.get_ref(), w.metadata.as_ref()))
            .cloned()
            .ok_or_else(|| Status::not_found("Membership"))?;
        let _ = &mut resource;
        Ok(Response::new(resource))
    }
    async fn delete_membership(
        &self,
        r: Request<meta::DeleteOptions>,
    ) -> Result<Response<meta::OperationResult>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let g = meta::GetOptions {
            uid: r.get_ref().uid.clone(),
            name: r.get_ref().name.clone(),
        };
        let name = d
            .memberships
            .values()
            .find(|w| ref_matches(&g, w.metadata.as_ref()))
            .and_then(|w| w.metadata.as_ref())
            .map(|m| m.name.clone())
            .ok_or_else(|| Status::not_found("Membership"))?;
        d.memberships.remove(&name);
        Ok(Response::new(meta::OperationResult {}))
    }
    async fn list_membership(
        &self,
        r: Request<proto::ListMembershipOptions>,
    ) -> Result<Response<proto::MembershipList>, Status> {
        check(&r)?;
        let d = self.shared.data.lock().unwrap();
        let (items, info) = page(d.memberships.values().cloned(), r.get_ref().common);
        Ok(Response::new(proto::MembershipList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
    async fn update_membership(
        &self,
        r: Request<proto::Membership>,
    ) -> Result<Response<proto::Membership>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        let mut d = self.shared.data.lock().unwrap();
        d.updates += 1;
        d.memberships.insert(
            resource.metadata.as_ref().unwrap().name.clone(),
            resource.clone(),
        );
        Ok(Response::new(resource))
    }
    async fn create_membership(
        &self,
        r: Request<proto::CreateMembershipRequest>,
    ) -> Result<Response<proto::Membership>, Status> {
        check(&r)?;
        let resource = proto::Membership {
            metadata: Some(md("member.space")),
            spec: Some(proto::membership::Spec {
                role: r.get_ref().role,
            }),
            ..Default::default()
        };
        self.shared
            .data
            .lock()
            .unwrap()
            .memberships
            .insert("member.space".into(), resource.clone());
        Ok(Response::new(resource))
    }
    async fn get_space_membership(
        &self,
        r: Request<proto::GetSpaceMembershipRequest>,
    ) -> Result<Response<proto::Membership>, Status> {
        check(&r)?;
        Ok(Response::new(
            self.shared
                .data
                .lock()
                .unwrap()
                .memberships
                .values()
                .next()
                .cloned()
                .ok_or_else(|| Status::not_found("member"))?,
        ))
    }
    async fn build_template(
        &self,
        r: Request<proto::BuildTemplateRequest>,
    ) -> Result<Response<proto::Template>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let reference = r.get_ref().template_ref.as_ref().unwrap();
        let t = d
            .templates
            .values_mut()
            .find(|t| {
                t.metadata
                    .as_ref()
                    .is_some_and(|m| m.name == reference.name || m.uid == reference.uid)
            })
            .ok_or_else(|| Status::not_found("Template"))?;
        t.status.get_or_insert_default().build_info = Some(proto::template::status::BuildInfo {
            current_running_build_id: "new-build".into(),
            current_ready_build_id: "old-build".into(),
            builds: vec![
                proto::template::status::build_info::Build {
                    id: "old-build".into(),
                    state: 2,
                    ..Default::default()
                },
                proto::template::status::build_info::Build {
                    id: "new-build".into(),
                    state: 1,
                    ..Default::default()
                },
            ],
        });
        Ok(Response::new(t.clone()))
    }
    async fn cancel_build_template(
        &self,
        r: Request<proto::CancelBuildTemplateRequest>,
    ) -> Result<Response<proto::Template>, Status> {
        check(&r)?;
        let mut d = self.shared.data.lock().unwrap();
        let reference = r.get_ref().template_ref.as_ref().unwrap();
        let t = d
            .templates
            .values_mut()
            .find(|t| {
                t.metadata
                    .as_ref()
                    .is_some_and(|m| m.name == reference.name || m.uid == reference.uid)
            })
            .ok_or_else(|| Status::not_found("Template"))?;
        let info = t
            .status
            .get_or_insert_default()
            .build_info
            .get_or_insert_default();
        for b in &mut info.builds {
            if b.id == info.current_running_build_id {
                b.is_canceled = true;
                b.state = 3;
            }
        }
        info.current_running_build_id.clear();
        Ok(Response::new(t.clone()))
    }
    async fn get_user_config(
        &self,
        r: Request<proto::GetUserConfigRequest>,
    ) -> Result<Response<proto::UserConfig>, Status> {
        check(&r)?;
        Ok(Response::new(
            self.shared.data.lock().unwrap().config.clone(),
        ))
    }
    async fn update_user_config(
        &self,
        r: Request<proto::UserConfig>,
    ) -> Result<Response<proto::UserConfig>, Status> {
        check(&r)?;
        let resource = r.into_inner();
        self.shared.data.lock().unwrap().config = resource.clone();
        Ok(Response::new(resource))
    }
    async fn list_region(
        &self,
        r: Request<proto::ListRegionOptions>,
    ) -> Result<Response<proto::RegionList>, Status> {
        check(&r)?;
        let (items, info) = page(
            [proto::Region {
                metadata: Some(md("default")),
                ..Default::default()
            }]
            .into_iter(),
            r.get_ref().common,
        );
        Ok(Response::new(proto::RegionList {
            items,
            list_response_meta: Some(info),
            ..Default::default()
        }))
    }
}
struct ExecGuard(Arc<Shared>);
impl Drop for ExecGuard {
    fn drop(&mut self) {
        self.0.active_exec.fetch_sub(1, Ordering::SeqCst);
        self.0.closed_exec.fetch_add(1, Ordering::SeqCst);
    }
}
fn stdout(data: Bytes) -> proto::ExecResponse {
    proto::ExecResponse {
        r#type: Some(proto::exec_response::Type::Stdout(
            proto::exec_response::Stdout { data },
        )),
    }
}
fn stderr(data: Bytes) -> proto::ExecResponse {
    proto::ExecResponse {
        r#type: Some(proto::exec_response::Type::Stderr(
            proto::exec_response::Stderr { data },
        )),
    }
}
fn exit(code: i32) -> proto::ExecResponse {
    proto::ExecResponse {
        r#type: Some(proto::exec_response::Type::Exit(
            proto::exec_response::Exit { code },
        )),
    }
}
#[tonic::async_trait]
impl proto::workspace_service_server::WorkspaceService for Service {
    async fn exec(
        &self,
        r: Request<tonic::Streaming<proto::ExecRequest>>,
    ) -> Result<Response<BoxStream<proto::ExecResponse>>, Status> {
        check(&r)?;
        let mut input = r.into_inner();
        let shared = self.shared.clone();
        let dir = self.dir.clone();
        let stream = async_stream::try_stream! {
            shared.active_exec.fetch_add(1,Ordering::SeqCst);let _guard=ExecGuard(shared.clone());
            let message=input.message().await?.ok_or_else(||Status::invalid_argument("first request missing"))?;
            let Some(proto::exec_request::Type::Request(request))=message.r#type else{Err(Status::invalid_argument("first message must initialize execution"))?;unreachable!()};
            shared.data.lock().unwrap().commands.push(request.clone());
            if request.command=="FAKE_HANG"{std::future::pending::<()>().await;}
            if request.command=="FAKE_NO_EXIT"{yield stdout(Bytes::from_static(b"partial"));return;}
            if request.command=="FAKE_BINARY"{yield stdout(Bytes::from_static(&[0,255,128]));yield stderr(Bytes::from_static(b"error"));yield exit(0);std::future::pending::<()>().await;}
            if request.command=="FAKE_STATUS"{Err(Status::permission_denied("exec denied"))?;}
            let mut command=tokio::process::Command::new("sh");command.arg("-c").arg(&request.command).current_dir(if request.working_dir.is_empty(){dir}else{request.working_dir.clone().into()}).kill_on_drop(true);
            for env in request.env_vars {command.env(env.key,env.value);}
            command.stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).stdin(if request.has_stdin{std::process::Stdio::piped()}else{std::process::Stdio::null()});
            let mut child=command.spawn().map_err(|e|Status::internal(e.to_string()))?;
            let mut out=child.stdout.take().unwrap();let mut err=child.stderr.take().unwrap();let mut stdin=child.stdin.take();let mut out_open=true;let mut err_open=true;let mut input_open=request.has_stdin;let mut code=None;
            let mut outbuf=vec![0;16384];let mut errbuf=vec![0;16384];
            use tokio::io::{AsyncReadExt,AsyncWriteExt};
            enum Step {Out(std::io::Result<usize>),Err(std::io::Result<usize>),Exit(std::io::Result<std::process::ExitStatus>),Input(Result<Option<proto::ExecRequest>,Status>)}
            while out_open||err_open||code.is_none(){
                let step=tokio::select! {
                    n=out.read(&mut outbuf),if out_open=>Step::Out(n),
                    n=err.read(&mut errbuf),if err_open=>Step::Err(n),
                    status=child.wait(),if code.is_none()=>Step::Exit(status),
                    msg=input.message(),if input_open=>Step::Input(msg),
                };
                match step {
                    Step::Out(n)=>{let n=n.map_err(|e|Status::internal(e.to_string()))?;if n==0{out_open=false;}else{yield stdout(Bytes::copy_from_slice(&outbuf[..n]));}},
                    Step::Err(n)=>{let n=n.map_err(|e|Status::internal(e.to_string()))?;if n==0{err_open=false;}else{yield stderr(Bytes::copy_from_slice(&errbuf[..n]));}},
                    Step::Exit(status)=>{code=Some(status.map_err(|e|Status::internal(e.to_string()))?.code().unwrap_or(137));},
                    Step::Input(msg)=>{match msg?{Some(proto::ExecRequest{r#type:Some(proto::exec_request::Type::WriteData(w))})=>{if let Some(stdin)=stdin.as_mut(){stdin.write_all(&w.data).await.map_err(|e|Status::internal(e.to_string()))?;}},Some(proto::ExecRequest{r#type:Some(proto::exec_request::Type::Kill(_))})=>{child.start_kill().map_err(|e|Status::internal(e.to_string()))?;},None=>input_open=false,_=>{}}},
                }
            }
            yield exit(code.unwrap());
            // Cordium keeps this RPC open after Exit; the SDK must release it itself.
            std::future::pending::<()>().await;
        };
        Ok(Response::new(Box::pin(stream)))
    }
    async fn create_terminal(
        &self,
        r: Request<proto::CreateTerminalRequest>,
    ) -> Result<Response<proto::CreateTerminalResponse>, Status> {
        check(&r)?;
        self.shared
            .data
            .lock()
            .unwrap()
            .terminals
            .push("ws1-term".into());
        Ok(Response::new(proto::CreateTerminalResponse {
            id: "ws1-term".into(),
        }))
    }
    async fn list_terminal(
        &self,
        r: Request<proto::ListTerminalRequest>,
    ) -> Result<Response<proto::ListTerminalResponse>, Status> {
        check(&r)?;
        let items = self
            .shared
            .data
            .lock()
            .unwrap()
            .terminals
            .iter()
            .map(|id| proto::Terminal { id: id.clone() })
            .collect();
        Ok(Response::new(proto::ListTerminalResponse { items }))
    }
    async fn remove_terminal(
        &self,
        r: Request<proto::RemoveTerminalRequest>,
    ) -> Result<Response<proto::RemoveTerminalResponse>, Status> {
        check(&r)?;
        self.shared
            .data
            .lock()
            .unwrap()
            .terminals
            .retain(|id| id != &r.get_ref().id);
        Ok(Response::new(proto::RemoveTerminalResponse {}))
    }
    async fn write_terminal_data(
        &self,
        r: Request<proto::WriteTerminalDataRequest>,
    ) -> Result<Response<proto::WriteTerminalDataResponse>, Status> {
        check(&r)?;
        self.shared
            .data
            .lock()
            .unwrap()
            .terminal_writes
            .push(r.into_inner().data);
        Ok(Response::new(proto::WriteTerminalDataResponse {}))
    }
    async fn set_terminal_window_size(
        &self,
        r: Request<proto::SetTerminalWindowSizeRequest>,
    ) -> Result<Response<proto::SetTerminalWindowSizeResponse>, Status> {
        check(&r)?;
        self.shared
            .data
            .lock()
            .unwrap()
            .terminal_sizes
            .push((r.get_ref().cols, r.get_ref().rows));
        Ok(Response::new(proto::SetTerminalWindowSizeResponse {}))
    }
    async fn listen_terminal(
        &self,
        r: Request<proto::ListenTerminalRequest>,
    ) -> Result<Response<BoxStream<proto::ListenTerminalResponse>>, Status> {
        check(&r)?;
        let stream = async_stream::try_stream! {yield proto::ListenTerminalResponse{r#type:Some(proto::listen_terminal_response::Type::Stdout(proto::listen_terminal_response::Stdout{data:Bytes::from_static(b"prompt> ")}))};yield proto::ListenTerminalResponse{r#type:Some(proto::listen_terminal_response::Type::WindowSize(proto::listen_terminal_response::WindowSize{cols:120,rows:40}))};std::future::pending::<()>().await;};
        Ok(Response::new(Box::pin(stream)))
    }
    async fn listen_log(
        &self,
        r: Request<proto::ListenLogRequest>,
    ) -> Result<Response<BoxStream<proto::ListenLogResponse>>, Status> {
        check(&r)?;
        let stream = async_stream::try_stream! {yield proto::ListenLogResponse::default();std::future::pending::<()>().await;};
        Ok(Response::new(Box::pin(stream)))
    }
}
#[tonic::async_trait]
impl proto::management_service_server::ManagementService for Service {
    async fn get_cluster_config(
        &self,
        r: Request<proto::GetClusterConfigRequest>,
    ) -> Result<Response<proto::ClusterConfig>, Status> {
        check(&r)?;
        Ok(Response::new(
            self.shared.data.lock().unwrap().cluster.clone(),
        ))
    }
    async fn update_cluster_config(
        &self,
        r: Request<proto::ClusterConfig>,
    ) -> Result<Response<proto::ClusterConfig>, Status> {
        check(&r)?;
        self.shared.data.lock().unwrap().cluster = r.get_ref().clone();
        Ok(Response::new(r.into_inner()))
    }
}
#[tonic::async_trait]
impl authv1::main_service_server::MainService for Service {
    async fn authenticate_with_authentication_token(
        &self,
        r: Request<authv1::AuthenticateWithAuthenticationTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        assert_eq!(r.get_ref().authentication_token, "one-time");
        self.shared.auth_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.shared.auth_delay.load(Ordering::SeqCst) as u64,
        ))
        .await;
        if self.shared.auth_fail.load(Ordering::SeqCst) {
            return Err(Status::unauthenticated(
                "token consumed but session response lost",
            ));
        }
        Ok(Response::new(authv1::SessionToken {
            access_token: "session-token".into(),
            refresh_token: "session-refresh".into(),
            expires_in: 3600,
            refresh_token_expires_in: 7200,
        }))
    }
    async fn authenticate_with_refresh_token(
        &self,
        r: Request<authv1::AuthenticateWithRefreshTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        assert_eq!(
            r.metadata().get("x-octelium-refresh-token").unwrap(),
            "session-refresh"
        );
        self.shared.refresh_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.shared.auth_delay.load(Ordering::SeqCst) as u64,
        ))
        .await;
        Ok(Response::new(authv1::SessionToken {
            access_token: "refreshed-token".into(),
            expires_in: 3600,
            ..Default::default()
        }))
    }
}
struct Cluster {
    client: Client,
    shared: Arc<Shared>,
    server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
    endpoint: String,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.client.close();
        self.server.abort();
    }
}
impl Cluster {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(Shared::default());
        let service = Service {
            shared: shared.clone(),
            dir: dir.path().to_path_buf(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::main_service_server::MainServiceServer::new(
                    service.clone(),
                ))
                .add_service(
                    proto::workspace_service_server::WorkspaceServiceServer::new(service.clone()),
                )
                .add_service(
                    proto::management_service_server::ManagementServiceServer::new(service.clone()),
                )
                .add_service(authv1::main_service_server::MainServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = Client::builder()
            .domain("example.com")
            .api_endpoint(&endpoint)
            .configure_transport(|builder| builder.allow_insecure_api(true))
            .access_token("token")
            .without_environment_credentials()
            .build()
            .await
            .unwrap();
        Self {
            client,
            shared,
            server,
            _dir: dir,
            endpoint,
        }
    }
    async fn workspace(&self) -> cordium::Workspace {
        self.client
            .workspaces()
            .run(WorkspaceOptions::new().image("alpine:latest"))
            .await
            .unwrap()
    }
    async fn authenticated(&self) -> Client {
        Client::builder()
            .domain("example.com")
            .api_endpoint(&self.endpoint)
            .configure_transport(|builder| builder.allow_insecure_api(true))
            .authenticator(cordium::AuthenticationToken::new("one-time"))
            .without_environment_credentials()
            .build()
            .await
            .unwrap()
    }
}
fn short_wait() -> WaitOptions {
    WaitOptions::default()
        .timeout(Some(Duration::from_millis(100)))
        .poll_interval(Duration::from_millis(5))
}

#[tokio::test]
async fn workspace_sources_lifecycle_cache_and_urls() {
    let c = Cluster::new().await;
    let ws = c
        .client
        .workspaces()
        .run_with(
            WorkspaceOptions::new()
                .template("base.space")
                .image("python:3.14")
                .env("API_KEY", cordium::EnvValue::secret("api.space"))
                .variable("VERSION", "v1")
                .resources(cordium::Resources::new().cpu_millicores(1000))
                .application(cordium::Application::new("web", 3000).default_app()),
            StartOptions::default()
                .region("eu")
                .variable("VERSION", "v2"),
            short_wait(),
        )
        .await
        .unwrap();
    assert_eq!(ws.state(), cordium::State::Running);
    assert!(ws.is_ready());
    assert_eq!(ws.url().as_deref(), Some("https://abc.cordium.example.com"));
    assert_eq!(ws.app_url("web").unwrap(), ws.url());
    assert_eq!(
        ws.port_url(8080).unwrap().as_deref(),
        Some("https://port_8080_abc.cordium.example.com")
    );
    assert!(ws.app_url("bad.name").is_err());
    {
        let d = c.shared.data.lock().unwrap();
        assert_eq!(
            d.created[0]
                .status
                .as_ref()
                .unwrap()
                .template_ref
                .as_ref()
                .unwrap()
                .name,
            "base.space"
        );
        assert_eq!(d.starts[0].config.as_ref().unwrap().vars[0].value, "v2");
        assert_eq!(
            d.starts[0]
                .config
                .as_ref()
                .unwrap()
                .region_ref
                .as_ref()
                .unwrap()
                .name,
            "eu"
        );
    }
    let mut copy = ws.proto();
    copy.metadata.as_mut().unwrap().name = "local edit".into();
    assert_ne!(ws.name(), "local edit");
    let clone = ws.clone();
    ws.stop().await.unwrap();
    assert!(clone.is_stopped());
    assert_eq!(ws.url(), None);
    ws.wait_stopped(short_wait()).await.unwrap();
    let fetched = c
        .client
        .workspaces()
        .get(Reference::uid(ws.uid()))
        .await
        .unwrap();
    assert_eq!(fetched.name(), ws.name());
    ws.modify(|r| {
        r.metadata.as_mut().unwrap().display_name = "changed".into();
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(clone.display_name(), "changed");
    let snap = ws.snapshot("copy").await.unwrap();
    assert_eq!(snap.status.unwrap().workspace_ref.unwrap().uid, ws.uid());
    ws.delete().await.unwrap();
    assert!(
        matches!(ws.refresh().await.unwrap_err(),Error::Status(s) if s.code()==tonic::Code::NotFound)
    );
}
#[tokio::test]
async fn invalid_inputs_fail_before_creation() {
    let c = Cluster::new().await;
    let options = [
        WorkspaceOptions::new().template("a").snapshot("b"),
        WorkspaceOptions::new().snapshot("b").ephemeral(true),
        WorkspaceOptions::new().image(""),
        WorkspaceOptions::new().application(cordium::Application::new("BAD", 80)),
        WorkspaceOptions::new().application(cordium::Application::new("web", 0)),
        WorkspaceOptions::new()
            .mount("v", "/data", false)
            .mount("v2", "/data/subdir", false),
        WorkspaceOptions::new().resources(cordium::Resources::new().cpu_millicores(0)),
    ];
    for o in options {
        assert!(matches!(
            c.client.workspaces().create(o).await.unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }
    assert!(
        c.client
            .workspaces()
            .run_with(
                WorkspaceOptions::new(),
                StartOptions::default(),
                WaitOptions::default().poll_interval(Duration::ZERO)
            )
            .await
            .is_err()
    );
    assert!(c.shared.data.lock().unwrap().created.is_empty());
    assert!(
        c.client
            .templates()
            .create("tpl", WorkspaceOptions::new().ephemeral(true))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn pagination_is_lazy_and_detects_nonprogress() {
    let c = Cluster::new().await;
    for _ in 0..5 {
        c.client
            .workspaces()
            .create(WorkspaceOptions::new())
            .await
            .unwrap();
    }
    let mut all = c.client.workspaces().all(ListOptions::new().page_size(2));
    assert!(c.shared.data.lock().unwrap().list_pages.is_empty());
    assert!(all.next().await.unwrap().is_ok());
    assert!(all.next().await.unwrap().is_ok());
    assert_eq!(c.shared.data.lock().unwrap().list_pages, vec![0]);
    let mut count = 2;
    while let Some(item) = all.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 5);
    assert_eq!(c.shared.data.lock().unwrap().list_pages, vec![0, 1, 2]);
    c.shared.bad_pagination.store(true, Ordering::SeqCst);
    let mut bad = c.client.workspaces().all(ListOptions::new().page_size(2));
    let mut error = false;
    while let Some(item) = bad.next().await {
        if matches!(item, Err(Error::Protocol(_))) {
            error = true;
            break;
        }
    }
    assert!(error);
    assert!(
        c.client
            .workspaces()
            .list(ListOptions::new().of_workspace("ws1"))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn waits_fail_early_on_workspace_failure() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    {
        let mut d = c.shared.data.lock().unwrap();
        let status = d
            .workspaces
            .get_mut(&ws.name())
            .unwrap()
            .status
            .as_mut()
            .unwrap();
        status.state = cordium::State::Stopped as i32;
        status.failure = Some(proto::workspace::status::Failure {
            message: "image could not be pulled".into(),
            ..Default::default()
        });
    }
    match ws.wait_running(short_wait()).await.unwrap_err() {
        Error::WorkspaceFailed { workspace, message } => {
            assert!(message.contains("image"));
            assert_eq!(workspace.metadata.unwrap().name, ws.name());
        }
        e => panic!("{e:?}"),
    }
    ws.wait_stopped(short_wait()).await.unwrap();
}
#[tokio::test]
async fn deadlines_and_shutdown_interrupt_unary_and_stream_calls() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    c.shared.stall.store(true, Ordering::SeqCst);
    assert!(matches!(
        ws.wait_running(WaitOptions::default().timeout(Some(Duration::from_millis(20))))
            .await
            .unwrap_err(),
        Error::DeadlineExceeded
    ));
    c.shared.stall.store(false, Ordering::SeqCst);
    let mut watch = ws
        .watch(StreamOptions::default().timeout(Some(Duration::from_millis(20))))
        .await
        .unwrap();
    assert!(matches!(
        watch.next().await.unwrap(),
        Err(Error::DeadlineExceeded)
    ));
    assert!(watch.next().await.is_none());
    let mut watch = ws.watch(StreamOptions::default()).await.unwrap();
    let mut exec = ws.exec("FAKE_HANG").stream().await.unwrap();
    c.client.close();
    assert!(matches!(watch.next().await.unwrap(), Err(Error::Closed)));
    assert!(matches!(exec.next().await.unwrap(), Err(Error::Closed)));
    assert!(c.client.clone().is_closed());
    assert!(!c.shared.data.lock().unwrap().workspaces.is_empty());
    assert!(matches!(ws.refresh().await.unwrap_err(), Error::Closed));
}
#[tokio::test]
async fn execution_is_binary_bounded_checked_and_releases_rpc_on_exit() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    let result = tokio::time::timeout(Duration::from_secs(2), ws.exec("FAKE_BINARY").run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&result.stdout[..], &[0, 255, 128]);
    assert_eq!(&result.stderr[..], b"error");
    assert!(result.stdout_text().contains('�'));
    let mut stream = ws
        .exec("printf 0123456789; printf err >&2")
        .max_capture_bytes(4)
        .stream()
        .await
        .unwrap();
    let mut observed = Vec::new();
    while let Some(event) = stream.next().await {
        if let ExecEvent::Stdout(data) = event.unwrap() {
            observed.extend_from_slice(&data);
        }
    }
    let result = stream.wait().await.unwrap();
    assert_eq!(observed, b"0123456789");
    assert_eq!(result.stdout, b"0123"[..]);
    assert!(result.truncated);
    let err = ws.exec("printf failed >&2; exit 7").await.unwrap_err();
    assert!(matches!(err,Error::CommandFailed(r) if r.exit_code==7 && r.stderr==b"failed"[..]));
    let r = ws.exec("exit 3").check(false).await.unwrap();
    assert_eq!(r.exit_code, 3);
    tokio::time::timeout(Duration::from_secs(2), async {
        while c.shared.active_exec.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(c.shared.closed_exec.load(Ordering::SeqCst) >= 4);
}
#[tokio::test]
async fn command_argv_environment_stdin_kill_and_protocol_errors() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    let value = "literal ' $(touch injected); * ";
    let r = ws
        .exec(Command::argv(["printf", "%s", value]))
        .await
        .unwrap();
    assert_eq!(r.stdout_text(), value);
    assert!(!c._dir.path().join("injected").exists());
    let r = ws
        .exec("printf '%s' \"$VALUE\"")
        .env("VALUE", "custom")
        .await
        .unwrap();
    assert_eq!(r.stdout_text(), "custom");
    let r = ws
        .exec("head -c 4")
        .stdin(Bytes::from_static(&[0, 255, 1, 2]))
        .await
        .unwrap();
    assert_eq!(r.stdout, Bytes::from_static(&[0, 255, 1, 2]));
    let session = ws
        .exec("exec sleep 5")
        .stdin_enabled(true)
        .check(false)
        .stream()
        .await
        .unwrap();
    session.input().kill().await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), session.wait())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(r.exit_code, 0);
    assert!(r.killed);
    assert!(matches!(
        ws.exec("FAKE_NO_EXIT").await.unwrap_err(),
        Error::Protocol(_)
    ));
    assert!(
        matches!(ws.exec("FAKE_STATUS").await.unwrap_err(),Error::Status(s) if s.code()==tonic::Code::PermissionDenied)
    );
    assert!(matches!(
        ws.exec("FAKE_HANG")
            .timeout(Some(Duration::from_millis(20)))
            .await
            .unwrap_err(),
        Error::DeadlineExceeded
    ));
    assert!(ws.exec(Command::argv(Vec::<String>::new())).await.is_err());
    assert!(cordium::shell_quote("a\0b").is_err());
}
#[tokio::test]
async fn file_roundtrip_limits_literal_paths_and_atomic_failure() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    let data = (0..524_288).map(|i| (i % 256) as u8).collect::<Vec<_>>();
    let path = "./folder with spaces/-$(touch injected)'file";
    ws.files().write(path, &data).await.unwrap();
    assert_eq!(ws.files().read(path).await.unwrap(), data[..]);
    assert!(!c._dir.path().join("injected").exists());
    assert!(matches!(
        ws.files().max_read_bytes(10).read(path).await.unwrap_err(),
        Error::LimitExceeded(_)
    ));
    assert!(matches!(
        ws.files().read_text(path).await.unwrap_err(),
        Error::Utf8(_)
    ));
    let local = c._dir.path().join("local.bin");
    tokio::fs::write(&local, b"original").await.unwrap();
    let error = ws.files().download("./missing", &local).await.unwrap_err();
    assert!(matches!(error, Error::CommandFailed(_)));
    assert_eq!(tokio::fs::read(&local).await.unwrap(), b"original");
    ws.files().download(path, &local).await.unwrap();
    assert_eq!(tokio::fs::read(&local).await.unwrap(), data);
    ws.files().upload(&local, "./uploaded").await.unwrap();
    assert_eq!(ws.files().read("./uploaded").await.unwrap(), data[..]);
    ws.files().write_text("./empty", "").await.unwrap();
    assert!(ws.files().read("./empty").await.unwrap().is_empty());
    ws.files().mkdir("-directory").await.unwrap();
    assert!(c._dir.path().join("-directory").is_dir());
    ws.files().remove("-directory", true).await.unwrap();
    assert!(
        ws.files()
            .upload_reader("./short", 20, &b"short"[..])
            .await
            .is_err()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            tokio::fs::metadata(&local)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
#[tokio::test]
async fn terminals_persist_when_detached_and_support_concurrent_input() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    let mut terminal = ws
        .terminals()
        .create(TerminalOptions::default().size(120, 40))
        .await
        .unwrap();
    assert_eq!(
        terminal.next().await.unwrap().unwrap(),
        cordium::TerminalEvent::Output(Bytes::from_static(b"prompt> "))
    );
    let input = terminal.input();
    input.write("echo hello\n").await.unwrap();
    input.resize(100, 30).await.unwrap();
    assert!(matches!(
        terminal.next().await.unwrap().unwrap(),
        cordium::TerminalEvent::Resize {
            cols: 120,
            rows: 40
        }
    ));
    terminal.detach();
    assert_eq!(ws.terminals().list().await.unwrap(), vec!["ws1-term"]);
    assert!(input.write("no").await.is_err());
    let mut attached = ws
        .terminals()
        .attach("ws1-term", StreamOptions::default())
        .await
        .unwrap();
    attached.remove().await.unwrap();
    assert!(ws.terminals().list().await.unwrap().is_empty());
    assert!(c.shared.data.lock().unwrap().terminal_writes[0] == b"echo hello\n"[..]);
    assert_eq!(
        c.shared.data.lock().unwrap().terminal_sizes,
        vec![(100, 30)]
    );
    assert!(
        ws.terminals()
            .create(TerminalOptions::default().size(0, 24))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn resource_crud_secrets_memberships_and_preferences() {
    let c = Cluster::new().await;
    let space = c
        .client
        .spaces()
        .create(
            "team",
            cordium::SpaceOptions::default()
                .organization()
                .display_name("Team"),
        )
        .await
        .unwrap();
    assert_eq!(space.metadata.as_ref().unwrap().name, "team.cordium");
    c.client
        .spaces()
        .modify("team.cordium", |r| {
            r.metadata.as_mut().unwrap().display_name = "New name".into();
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        c.client
            .spaces()
            .get(Reference::uid("uid-team.cordium"))
            .await
            .unwrap()
            .metadata
            .unwrap()
            .display_name,
        "New name"
    );
    let before = c.shared.data.lock().unwrap().updates;
    assert!(
        c.client
            .spaces()
            .modify("team.cordium", |_| Err(Error::InvalidArgument(
                "abort".into()
            )))
            .await
            .is_err()
    );
    assert_eq!(c.shared.data.lock().unwrap().updates, before);
    let secret = c
        .client
        .secrets()
        .create_json(
            "token.team",
            serde_json::json!({"text":"value","list":[null,true,12.5]}),
        )
        .await
        .unwrap();
    assert!(secret.data.is_none());
    assert!(
        c.client
            .secrets()
            .get("token.team")
            .await
            .unwrap()
            .data
            .is_none()
    );
    assert!(
        c.client
            .secrets()
            .create_json(
                "large",
                serde_json::json!({"value":9_007_199_254_740_993u64})
            )
            .await
            .is_err()
    );
    assert!(
        c.client
            .secrets()
            .create_json("notmap", serde_json::json!([1, 2]))
            .await
            .is_err()
    );
    c.client
        .secrets()
        .create_bytes("binary", Bytes::from_static(&[0, 255]))
        .await
        .unwrap();
    c.client
        .user_secrets()
        .create_text("personal", "value")
        .await
        .unwrap();
    c.client
        .user_secrets()
        .set_text("personal", "updated")
        .await
        .unwrap();
    let key = c.client.user_secrets().create_ssh_key("ssh").await.unwrap();
    assert_eq!(key.spec.unwrap().r#type, 1);
    assert!(
        c.shared.data.lock().unwrap().user_secrets["ssh"]
            .data
            .is_none()
    );
    let member = c
        .client
        .memberships()
        .add(
            "team.cordium",
            cordium::Member::Email("user@example.com".into()),
            cordium::Role::User,
        )
        .await
        .unwrap();
    assert_eq!(member.spec.unwrap().role, 3);
    c.client
        .memberships()
        .set_role("member.space", cordium::Role::Admin)
        .await
        .unwrap();
    assert_eq!(
        c.client
            .memberships()
            .mine("team.cordium")
            .await
            .unwrap()
            .spec
            .unwrap()
            .role,
        2
    );
    c.client
        .git_providers()
        .create_github("github.team", "client", "token.team", vec!["repo".into()])
        .await
        .unwrap();
    c.client
        .git_providers()
        .create_gitlab("gitlab.team", "client", "token.team", vec![])
        .await
        .unwrap();
    c.client
        .user_config()
        .set_preferred_region("eu")
        .await
        .unwrap();
    assert_eq!(
        c.client
            .user_config()
            .get()
            .await
            .unwrap()
            .spec
            .unwrap()
            .preferred_region,
        "eu"
    );
    c.client
        .user_config()
        .set_dotfiles(proto::user_config::spec::Dotfiles {
            url: "https://example.com/dotfiles.git".into(),
            branch: "main".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        c.client
            .user_config()
            .get()
            .await
            .unwrap()
            .spec
            .unwrap()
            .preferred_region,
        "eu"
    );
    c.client
        .management()
        .modify_cluster_config(|r| {
            r.kind = "ClusterConfig".into();
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        c.client
            .management()
            .get_cluster_config()
            .await
            .unwrap()
            .kind,
        "ClusterConfig"
    );
    assert_eq!(
        c.client
            .regions()
            .list(ListOptions::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        c.client
            .spaces()
            .all(ListOptions::default())
            .next()
            .await
            .unwrap()
            .unwrap()
            .metadata
            .unwrap()
            .name,
        "team.cordium"
    );
    c.client.secrets().delete("binary").await.unwrap();
    c.client.user_secrets().delete("personal").await.unwrap();
    c.client.memberships().delete("member.space").await.unwrap();
    c.client
        .git_providers()
        .delete("github.team")
        .await
        .unwrap();
    c.client.spaces().delete("team.cordium").await.unwrap();
}
#[tokio::test]
async fn template_build_wait_tracks_exact_id_and_storage_readiness() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    c.client
        .templates()
        .create(
            "template",
            WorkspaceOptions::new()
                .image("alpine:latest")
                .task(cordium::Task::new("setup", "true"))
                .git_provider("github"),
        )
        .await
        .unwrap();
    let t = c
        .client
        .templates()
        .build("template", vec![])
        .await
        .unwrap();
    assert_eq!(
        t.status
            .unwrap()
            .build_info
            .unwrap()
            .current_running_build_id,
        "new-build"
    );
    assert!(matches!(
        c.client
            .templates()
            .wait_for_build("template", "new-build", short_wait())
            .await
            .unwrap_err(),
        Error::DeadlineExceeded
    ));
    c.client.templates().cancel_build("template").await.unwrap();
    assert!(matches!(
        c.client
            .templates()
            .wait_for_build("template", "new-build", short_wait())
            .await
            .unwrap_err(),
        Error::ResourceFailed { .. }
    ));
    assert_eq!(
        c.client
            .templates()
            .wait_for_build("template", "old-build", short_wait())
            .await
            .unwrap()
            .id,
        "old-build"
    );
    let snapshot = ws.snapshot("snapshot").await.unwrap();
    c.shared
        .data
        .lock()
        .unwrap()
        .snapshots
        .get_mut("snapshot")
        .unwrap()
        .status
        .as_mut()
        .unwrap()
        .state = 2;
    assert_eq!(
        c.client
            .snapshots()
            .wait_ready(Reference::uid(snapshot.metadata.unwrap().uid), short_wait())
            .await
            .unwrap()
            .status
            .unwrap()
            .state,
        2
    );
    let restored = c
        .client
        .workspaces()
        .create(WorkspaceOptions::new().snapshot("snapshot"))
        .await
        .unwrap();
    assert_eq!(
        restored.status().workspace_snapshot_ref.unwrap().name,
        "snapshot"
    );
    c.client
        .volumes()
        .create(
            "data",
            cordium::VolumeOptions::default()
                .size_mb(1024)
                .shared(true)
                .region("eu"),
        )
        .await
        .unwrap();
    let v = c.client.volumes().grow("data", 2048).await.unwrap();
    assert_eq!(v.spec.unwrap().size.unwrap().megabytes, 2048);
    assert!(c.client.volumes().grow("data", 1024).await.is_err());
    c.shared
        .data
        .lock()
        .unwrap()
        .volumes
        .get_mut("data")
        .unwrap()
        .status
        .as_mut()
        .unwrap()
        .state = 2;
    c.client
        .volumes()
        .wait_ready("data", short_wait())
        .await
        .unwrap();
    c.shared
        .data
        .lock()
        .unwrap()
        .snapshots
        .get_mut("snapshot")
        .unwrap()
        .status
        .as_mut()
        .unwrap()
        .state = 3;
    assert!(matches!(
        c.client
            .snapshots()
            .wait_ready("snapshot", short_wait())
            .await
            .unwrap_err(),
        Error::ResourceFailed { .. }
    ));
    c.client.snapshots().delete("snapshot").await.unwrap();
    c.client.volumes().delete("data").await.unwrap();
    c.client.templates().delete("template").await.unwrap();
}
#[tokio::test]
async fn authentication_is_shared_and_survives_caller_cancellation() {
    let c = Cluster::new().await;
    c.shared.auth_delay.store(100, Ordering::SeqCst);
    let client = c.authenticated().await;
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.workspaces().list(ListOptions::default()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while c.shared.auth_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    first.abort();
    let workspaces = client.workspaces();
    let (a, b) = tokio::join!(
        workspaces.list(ListOptions::default()),
        workspaces.list(ListOptions::default())
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(c.shared.auth_calls.load(Ordering::SeqCst), 1);
    client.transport().invalidate_access_token();
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.access_token().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while c.shared.refresh_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert_eq!(client.access_token().await.unwrap(), "refreshed-token");
    assert_eq!(c.shared.refresh_calls.load(Ordering::SeqCst), 1);
    client.close();
}
#[tokio::test]
async fn failed_one_time_authentication_is_never_replayed() {
    let c = Cluster::new().await;
    c.shared.auth_fail.store(true, Ordering::SeqCst);
    let client = c.authenticated().await;
    assert!(client.access_token().await.is_err());
    assert!(client.access_token().await.is_err());
    assert_eq!(c.shared.auth_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_opening_and_composite_start_share_total_deadlines() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    c.shared.watch_delay.store(100, Ordering::SeqCst);
    assert!(matches!(
        ws.watch(StreamOptions::default().timeout(Some(Duration::from_millis(20))))
            .await
            .unwrap_err(),
        Error::DeadlineExceeded
    ));
    assert!(matches!(
        ws.logs(StreamOptions::default().timeout(Some(Duration::MAX)))
            .await
            .unwrap_err(),
        Error::InvalidArgument(_)
    ));
    let client = Client::builder()
        .domain("example.com")
        .api_endpoint(&c.endpoint)
        .configure_transport(|builder| builder.allow_insecure_api(true))
        .access_token("token")
        .timeout(Some(Duration::from_millis(80)))
        .build()
        .await
        .unwrap();
    let ws = client.workspaces().get(ws.name()).await.unwrap();
    c.shared.rpc_delay.store(50, Ordering::SeqCst);
    assert!(matches!(
        ws.start(StartOptions::default()).await.unwrap_err(),
        Error::DeadlineExceeded
    ));
    client.close();
}
#[tokio::test]
async fn closing_a_client_unblocks_stdin_and_drops_execution() {
    let c = Cluster::new().await;
    let ws = c.workspace().await;
    let mut session = ws
        .exec("FAKE_HANG")
        .stdin_enabled(true)
        .stream()
        .await
        .unwrap();
    let input = session.input();
    let writer = tokio::spawn(async move { input.write(vec![0; 4 * 1024 * 1024]).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    c.client.close();
    assert!(matches!(session.next().await.unwrap(), Err(Error::Closed)));
    let outcome = tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome,
        Err(Error::Closed) | Err(Error::Protocol(_))
    ));
}
#[cfg(feature = "http")]
#[tokio::test]
async fn http_authorization_is_scoped_before_token_exchange() {
    let c = Cluster::new().await;
    let client = c.authenticated().await;
    assert!(matches!(
        client
            .http()
            .get("https://example.com.evil.invalid")
            .send()
            .await
            .unwrap_err(),
        octelium::Error::HttpAuthorization { .. }
    ));
    assert!(
        client
            .http()
            .get("http://abc.cordium.example.com")
            .send()
            .await
            .is_err()
    );
    assert!(
        client
            .http()
            .get("https://user:pass@abc.cordium.example.com")
            .send()
            .await
            .is_err()
    );
    assert_eq!(c.shared.auth_calls.load(Ordering::SeqCst), 0);
    client.close();
}
