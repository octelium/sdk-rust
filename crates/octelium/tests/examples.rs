#![allow(clippy::duplicate_mod)]

#[path = "../examples/cluster_config.rs"]
#[allow(dead_code)]
mod cluster_config;
#[path = "../examples/credentials.rs"]
#[allow(dead_code)]
mod credentials;
#[path = "../examples/policies.rs"]
#[allow(dead_code)]
mod policies;
#[path = "../examples/services.rs"]
#[allow(dead_code)]
mod services;
#[path = "../examples/users.rs"]
#[allow(dead_code)]
mod users;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use octelium::apis::{corev1, metav1};
use octelium::Client;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    user: Mutex<corev1::User>,
    service: Mutex<corev1::Service>,
    policy: Mutex<corev1::Policy>,
    credential: Mutex<corev1::Credential>,
    config: Mutex<corev1::ClusterConfig>,
    pages: Mutex<Vec<(String, u32)>>,
    writes: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    generations: AtomicUsize,
    fail_reads: AtomicBool,
}

impl State {
    fn read(&self) -> Result<(), Status> {
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(Status::unavailable("read failed"));
        }
        Ok(())
    }

    fn page(
        &self,
        kind: &str,
        common: Option<metav1::CommonListOptions>,
    ) -> metav1::ListResponseMeta {
        let common = common.unwrap();
        assert_eq!(common.items_per_page, 100);
        self.pages.lock().unwrap().push((kind.into(), common.page));
        metav1::ListResponseMeta {
            has_more: common.page == 0,
            page: common.page,
            ..Default::default()
        }
    }
}

#[tonic::async_trait]
impl corev1::main_service_server::MainService for State {
    async fn list_user(
        &self,
        request: Request<corev1::ListUserOptions>,
    ) -> Result<Response<corev1::UserList>, Status> {
        Ok(Response::new(corev1::UserList {
            list_response_meta: Some(self.page("user", request.into_inner().common)),
            ..Default::default()
        }))
    }
    async fn list_service(
        &self,
        request: Request<corev1::ListServiceOptions>,
    ) -> Result<Response<corev1::ServiceList>, Status> {
        Ok(Response::new(corev1::ServiceList {
            list_response_meta: Some(self.page("service", request.into_inner().common)),
            ..Default::default()
        }))
    }
    async fn list_policy(
        &self,
        request: Request<corev1::ListPolicyOptions>,
    ) -> Result<Response<corev1::PolicyList>, Status> {
        Ok(Response::new(corev1::PolicyList {
            list_response_meta: Some(self.page("policy", request.into_inner().common)),
            ..Default::default()
        }))
    }
    async fn list_credential(
        &self,
        request: Request<corev1::ListCredentialOptions>,
    ) -> Result<Response<corev1::CredentialList>, Status> {
        Ok(Response::new(corev1::CredentialList {
            list_response_meta: Some(self.page("credential", request.into_inner().common)),
            ..Default::default()
        }))
    }
    async fn get_user(
        &self,
        _: Request<metav1::GetOptions>,
    ) -> Result<Response<corev1::User>, Status> {
        self.read()?;
        Ok(Response::new(self.user.lock().unwrap().clone()))
    }
    async fn get_service(
        &self,
        _: Request<metav1::GetOptions>,
    ) -> Result<Response<corev1::Service>, Status> {
        self.read()?;
        Ok(Response::new(self.service.lock().unwrap().clone()))
    }
    async fn get_policy(
        &self,
        _: Request<metav1::GetOptions>,
    ) -> Result<Response<corev1::Policy>, Status> {
        self.read()?;
        Ok(Response::new(self.policy.lock().unwrap().clone()))
    }
    async fn get_credential(
        &self,
        _: Request<metav1::GetOptions>,
    ) -> Result<Response<corev1::Credential>, Status> {
        self.read()?;
        Ok(Response::new(self.credential.lock().unwrap().clone()))
    }
    async fn get_cluster_config(
        &self,
        _: Request<corev1::GetClusterConfigRequest>,
    ) -> Result<Response<corev1::ClusterConfig>, Status> {
        self.read()?;
        Ok(Response::new(self.config.lock().unwrap().clone()))
    }
    async fn create_user(
        &self,
        request: Request<corev1::User>,
    ) -> Result<Response<corev1::User>, Status> {
        let value = request.into_inner();
        *self.user.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("create-user".into());
        Ok(Response::new(value))
    }
    async fn create_service(
        &self,
        request: Request<corev1::Service>,
    ) -> Result<Response<corev1::Service>, Status> {
        let value = request.into_inner();
        *self.service.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("create-service".into());
        Ok(Response::new(value))
    }
    async fn create_policy(
        &self,
        request: Request<corev1::Policy>,
    ) -> Result<Response<corev1::Policy>, Status> {
        let value = request.into_inner();
        *self.policy.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("create-policy".into());
        Ok(Response::new(value))
    }
    async fn create_credential(
        &self,
        request: Request<corev1::Credential>,
    ) -> Result<Response<corev1::Credential>, Status> {
        let value = request.into_inner();
        *self.credential.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("create-credential".into());
        Ok(Response::new(value))
    }
    async fn update_user(
        &self,
        request: Request<corev1::User>,
    ) -> Result<Response<corev1::User>, Status> {
        let value = request.into_inner();
        *self.user.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("update-user".into());
        Ok(Response::new(value))
    }
    async fn update_service(
        &self,
        request: Request<corev1::Service>,
    ) -> Result<Response<corev1::Service>, Status> {
        let value = request.into_inner();
        *self.service.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("update-service".into());
        Ok(Response::new(value))
    }
    async fn update_policy(
        &self,
        request: Request<corev1::Policy>,
    ) -> Result<Response<corev1::Policy>, Status> {
        let value = request.into_inner();
        *self.policy.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("update-policy".into());
        Ok(Response::new(value))
    }
    async fn update_credential(
        &self,
        request: Request<corev1::Credential>,
    ) -> Result<Response<corev1::Credential>, Status> {
        let value = request.into_inner();
        *self.credential.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("update-credential".into());
        Ok(Response::new(value))
    }
    async fn update_cluster_config(
        &self,
        request: Request<corev1::ClusterConfig>,
    ) -> Result<Response<corev1::ClusterConfig>, Status> {
        let value = request.into_inner();
        *self.config.lock().unwrap() = value.clone();
        self.writes.lock().unwrap().push("update-config".into());
        Ok(Response::new(value))
    }
    async fn delete_user(
        &self,
        request: Request<metav1::DeleteOptions>,
    ) -> Result<Response<metav1::OperationResult>, Status> {
        self.deletes.lock().unwrap().push(request.into_inner().name);
        Ok(Response::new(metav1::OperationResult {}))
    }
    async fn delete_service(
        &self,
        request: Request<metav1::DeleteOptions>,
    ) -> Result<Response<metav1::OperationResult>, Status> {
        self.deletes.lock().unwrap().push(request.into_inner().name);
        Ok(Response::new(metav1::OperationResult {}))
    }
    async fn delete_policy(
        &self,
        request: Request<metav1::DeleteOptions>,
    ) -> Result<Response<metav1::OperationResult>, Status> {
        self.deletes.lock().unwrap().push(request.into_inner().name);
        Ok(Response::new(metav1::OperationResult {}))
    }
    async fn delete_credential(
        &self,
        request: Request<metav1::DeleteOptions>,
    ) -> Result<Response<metav1::OperationResult>, Status> {
        self.deletes.lock().unwrap().push(request.into_inner().name);
        Ok(Response::new(metav1::OperationResult {}))
    }
    async fn generate_credential_token(
        &self,
        _: Request<corev1::GenerateCredentialTokenRequest>,
    ) -> Result<Response<corev1::CredentialToken>, Status> {
        self.generations.fetch_add(1, Ordering::SeqCst);
        let kind = self
            .credential
            .lock()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .r#type();
        let token = match kind {
            corev1::credential::spec::Type::AuthToken => {
                corev1::credential_token::Type::AuthenticationToken(
                    corev1::credential_token::AuthenticationToken {
                        authentication_token: "auth-secret".into(),
                    },
                )
            }
            corev1::credential::spec::Type::Oauth2 => {
                corev1::credential_token::Type::Oauth2Credentials(
                    corev1::credential_token::OAuth2Credentials {
                        client_id: "client".into(),
                        client_secret: "oauth-secret".into(),
                    },
                )
            }
            corev1::credential::spec::Type::AccessToken => {
                corev1::credential_token::Type::AccessToken(corev1::credential_token::AccessToken {
                    access_token: "access-secret".into(),
                })
            }
            _ => return Err(Status::invalid_argument("credential type")),
        };
        Ok(Response::new(corev1::CredentialToken {
            r#type: Some(token),
        }))
    }
}

struct Cluster {
    client: Client,
    state: Arc<State>,
    server: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn new() -> Self {
        let state = Arc::new(State::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = corev1::main_service_server::MainServiceServer::from_arc(state.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = Client::builder()
            .domain("example.com")
            .api_endpoint(endpoint)
            .allow_insecure_api(true)
            .access_token("admin")
            .build()
            .await
            .unwrap();
        Self {
            client,
            state,
            server,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.client.close();
        self.server.abort();
    }
}

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).into()).collect()
}

#[tokio::test]
async fn every_list_uses_has_more_even_when_a_page_is_empty() {
    let cluster = Cluster::new().await;
    users::run(&cluster.client, &args(&["list"])).await.unwrap();
    services::run(&cluster.client, &args(&["list"]))
        .await
        .unwrap();
    policies::run(&cluster.client, &args(&["list"]))
        .await
        .unwrap();
    credentials::run(&cluster.client, &args(&["list"]))
        .await
        .unwrap();
    assert_eq!(
        *cluster.state.pages.lock().unwrap(),
        ["user", "service", "policy", "credential"]
            .into_iter()
            .flat_map(|kind| [(kind.into(), 0), (kind.into(), 1)])
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn user_creation_and_updates_preserve_unselected_fields() {
    let cluster = Cluster::new().await;
    users::run(
        &cluster.client,
        &args(&[
            "create",
            "alice",
            "human",
            "alice@example.com",
            "engineering",
        ]),
    )
    .await
    .unwrap();
    let mut expected = cluster.state.user.lock().unwrap().clone();
    expected.metadata.as_mut().unwrap().uid = "original-uid".into();
    expected.spec.as_mut().unwrap().authorization = Some(Default::default());
    *cluster.state.user.lock().unwrap() = expected.clone();
    users::run(&cluster.client, &args(&["update", "alice", "groups", "-"]))
        .await
        .unwrap();
    expected.spec.as_mut().unwrap().groups.clear();
    assert_eq!(*cluster.state.user.lock().unwrap(), expected);
    users::run(&cluster.client, &args(&["get", "alice"]))
        .await
        .unwrap();
    users::run(&cluster.client, &args(&["delete", "alice"]))
        .await
        .unwrap();
    assert_eq!(*cluster.state.deletes.lock().unwrap(), ["alice"]);
}

#[tokio::test]
async fn workload_users_reject_email_updates_before_writing() {
    let cluster = Cluster::new().await;
    users::run(&cluster.client, &args(&["create", "ci-agent", "workload"]))
        .await
        .unwrap();
    assert_eq!(
        cluster
            .state
            .user
            .lock()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .r#type(),
        corev1::user::spec::Type::Workload
    );
    assert!(users::run(
        &cluster.client,
        &args(&["update", "ci-agent", "email", "ci@example.com"])
    )
    .await
    .is_err());
    assert_eq!(
        cluster.state.writes.lock().unwrap().as_slice(),
        ["create-user"]
    );
}

#[tokio::test]
async fn public_services_attach_policies_and_preserve_upstream_details() {
    let cluster = Cluster::new().await;
    services::run(
        &cluster.client,
        &args(&[
            "create",
            "default.orders-api",
            "http://orders.default.svc:8080",
            "orders-access",
            "true",
        ]),
    )
    .await
    .unwrap();
    let mut expected = cluster.state.service.lock().unwrap().clone();
    let spec = expected.spec.as_mut().unwrap();
    assert!(spec.is_public);
    assert!(!spec.is_anonymous);
    assert_eq!(
        spec.authorization.as_ref().unwrap().policies,
        ["orders-access"]
    );
    spec.config
        .as_mut()
        .unwrap()
        .upstream
        .as_mut()
        .unwrap()
        .user = "upstream-owner".into();
    *cluster.state.service.lock().unwrap() = expected.clone();
    services::run(
        &cluster.client,
        &args(&[
            "update",
            "default.orders-api",
            "upstream",
            "https://orders.internal:8443",
        ]),
    )
    .await
    .unwrap();
    expected
        .spec
        .as_mut()
        .unwrap()
        .config
        .as_mut()
        .unwrap()
        .upstream
        .as_mut()
        .unwrap()
        .r#type = Some(corev1::service::spec::config::upstream::Type::Url(
        "https://orders.internal:8443".into(),
    ));
    assert_eq!(*cluster.state.service.lock().unwrap(), expected);
    services::run(&cluster.client, &args(&["get", "default.orders-api"]))
        .await
        .unwrap();
    services::run(&cluster.client, &args(&["delete", "default.orders-api"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn policy_updates_change_only_the_named_rule() {
    let cluster = Cluster::new().await;
    policies::run(
        &cluster.client,
        &args(&[
            "create",
            "orders-access",
            "ctx.user.metadata.name == 'ci-agent'",
        ]),
    )
    .await
    .unwrap();
    let mut expected = cluster.state.policy.lock().unwrap().clone();
    expected
        .spec
        .as_mut()
        .unwrap()
        .rules
        .push(corev1::policy::spec::Rule {
            name: "other".into(),
            ..Default::default()
        });
    *cluster.state.policy.lock().unwrap() = expected.clone();
    policies::run(
        &cluster.client,
        &args(&["update", "orders-access", "allow-access", "false"]),
    )
    .await
    .unwrap();
    expected.spec.as_mut().unwrap().rules[0].condition = Some(corev1::Condition {
        r#type: Some(corev1::condition::Type::Match("false".into())),
    });
    assert_eq!(*cluster.state.policy.lock().unwrap(), expected);
    assert!(policies::run(
        &cluster.client,
        &args(&["update", "orders-access", "missing", "true"])
    )
    .await
    .is_err());
    assert_eq!(
        cluster.state.writes.lock().unwrap().as_slice(),
        ["create-policy", "update-policy"]
    );
    policies::run(&cluster.client, &args(&["get", "orders-access"]))
        .await
        .unwrap();
    policies::run(&cluster.client, &args(&["delete", "orders-access"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn all_credential_types_separate_resource_creation_from_secret_rotation() {
    let cluster = Cluster::new().await;
    for kind in ["auth-token", "oauth2", "access-token"] {
        credentials::run(
            &cluster.client,
            &args(&["create", "ci-credential", "ci-agent", kind, "24"]),
        )
        .await
        .unwrap();
        let credential = cluster.state.credential.lock().unwrap().clone();
        let spec = credential.spec.unwrap();
        assert_eq!(spec.user, "ci-agent");
        assert_eq!(
            spec.session_type(),
            corev1::session::status::Type::Clientless
        );
        assert_eq!(
            spec.max_authentications,
            if kind == "auth-token" { 1 } else { 0 }
        );
        assert!(spec.expires_at.is_some());
        let before = cluster.state.generations.load(Ordering::SeqCst);
        credentials::run(&cluster.client, &args(&["token", "ci-credential"]))
            .await
            .unwrap();
        credentials::run(&cluster.client, &args(&["token", "ci-credential"]))
            .await
            .unwrap();
        assert_eq!(cluster.state.generations.load(Ordering::SeqCst), before + 2);
    }
    let mut expected = cluster.state.credential.lock().unwrap().clone();
    credentials::run(
        &cluster.client,
        &args(&["set-enabled", "ci-credential", "false"]),
    )
    .await
    .unwrap();
    expected.spec.as_mut().unwrap().is_disabled = true;
    assert_eq!(*cluster.state.credential.lock().unwrap(), expected);
    credentials::run(&cluster.client, &args(&["get", "ci-credential"]))
        .await
        .unwrap();
    credentials::run(&cluster.client, &args(&["delete", "ci-credential"]))
        .await
        .unwrap();
}

#[tokio::test]
async fn cluster_config_updates_preserve_the_other_session_and_ingress_settings() {
    let cluster = Cluster::new().await;
    let mut expected = corev1::ClusterConfig {
        spec: Some(corev1::cluster_config::Spec {
            ingress: Some(corev1::cluster_config::spec::Ingress {
                use_forwarded_for_header: true,
                xff_num_trusted_hops: 2,
            }),
            session: Some(corev1::cluster_config::spec::Session {
                human: Some(corev1::cluster_config::spec::session::Human {
                    max_per_user: 10,
                    ..Default::default()
                }),
                workload: Some(corev1::cluster_config::spec::session::Workload {
                    max_per_user: 50,
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    *cluster.state.config.lock().unwrap() = expected.clone();
    cluster_config::run(
        &cluster.client,
        &args(&["set-session-limit", "human", "20"]),
    )
    .await
    .unwrap();
    expected
        .spec
        .as_mut()
        .unwrap()
        .session
        .as_mut()
        .unwrap()
        .human
        .as_mut()
        .unwrap()
        .max_per_user = 20;
    assert_eq!(*cluster.state.config.lock().unwrap(), expected);
    cluster_config::run(&cluster.client, &args(&["get"]))
        .await
        .unwrap();
    for limit in ["0", "1001", "bad"] {
        assert!(cluster_config::run(
            &cluster.client,
            &args(&["set-session-limit", "human", limit])
        )
        .await
        .is_err());
    }
    assert_eq!(
        cluster.state.writes.lock().unwrap().as_slice(),
        ["update-config"]
    );
}

#[tokio::test]
async fn failed_reads_never_send_partial_updates() {
    let cluster = Cluster::new().await;
    cluster.state.fail_reads.store(true, Ordering::SeqCst);
    assert!(users::run(
        &cluster.client,
        &args(&["update", "alice", "groups", "engineering"])
    )
    .await
    .is_err());
    assert!(services::run(
        &cluster.client,
        &args(&["update", "default.orders-api", "enabled", "false"])
    )
    .await
    .is_err());
    assert!(policies::run(
        &cluster.client,
        &args(&["update", "orders-access", "allow-access", "true"])
    )
    .await
    .is_err());
    assert!(credentials::run(
        &cluster.client,
        &args(&["set-enabled", "ci-credential", "false"])
    )
    .await
    .is_err());
    assert!(cluster_config::run(
        &cluster.client,
        &args(&["set-session-limit", "human", "20"])
    )
    .await
    .is_err());
    assert!(cluster.state.writes.lock().unwrap().is_empty());
}
