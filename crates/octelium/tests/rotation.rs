use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use octelium::apis::{authv1, cordiumv1};
use octelium::{AccessToken, AccessTokenProviderFn, AuthenticationToken, Client, Error};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    authentications: AtomicUsize,
    refreshes: AtomicUsize,
    refresh_token: Mutex<String>,
    refresh_status: Option<tonic::Code>,
    lose_refresh_response: bool,
    lose_initial_response: bool,
    delay_refresh: bool,
    access_lifetime: Option<i64>,
    refreshed: Notify,
    finish_refresh: Notify,
    finish_stream: Arc<Notify>,
    requests: Mutex<Vec<authv1::AuthenticateWithAuthenticationTokenRequest>>,
}

fn session(index: usize) -> authv1::SessionToken {
    authv1::SessionToken {
        access_token: format!("access-{index}"),
        refresh_token: format!("refresh-{index}"),
        expires_in: 3600,
        refresh_token_expires_in: 7200,
    }
}

#[tonic::async_trait]
impl authv1::main_service_server::MainService for State {
    async fn authenticate_with_assertion(
        &self,
        request: Request<authv1::AuthenticateWithAssertionRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        assert!(request.get_ref().identity_provider_ref.is_none());
        assert_eq!(request.get_ref().assertion, "oidc-assertion");
        let index = self.authentications.fetch_add(1, Ordering::SeqCst) + 1;
        *self.refresh_token.lock().unwrap() = format!("refresh-{index}");
        let mut token = session(index);
        if let Some(lifetime) = self.access_lifetime {
            token.expires_in = lifetime;
        }
        Ok(Response::new(token))
    }

    async fn authenticate_with_authentication_token(
        &self,
        request: Request<authv1::AuthenticateWithAuthenticationTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        assert!(request.metadata().get("x-octelium-auth").is_none());
        self.requests.lock().unwrap().push(request.into_inner());
        let index = self.authentications.fetch_add(1, Ordering::SeqCst) + 1;
        *self.refresh_token.lock().unwrap() = format!("refresh-{index}");
        if self.lose_initial_response {
            return Err(Status::unavailable("response lost after commit"));
        }
        Ok(Response::new(session(index)))
    }

    async fn authenticate_with_refresh_token(
        &self,
        request: Request<authv1::AuthenticateWithRefreshTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        assert!(request.metadata().get("x-octelium-auth").is_none());
        assert!(request.metadata().get("authorization").is_none());
        assert!(request.metadata().get("cookie").is_none());
        assert_eq!(
            request
                .metadata()
                .get("x-octelium-refresh-token")
                .unwrap()
                .to_str()
                .unwrap(),
            self.refresh_token.lock().unwrap().as_str()
        );
        if let Some(code) = self.refresh_status {
            return Err(Status::new(code, "refresh failed"));
        }
        let index = self.refreshes.load(Ordering::SeqCst) + 1;
        *self.refresh_token.lock().unwrap() = format!("refresh-{index}");
        self.refreshed.notify_one();
        if self.lose_refresh_response {
            std::future::pending::<()>().await;
        }
        if self.delay_refresh {
            self.finish_refresh.notified().await;
        }
        Ok(Response::new(session(index)))
    }
}

#[tonic::async_trait]
impl cordiumv1::workspace_service_server::WorkspaceService for State {
    async fn listen_log(
        &self,
        _request: Request<cordiumv1::ListenLogRequest>,
    ) -> Result<
        Response<
            Pin<
                Box<
                    dyn tokio_stream::Stream<Item = Result<cordiumv1::ListenLogResponse, Status>>
                        + Send,
                >,
            >,
        >,
        Status,
    > {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let finish = self.finish_stream.clone();
        tokio::spawn(async move {
            if sender
                .send(Ok(cordiumv1::ListenLogResponse::default()))
                .await
                .is_err()
            {
                return;
            }
            finish.notified().await;
            let _ = sender
                .send(Err(Status::unauthenticated("token revoked")))
                .await;
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
}

async fn serve(state: Arc<State>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(authv1::main_service_server::MainServiceServer::from_arc(
                state.clone(),
            ))
            .add_service(
                cordiumv1::workspace_service_server::WorkspaceServiceServer::from_arc(state),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, server)
}

fn builder(addr: SocketAddr) -> octelium::ClientBuilder {
    Client::builder()
        .domain("example.com")
        .api_endpoint(format!("http://{addr}"))
        .allow_insecure_api(true)
        .without_environment_credentials()
}

#[tokio::test]
async fn refresh_rotation_uses_only_the_latest_refresh_token() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(AuthenticationToken::new("credential").with_code_verifier(vec![1, 2, 255]))
        .scopes(["api:core.MainService/ListUser"])
        .build()
        .await
        .unwrap();
    assert_eq!(client.access_token().await.unwrap(), "access-1");
    for index in 2..=4 {
        client.invalidate_access_token();
        assert_eq!(
            client.access_token().await.unwrap(),
            format!("access-{index}")
        );
    }
    assert_eq!(state.authentications.load(Ordering::SeqCst), 1);
    {
        let requests = state.requests.lock().unwrap();
        assert_eq!(requests[0].scopes, ["api:core.MainService/ListUser"]);
        assert_eq!(requests[0].code_verifier.as_ref(), [1, 2, 255]);
    }
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn a_lost_refresh_response_is_not_replayed() {
    let state = Arc::new(State {
        lose_refresh_response: true,
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authentication_timeout(Duration::from_millis(500))
        .authenticator(AuthenticationToken::new("credential"))
        .build()
        .await
        .unwrap();
    client.token().await.unwrap();
    client.invalidate_access_token();
    assert!(matches!(client.token().await, Err(Error::Refresh(_))));
    state.refreshed.notified().await;
    assert_eq!(state.refresh_token.lock().unwrap().as_str(), "refresh-2");
    assert!(matches!(client.token().await, Err(Error::SessionExpired)));
    assert_eq!(state.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(state.authentications.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn an_ambiguous_one_time_authentication_is_not_replayed() {
    let state = Arc::new(State {
        lose_initial_response: true,
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(AuthenticationToken::new("credential"))
        .build()
        .await
        .unwrap();
    assert!(client.token().await.is_err());
    assert!(matches!(client.token().await, Err(Error::SessionExpired)));
    assert_eq!(state.authentications.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn explicitly_reusable_credentials_can_replace_expired_sessions() {
    let state = Arc::new(State {
        refresh_status: Some(tonic::Code::Unauthenticated),
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(AuthenticationToken::new("credential").with_reauthentication(true))
        .build()
        .await
        .unwrap();
    assert_eq!(client.access_token().await.unwrap(), "access-1");
    client.invalidate_access_token();
    assert_eq!(client.access_token().await.unwrap(), "access-2");
    assert_eq!(state.authentications.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn grpc_response_trailers_invalidate_the_sent_token() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state.clone()).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = builder(addr)
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    Ok(AccessToken::new(format!(
                        "external-{}",
                        attempts.fetch_add(1, Ordering::SeqCst)
                    )))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let mut stream = client
        .cordium_v1_workspace()
        .listen_log(cordiumv1::ListenLogRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().is_some());
    state.finish_stream.notify_one();
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(client.access_token().await.unwrap(), "external-1");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn late_stream_failures_preserve_a_newer_token_generation() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state.clone()).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = builder(addr)
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    Ok(AccessToken::new(format!(
                        "external-{}",
                        attempts.fetch_add(1, Ordering::SeqCst)
                    )))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let mut stream = client
        .cordium_v1_workspace()
        .listen_log(cordiumv1::ListenLogRequest::default())
        .await
        .unwrap()
        .into_inner();
    stream.message().await.unwrap();
    client.invalidate_access_token();
    assert_eq!(client.access_token().await.unwrap(), "external-1");
    state.finish_stream.notify_one();
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(client.access_token().await.unwrap(), "external-1");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn closing_cancels_an_active_response_stream() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state).await;
    let client = builder(addr)
        .access_token("external")
        .build()
        .await
        .unwrap();
    let mut stream = client
        .cordium_v1_workspace()
        .listen_log(cordiumv1::ListenLogRequest::default())
        .await
        .unwrap()
        .into_inner();
    stream.message().await.unwrap();
    client.shutdown().await;
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        tonic::Code::Cancelled
    );
    server.abort();
}

#[tokio::test]
async fn caller_cancellation_preserves_a_committed_refresh_rotation() {
    let state = Arc::new(State {
        delay_refresh: true,
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(AuthenticationToken::new("credential"))
        .build()
        .await
        .unwrap();
    client.token().await.unwrap();
    client.invalidate_access_token();
    let caller = tokio::spawn({
        let client = client.clone();
        async move { client.token().await }
    });
    state.refreshed.notified().await;
    caller.abort();
    let survivor = tokio::spawn({
        let client = client.clone();
        async move { client.access_token().await }
    });
    state.finish_refresh.notify_one();
    assert_eq!(survivor.await.unwrap().unwrap(), "access-2");
    assert_eq!(state.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(state.authentications.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}

struct OneTimePreflight {
    attempts: AtomicUsize,
}

#[tonic::async_trait]
impl octelium::Authenticator for OneTimePreflight {
    async fn authenticate(
        &self,
        mut client: octelium::AuthServiceClient,
        scopes: &[String],
    ) -> Result<authv1::SessionToken, octelium::BoxError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(std::io::Error::other("local credential source unavailable").into());
        }
        Ok(client
            .authenticate_with_authentication_token(
                authv1::AuthenticateWithAuthenticationTokenRequest {
                    authentication_token: "credential".into(),
                    scopes: scopes.to_vec(),
                    ..Default::default()
                },
            )
            .await?
            .into_inner())
    }
}

#[tokio::test]
async fn local_preflight_failures_do_not_consume_one_time_credentials() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(OneTimePreflight {
            attempts: AtomicUsize::new(0),
        })
        .build()
        .await
        .unwrap();
    assert!(matches!(
        client.token().await,
        Err(Error::Authentication(_))
    ));
    assert_eq!(state.authentications.load(Ordering::SeqCst), 0);
    assert_eq!(client.access_token().await.unwrap(), "access-1");
    assert_eq!(state.authentications.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn assertions_allow_issuer_inference_and_are_not_reloaded_for_refresh() {
    let state = Arc::new(State::default());
    let (addr, server) = serve(state.clone()).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = builder(addr)
        .authenticator(octelium::Assertion::from_fn({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok("oidc-assertion".into())
                }
            }
        }))
        .build()
        .await
        .unwrap();
    assert_eq!(client.access_token().await.unwrap(), "access-1");
    client.invalidate_access_token();
    assert_eq!(client.access_token().await.unwrap(), "access-2");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn credential_provider_latency_does_not_consume_a_new_sessions_lifetime() {
    let state = Arc::new(State {
        access_lifetime: Some(1),
        ..Default::default()
    });
    let (addr, server) = serve(state).await;
    let client = builder(addr)
        .authenticator(octelium::Assertion::from_fn(|| async {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            Ok("oidc-assertion".into())
        }))
        .build()
        .await
        .unwrap();
    assert_eq!(client.access_token().await.unwrap(), "access-1");
    client.shutdown().await;
    server.abort();
}

struct RefreshSeed;

#[tonic::async_trait]
impl octelium::Authenticator for RefreshSeed {
    async fn authenticate(
        &self,
        mut client: octelium::AuthServiceClient,
        _: &[String],
    ) -> Result<authv1::SessionToken, octelium::BoxError> {
        let mut request = Request::new(authv1::AuthenticateWithRefreshTokenRequest {});
        request
            .metadata_mut()
            .insert("x-octelium-refresh-token", "refresh-1".parse().unwrap());
        Ok(client
            .authenticate_with_refresh_token(request)
            .await?
            .into_inner())
    }
}

#[tokio::test]
async fn custom_authenticators_can_supply_an_initial_refresh_credential() {
    let state = Arc::new(State {
        refresh_token: Mutex::new("refresh-1".into()),
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authenticator(RefreshSeed)
        .build()
        .await
        .unwrap();
    assert_eq!(client.access_token().await.unwrap(), "access-2");
    client.invalidate_access_token();
    assert_eq!(client.access_token().await.unwrap(), "access-3");
    assert_eq!(state.authentications.load(Ordering::SeqCst), 0);
    assert_eq!(state.refreshes.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn ambiguous_initial_refresh_credentials_are_not_replayed() {
    let state = Arc::new(State {
        refresh_token: Mutex::new("refresh-1".into()),
        lose_refresh_response: true,
        ..Default::default()
    });
    let (addr, server) = serve(state.clone()).await;
    let client = builder(addr)
        .authentication_timeout(Duration::from_millis(500))
        .authenticator(RefreshSeed)
        .build()
        .await
        .unwrap();
    assert!(matches!(client.token().await, Err(Error::DeadlineExceeded)));
    assert!(matches!(client.token().await, Err(Error::SessionExpired)));
    assert_eq!(state.refreshes.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    server.abort();
}
