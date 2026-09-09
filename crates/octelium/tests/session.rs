//! End-to-end tests of the Session lifecycle against a real gRPC server.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use octelium::apis::authv1::main_service_server::{MainService, MainServiceServer};
use octelium::apis::{authv1, corev1};
use octelium::{AuthenticationToken, Client};
use tonic::{Request, Response, Status};

/// Records what the Cluster saw, so the tests can assert on the metadata the
/// SDK attached rather than on its internal state.
#[derive(Default)]
struct Calls {
    /// The `x-octelium-auth` values of every API call.
    access_tokens: Mutex<Vec<String>>,
    /// The `x-octelium-refresh-token` values of every authentication call.
    refresh_tokens: Mutex<Vec<Option<String>>>,
    authentications: AtomicUsize,
    refreshes: AtomicUsize,
    logouts: AtomicUsize,
}

impl Calls {
    fn metadata(request: &Request<impl Sized>, key: &str) -> Option<String> {
        request
            .metadata()
            .get(key)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }
}

struct AuthService {
    calls: Arc<Calls>,
    /// The tokens handed out, in order. The last one is reused once exhausted.
    sessions: Vec<authv1::SessionToken>,
    /// Rejects the refresh call, as an expired Session does.
    refresh_fails: bool,
}

fn session(access_token: &str, refresh_token: &str, expires_in: i64) -> authv1::SessionToken {
    authv1::SessionToken {
        access_token: access_token.to_string(),
        refresh_token: refresh_token.to_string(),
        expires_in,
        refresh_token_expires_in: 3600,
    }
}

#[tonic::async_trait]
impl MainService for AuthService {
    async fn authenticate_with_authentication_token(
        &self,
        request: Request<authv1::AuthenticateWithAuthenticationTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        let index = self.calls.authentications.fetch_add(1, Ordering::SeqCst);
        self.calls
            .refresh_tokens
            .lock()
            .unwrap()
            .push(Calls::metadata(
                &request,
                octelium::METADATA_KEY_REFRESH_TOKEN,
            ));

        if request.get_ref().authentication_token != "auth-token" {
            return Err(Status::unauthenticated("unknown authentication token"));
        }

        Ok(Response::new(
            self.sessions[index.min(self.sessions.len() - 1)].clone(),
        ))
    }

    async fn authenticate_with_assertion(
        &self,
        request: Request<authv1::AuthenticateWithAssertionRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        let index = self.calls.authentications.fetch_add(1, Ordering::SeqCst);

        if request.get_ref().assertion != "assertion" {
            return Err(Status::unauthenticated("unknown assertion"));
        }

        Ok(Response::new(
            self.sessions[index.min(self.sessions.len() - 1)].clone(),
        ))
    }

    async fn authenticate_with_refresh_token(
        &self,
        request: Request<authv1::AuthenticateWithRefreshTokenRequest>,
    ) -> Result<Response<authv1::SessionToken>, Status> {
        let index = self.calls.refreshes.fetch_add(1, Ordering::SeqCst);
        self.calls
            .refresh_tokens
            .lock()
            .unwrap()
            .push(Calls::metadata(
                &request,
                octelium::METADATA_KEY_REFRESH_TOKEN,
            ));

        if self.refresh_fails {
            return Err(Status::unauthenticated("the Session expired"));
        }

        Ok(Response::new(
            self.sessions[(index + 1).min(self.sessions.len() - 1)].clone(),
        ))
    }

    async fn logout(
        &self,
        _request: Request<authv1::LogoutRequest>,
    ) -> Result<Response<authv1::LogoutResponse>, Status> {
        self.calls.logouts.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(authv1::LogoutResponse {}))
    }

    async fn list_authenticator(
        &self,
        request: Request<authv1::ListAuthenticatorOptions>,
    ) -> Result<Response<authv1::AuthenticatorList>, Status> {
        // Any authenticated API call on this Service, used to observe the
        // access token the SDK attached.
        let token = Calls::metadata(&request, octelium::METADATA_KEY_AUTH).unwrap_or_default();
        self.calls.access_tokens.lock().unwrap().push(token.clone());

        if token != "access-1" && token != "access-2" {
            return Err(Status::unauthenticated("invalid access token"));
        }

        Ok(Response::new(authv1::AuthenticatorList::default()))
    }
}

/// Serves `service` on an ephemeral port until the returned guard is dropped.
async fn serve(service: AuthService) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MainServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    (addr, handle)
}

/// A Client holding a one-time authentication token.
async fn client(addr: SocketAddr) -> Client {
    client_with(addr, AuthenticationToken::new("auth-token")).await
}

/// A Client holding a reusable assertion.
async fn reusable_client(addr: SocketAddr) -> Client {
    client_with(
        addr,
        octelium::Assertion::from_fn(|| async { Ok("assertion".to_string()) }),
    )
    .await
}

async fn client_with(addr: SocketAddr, authenticator: impl octelium::Authenticator) -> Client {
    Client::builder()
        .domain("example.com")
        .api_endpoint(format!("http://{addr}"))
        .authenticator(authenticator)
        .without_environment_credentials()
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn authenticates_once_and_attaches_the_access_token() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("access-1", "refresh-1", 3600)],
        refresh_fails: false,
    })
    .await;

    let client = client(addr).await;

    for _ in 0..3 {
        client
            .auth_v1()
            .list_authenticator(authv1::ListAuthenticatorOptions::default())
            .await
            .unwrap();
    }

    assert_eq!(calls.authentications.load(Ordering::SeqCst), 1);
    assert_eq!(calls.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(
        *calls.access_tokens.lock().unwrap(),
        vec!["access-1"; 3],
        "every call carries the cached access token"
    );

    server.abort();
}

#[tokio::test]
async fn a_rejected_token_is_replaced_on_the_next_call() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        // The first Session is stale on arrival, so the Cluster rejects it.
        sessions: vec![
            session("stale", "refresh-1", 3600),
            session("access-1", "refresh-2", 3600),
        ],
        refresh_fails: false,
    })
    .await;

    let client = client(addr).await;

    let err = client
        .auth_v1()
        .list_authenticator(authv1::ListAuthenticatorOptions::default())
        .await
        .expect_err("the Cluster rejects the stale token");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // The rejection invalidated the cached token, so the Client refreshes the
    // Session before the next call rather than replaying it.
    client
        .auth_v1()
        .list_authenticator(authv1::ListAuthenticatorOptions::default())
        .await
        .unwrap();

    assert_eq!(calls.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        *calls.access_tokens.lock().unwrap(),
        vec!["stale", "access-1"]
    );
    assert_eq!(
        calls.refresh_tokens.lock().unwrap().as_slice(),
        [None, Some("refresh-1".to_string())],
        "the refresh call carries the Session's refresh token, the first authentication does not"
    );

    server.abort();
}

#[tokio::test]
async fn a_one_time_credential_is_not_reused_after_the_session_expires() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("access-1", "refresh-1", 3600)],
        refresh_fails: true,
    })
    .await;

    let client = client(addr).await;
    client.token().await.unwrap();

    // Losing the access token forces a refresh, which the Cluster rejects.
    client.invalidate_access_token();

    let err = client.token().await.expect_err("the Session is gone");
    assert!(
        matches!(err, octelium::Error::SessionExpired),
        "authentication tokens are one-time credentials: {err}"
    );
    assert_eq!(
        calls.authentications.load(Ordering::SeqCst),
        1,
        "the authentication token is not replayed"
    );

    server.abort();
}

#[tokio::test]
async fn a_reusable_credential_creates_a_new_session() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![
            session("access-1", "refresh-1", 3600),
            session("access-2", "refresh-2", 3600),
        ],
        // The Session is gone, so only a new authentication can recover it.
        refresh_fails: true,
    })
    .await;

    let client = reusable_client(addr).await;
    assert_eq!(client.access_token().await.unwrap(), "access-1");

    client.invalidate_access_token();

    // An assertion can be obtained again, unlike an authentication token, so
    // the Client authenticates once more instead of giving up.
    assert_eq!(client.access_token().await.unwrap(), "access-2");
    assert_eq!(calls.authentications.load(Ordering::SeqCst), 2);
    assert_eq!(calls.refreshes.load(Ordering::SeqCst), 1);

    server.abort();
}

#[tokio::test]
async fn logout_clears_the_session() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![
            session("access-1", "refresh-1", 3600),
            session("access-2", "refresh-2", 3600),
        ],
        refresh_fails: false,
    })
    .await;

    let client = reusable_client(addr).await;
    client.token().await.unwrap();
    client.logout().await.unwrap();

    assert_eq!(calls.logouts.load(Ordering::SeqCst), 1);

    // The Session is gone, so the next token authenticates from scratch.
    assert_eq!(client.access_token().await.unwrap(), "access-2");
    assert_eq!(calls.authentications.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn logging_out_of_a_one_time_session_ends_the_client() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("access-1", "refresh-1", 3600)],
        refresh_fails: false,
    })
    .await;

    let client = client(addr).await;
    client.token().await.unwrap();
    client.logout().await.unwrap();

    assert!(
        matches!(
            client.token().await.expect_err("the Session is gone"),
            octelium::Error::SessionExpired
        ),
        "an authentication token is spent once its Session ends"
    );
    assert_eq!(calls.authentications.load(Ordering::SeqCst), 1);

    server.abort();
}

#[tokio::test]
async fn a_closed_client_refuses_further_calls() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("access-1", "refresh-1", 3600)],
        refresh_fails: false,
    })
    .await;

    let client = client(addr).await;
    client.token().await.unwrap();
    client.close();

    assert!(matches!(
        client.token().await.expect_err("the client is closed"),
        octelium::Error::Closed
    ));

    let err = client
        .core_v1()
        .list_user(corev1::ListUserOptions::default())
        .await
        .expect_err("the client is closed");
    assert_eq!(err.code(), tonic::Code::Cancelled);

    server.abort();
}

#[tokio::test]
async fn an_external_access_token_is_used_as_is() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("unused", "unused", 3600)],
        refresh_fails: false,
    })
    .await;

    let client = Client::builder()
        .domain("example.com")
        .api_endpoint(format!("http://{addr}"))
        .access_token("access-2")
        .without_environment_credentials()
        .build()
        .await
        .unwrap();

    client
        .auth_v1()
        .list_authenticator(authv1::ListAuthenticatorOptions::default())
        .await
        .unwrap();

    assert_eq!(calls.authentications.load(Ordering::SeqCst), 0);
    assert_eq!(*calls.access_tokens.lock().unwrap(), vec!["access-2"]);
    assert!(matches!(
        client.logout().await.expect_err("there is no Session"),
        octelium::Error::NoManagedSession
    ));

    server.abort();
}

#[tokio::test]
async fn concurrent_callers_share_one_authentication() {
    let calls = Arc::new(Calls::default());
    let (addr, server) = serve(AuthService {
        calls: calls.clone(),
        sessions: vec![session("access-1", "refresh-1", 3600)],
        refresh_fails: false,
    })
    .await;

    let client = client(addr).await;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .auth_v1()
                .list_authenticator(authv1::ListAuthenticatorOptions::default())
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(
        calls.authentications.load(Ordering::SeqCst),
        1,
        "concurrent callers share one in-flight authentication"
    );
    assert_eq!(calls.access_tokens.lock().unwrap().len(), 32);

    server.abort();
}
