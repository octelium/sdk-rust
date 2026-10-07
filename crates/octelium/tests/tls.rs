use octelium::apis::corev1;
use octelium::{Client, Error};
use tonic::{Request, Response, Status};

const CERTIFICATE: &[u8] = include_bytes!("fixtures/localhost-cert.pem");
const CA: &[u8] = include_bytes!("fixtures/ca.pem");
const PRIVATE_KEY: &[u8] = include_bytes!("fixtures/localhost-key.pem");

struct ApiService;

#[tonic::async_trait]
impl corev1::main_service_server::MainService for ApiService {
    async fn list_user(
        &self,
        request: Request<corev1::ListUserOptions>,
    ) -> Result<Response<corev1::UserList>, Status> {
        assert_eq!(
            request
                .metadata()
                .get("x-octelium-auth")
                .unwrap()
                .to_str()
                .unwrap(),
            "token"
        );
        Ok(Response::new(corev1::UserList::default()))
    }
}

#[tokio::test]
async fn private_ca_trust_applies_to_grpc_and_http() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(CERTIFICATE, PRIVATE_KEY),
            ))
            .unwrap()
            .add_service(corev1::main_service_server::MainServiceServer::new(
                ApiService,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let builder = Client::builder()
        .domain("example.com")
        .api_endpoint(&origin)
        .tls_server_name("localhost")
        .access_token("token")
        .add_root_ca_pem(CA);
    #[cfg(feature = "http")]
    let builder = builder.authorized_http_origins([&origin]);
    let client = builder.build().await.unwrap();
    client
        .core_v1()
        .list_user(corev1::ListUserOptions::default())
        .await
        .unwrap();
    #[cfg(feature = "http")]
    {
        let response = client
            .http()
            .get(format!("{origin}/service"))
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_redirection());
        let untrusted = Client::builder()
            .domain("example.com")
            .access_token("token")
            .authorized_http_origins([&origin])
            .build()
            .await
            .unwrap();
        assert!(matches!(
            untrusted.http().get(&origin).send().await,
            Err(Error::Http(_))
        ));
        untrusted.shutdown().await;
    }
    client.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn malformed_root_certificates_are_rejected_during_build() {
    assert!(matches!(
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .add_root_ca_pem(b"not a certificate")
            .build()
            .await,
        Err(Error::Config(_))
    ));
}
