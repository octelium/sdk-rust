#![cfg(feature = "http")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use octelium::{AccessToken, AccessTokenProviderFn, Client, Error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server(response: String) -> (String, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        stream.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(request).unwrap()
    });
    (origin, task)
}

async fn client(origin: &str) -> Client {
    Client::builder()
        .domain("example.com")
        .access_token("fresh")
        .allow_insecure_http(true)
        .authorized_http_origins([origin])
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn redirects_are_returned_without_contacting_the_target() {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = format!("http://{}/private", target.local_addr().unwrap());
    let (origin, task) = server(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {destination}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")).await;
    let client = client(&origin).await;
    let response = client
        .http()
        .get(format!("{origin}/start"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err()
    );
    let request = task.await.unwrap().to_ascii_lowercase();
    assert!(request.contains("x-octelium-auth: fresh"));
    assert!(!request.contains("authorization:"));
    client.shutdown().await;
}

#[tokio::test]
async fn alternate_credentials_are_removed_without_losing_application_cookies() {
    let (origin, task) =
        server("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()).await;
    let client = client(&origin).await;
    client
        .http()
        .get(&origin)
        .header("authorization", "Bearer stale")
        .header("x-octelium-auth", "stale")
        .header("x-octelium-refresh-token", "secret")
        .header(
            "cookie",
            "application=ok; octelium_auth=stale; octelium_rt=secret",
        )
        .send()
        .await
        .unwrap();
    let request = task.await.unwrap().to_ascii_lowercase();
    assert!(request.contains("x-octelium-auth: fresh"));
    assert!(request.contains("cookie: application=ok"));
    assert!(!request.contains("stale"));
    assert!(!request.contains("secret"));
    client.shutdown().await;
}

#[tokio::test]
async fn http_401_invalidates_only_the_sent_token_without_replaying_the_request() {
    let (origin, task) = server(
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    )
    .await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = Client::builder()
        .domain("example.com")
        .allow_insecure_http(true)
        .authorized_http_origins([&origin])
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    Ok(AccessToken::new(format!(
                        "token-{}",
                        attempts.fetch_add(1, Ordering::SeqCst)
                    )))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    assert_eq!(
        client.http().get(&origin).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(client.access_token().await.unwrap(), "token-1");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn fixed_rejected_tokens_require_new_credentials() {
    let (origin, task) = server(
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    )
    .await;
    let client = client(&origin).await;
    client.http().get(&origin).send().await.unwrap();
    assert!(matches!(
        client.token().await,
        Err(Error::AccessTokenRejected)
    ));
    task.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn custom_policies_cannot_disable_https_userinfo_or_host_guards() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = Client::builder()
        .domain("example.com")
        .http_authorization_policy(|_: &reqwest::Request| Ok(()))
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(AccessToken::new("token"))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    for request in [
        client.http().get("http://allowed.test/").build().unwrap(),
        client
            .http()
            .get("https://user:secret@allowed.test/private?secret=value")
            .build()
            .unwrap(),
        client
            .http()
            .get("https://allowed.test/")
            .header("host", "other.test")
            .build()
            .unwrap(),
        client
            .http()
            .get("https://allowed.test/")
            .header("host", "allowed.test")
            .header("host", "other.test")
            .build()
            .unwrap(),
    ] {
        let err = client.http().execute(request).await.unwrap_err();
        assert!(matches!(err, Error::HttpAuthorization { .. }));
        assert!(!err.to_string().contains("secret"));
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn request_timeout_covers_token_acquisition() {
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new(|| async {
            std::future::pending().await
        }))
        .build()
        .await
        .unwrap();
    let err = client
        .http()
        .get("https://api.example.com/")
        .timeout(Duration::from_millis(30))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DeadlineExceeded));
    client.shutdown().await;
}

#[tokio::test]
async fn retained_http_clients_and_request_builders_reject_calls_after_close() {
    let client = Client::builder()
        .domain("example.com")
        .access_token("token")
        .build()
        .await
        .unwrap();
    let http = client.http();
    let request = http.get("https://api.example.com/");
    client.shutdown().await;
    assert!(matches!(request.send().await, Err(Error::Closed)));
    assert!(matches!(
        http.get("https://api.example.com/").send().await,
        Err(Error::Closed)
    ));
    assert!(http.inner().is_err());
}

#[tokio::test]
async fn malformed_authorized_origins_are_rejected() {
    for origin in [
        "https://example.com/path",
        "https://user:secret@example.com",
        "https://example.com/?secret=value",
        "ftp://example.com",
    ] {
        assert!(matches!(
            Client::builder()
                .domain("example.com")
                .access_token("token")
                .authorized_http_origins([origin])
                .build()
                .await,
            Err(Error::Config(_))
        ));
    }
}
