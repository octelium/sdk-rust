use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use octelium::{AccessToken, AccessTokenProviderFn, Client, Error};
use tokio::sync::{Barrier, Notify};

#[tokio::test]
async fn unavailable_transports_preserve_their_grpc_status() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    for managed_session in [false, true] {
        let builder = Client::builder()
            .domain("example.com")
            .api_endpoint(&endpoint)
            .allow_insecure_api(true)
            .request_timeout(Some(Duration::from_secs(2)));
        let client = if managed_session {
            builder.authenticator(octelium::AuthenticationToken::new("one-time"))
        } else {
            builder.access_token("access-token")
        }
        .build()
        .await
        .unwrap();
        let error = client
            .core_v1()
            .list_user(octelium_apis::corev1::ListUserOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn concurrent_failures_share_one_provider_attempt() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            let started = started.clone();
            let finish = finish.clone();
            move || {
                let attempts = attempts.clone();
                let started = started.clone();
                let finish = finish.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    finish.notified().await;
                    Err(std::io::Error::other("provider unavailable").into())
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(33));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let client = client.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client.token().await
        }));
    }
    barrier.wait().await;
    started.notified().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    finish.notify_waiters();
    for task in tasks {
        assert!(matches!(task.await.unwrap(), Err(Error::Authentication(_))));
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_caller_keeps_the_owned_exchange_alive() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            let started = started.clone();
            let finish = finish.clone();
            move || {
                let attempts = attempts.clone();
                let started = started.clone();
                let finish = finish.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    finish.notified().await;
                    Ok(AccessToken::new("fresh"))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let caller = tokio::spawn({
        let client = client.clone();
        async move { client.token().await }
    });
    started.notified().await;
    caller.abort();
    let survivor = tokio::spawn({
        let client = client.clone();
        async move { client.token().await }
    });
    finish.notify_one();
    assert_eq!(survivor.await.unwrap().unwrap().value, "fresh");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn forgetting_an_inflight_session_prevents_its_publication() {
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new({
            let started = started.clone();
            let finish = finish.clone();
            move || {
                let started = started.clone();
                let finish = finish.clone();
                async move {
                    started.notify_one();
                    finish.notified().await;
                    Ok(AccessToken::new("forgotten"))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let caller = tokio::spawn({
        let client = client.clone();
        async move { client.token().await }
    });
    started.notified().await;
    client.forget_session();
    finish.notify_one();
    assert!(matches!(caller.await.unwrap(), Err(Error::SessionChanged)));
    client.shutdown().await;
}

#[tokio::test]
async fn shutdown_waits_for_workers_and_drops_owned_credentials() {
    let owned = Arc::new(());
    let weak = Arc::downgrade(&owned);
    let started = Arc::new(Notify::new());
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new({
            let started = started.clone();
            move || {
                let owned = owned.clone();
                let started = started.clone();
                async move {
                    let _owned = owned;
                    started.notify_one();
                    std::future::pending().await
                }
            }
        }))
        .build()
        .await
        .unwrap();
    let caller = tokio::spawn({
        let client = client.clone();
        async move { client.token().await }
    });
    started.notified().await;
    client.shutdown().await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(caller.await.unwrap(), Err(Error::Closed)));
    assert!(client.raw_channel().is_err());
    client.shutdown().await;
}

#[tokio::test]
async fn hung_providers_have_a_client_owned_timeout() {
    let client = Client::builder()
        .domain("example.com")
        .authentication_timeout(Duration::from_millis(30))
        .access_token_provider(AccessTokenProviderFn::new(|| async {
            std::future::pending().await
        }))
        .build()
        .await
        .unwrap();
    assert!(matches!(client.token().await, Err(Error::DeadlineExceeded)));
    client.shutdown().await;
}

#[tokio::test]
async fn a_panicking_provider_does_not_poison_the_next_attempt() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let client = Client::builder()
        .domain("example.com")
        .access_token_provider(AccessTokenProviderFn::new({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        panic!("provider panic");
                    }
                    Ok(AccessToken::new("recovered"))
                }
            }
        }))
        .build()
        .await
        .unwrap();
    assert!(matches!(
        client.token().await,
        Err(Error::Authentication(_))
    ));
    assert_eq!(client.access_token().await.unwrap(), "recovered");
    client.shutdown().await;
}

#[tokio::test]
async fn api_deadlines_include_the_shared_provider_wait() {
    let client = Client::builder()
        .domain("example.com")
        .api_endpoint("http://127.0.0.1:9")
        .allow_insecure_api(true)
        .request_timeout(Some(Duration::from_millis(30)))
        .access_token_provider(AccessTokenProviderFn::new(|| async {
            std::future::pending().await
        }))
        .build()
        .await
        .unwrap();
    let status = client
        .core_v1()
        .list_user(octelium::apis::corev1::ListUserOptions::default())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    client.shutdown().await;
}

#[tokio::test]
async fn invalid_timeouts_and_plaintext_endpoints_are_rejected() {
    for builder in [
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .authentication_timeout(Duration::ZERO),
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .request_timeout(Some(Duration::ZERO)),
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .authentication_timeout(Duration::MAX),
        Client::builder()
            .domain("example.com")
            .access_token("token")
            .api_endpoint("http://localhost:8080"),
    ] {
        assert!(matches!(builder.build().await, Err(Error::Config(_))));
    }
}

#[tokio::test]
async fn provider_failures_preserve_grpc_status_codes_and_error_sources() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::PermissionDenied,
        tonic::Code::ResourceExhausted,
        tonic::Code::Unauthenticated,
    ] {
        let client = Client::builder()
            .domain("example.com")
            .api_endpoint("http://127.0.0.1:9")
            .allow_insecure_api(true)
            .access_token_provider(AccessTokenProviderFn::new(move || async move {
                Err(
                    Box::new(tonic::Status::new(code, "private provider detail"))
                        as octelium::BoxError,
                )
            }))
            .build()
            .await
            .unwrap();
        let status = client
            .core_v1()
            .list_user(octelium::apis::corev1::ListUserOptions::default())
            .await
            .unwrap_err();
        assert_eq!(status.code(), code);
        assert!(!status.message().contains("private provider detail"));
        assert!(std::error::Error::source(&status).is_some());
        client.shutdown().await;
    }
}
