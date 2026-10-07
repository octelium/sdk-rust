//! Lists the Users of a Cluster.
//!
//! ```sh
//! export OCTELIUM_DOMAIN=example.com
//! export OCTELIUM_AUTH_TOKEN=...
//! cargo run --example list_users
//! ```

use octelium::apis::corev1;
use octelium::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--help") {
        println!("list_users lists every User, following Core API pagination");
        return Ok(());
    }
    let client = Client::from_env().await?;
    let result = async {
        let mut api = client.core_v1();
        let mut page = 0;
        loop {
            let mut request = tonic::Request::new(corev1::ListUserOptions {
                common: Some(octelium::apis::metav1::CommonListOptions {
                    page,
                    items_per_page: 100,
                    ..Default::default()
                }),
            });
            request.set_timeout(std::time::Duration::from_secs(10));
            let users = api.list_user(request).await?.into_inner();
            for user in users.items {
                let metadata = user.metadata.unwrap_or_default();
                let spec = user.spec.unwrap_or_default();
                println!("{} ({:?})", metadata.name, spec.r#type());
            }
            if !users.list_response_meta.is_some_and(|meta| meta.has_more) {
                break;
            }
            page = page.checked_add(1).ok_or("pagination overflow")?;
        }
        Ok(())
    }
    .await;
    client.shutdown().await;
    result
}
