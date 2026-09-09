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
    // The Cluster domain and the credentials come from the environment.
    let client = Client::from_env().await?;

    println!("Connected to {}", client.domain());

    let users = client
        .core_v1()
        .list_user(corev1::ListUserOptions::default())
        .await?
        .into_inner();

    for user in users.items {
        let metadata = user.metadata.unwrap_or_default();
        let spec = user.spec.unwrap_or_default();
        println!("{} ({:?})", metadata.name, spec.r#type());
    }

    Ok(())
}
