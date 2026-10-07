//! Calls an HTTP Service behind the Cluster.
//!
//! ```sh
//! export OCTELIUM_DOMAIN=example.com
//! export OCTELIUM_AUTH_TOKEN=...
//! cargo run --example http_service -- https://my-service.example.com/api
//! ```

use octelium::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--help") {
        println!("http_service <HTTPS URL within the Cluster domain>");
        return Ok(());
    }
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: http_service <url within the Cluster domain>")?;

    let client = Client::from_env().await?;

    let result = async {
        let resp = client.http().get(&url).send().await?;
        println!("{}", resp.status().as_u16());
        println!("{}", resp.error_for_status()?.text().await?);
        Ok(())
    }
    .await;
    client.shutdown().await;
    result
}
