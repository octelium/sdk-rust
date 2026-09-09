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
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: http_service <url within the Cluster domain>")?;

    let client = Client::from_env().await?;

    // The access token is attached only to the Cluster domain and its
    // subdomains, so an unrelated URL is rejected before the request is sent.
    let resp = client.http().get(&url).send().await?;

    println!("{} {}", resp.status().as_u16(), url);
    println!("{}", resp.text().await?);

    Ok(())
}
