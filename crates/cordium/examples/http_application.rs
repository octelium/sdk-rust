use cordium::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = std::env::args()
        .nth(1)
        .expect("usage: http_application WORKSPACE");
    let client = Client::from_env().await?;
    let workspace = client.workspaces().get(name).await?;
    let url = workspace.port_url(3000)?.ok_or("workspace is stopped")?;
    let response = client
        .http()
        .get(url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?;
    println!("HTTP {}", response.status());
    println!("{}", response.text().await?);
    Ok(())
}
