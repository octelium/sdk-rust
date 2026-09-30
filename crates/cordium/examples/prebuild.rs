use cordium::{Client, WaitOptions, WorkspaceOptions};

#[tokio::main]
async fn main() -> cordium::Result<()> {
    let name = std::env::args().nth(1).expect("usage: prebuild TEMPLATE");
    let client = Client::from_env().await?;
    client
        .templates()
        .create(&name, WorkspaceOptions::new().image("rust:1.98"))
        .await?;
    let template = client
        .templates()
        .build(&name, vec!["latest".into()])
        .await?;
    let id = template
        .status
        .and_then(|s| s.build_info)
        .map(|i| i.current_running_build_id)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| cordium::Error::Protocol("build ID missing".into()))?;
    let ready = client
        .templates()
        .wait_for_build(&name, &id, WaitOptions::default())
        .await?;
    println!("build {} is ready", ready.id);
    Ok(())
}
