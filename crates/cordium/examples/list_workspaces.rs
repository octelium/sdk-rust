use cordium::{Client, ListOptions};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> cordium::Result<()> {
    let client = Client::from_env().await?;
    let mut workspaces = client.workspaces().all(ListOptions::new().page_size(100));
    while let Some(workspace) = workspaces.next().await {
        let workspace = workspace?;
        println!("{} {:?}", workspace.name(), workspace.state());
    }
    Ok(())
}
