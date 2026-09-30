use cordium::{Client, Command, WorkspaceOptions};

#[tokio::main]
async fn main() -> cordium::Result<()> {
    let client = Client::from_env().await?;
    let workspace = client
        .workspaces()
        .run(WorkspaceOptions::new().image("python:3.14").ephemeral(true))
        .await?;
    let outcome = async {
        workspace
            .files()
            .write_text("/tmp/greeting.txt", "Hello from Rust!\n")
            .await?;
        let result = workspace
            .exec(Command::argv(["cat", "/tmp/greeting.txt"]))
            .await?;
        println!("{}", result.stdout_text());
        Ok::<_, cordium::Error>(())
    }
    .await;
    let cleanup = workspace.delete().await;
    outcome?;
    cleanup
}
