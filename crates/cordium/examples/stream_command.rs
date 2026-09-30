use cordium::{Client, ExecEvent};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> cordium::Result<()> {
    let name = std::env::args()
        .nth(1)
        .expect("usage: stream_command WORKSPACE");
    let workspace = Client::from_env().await?.workspaces().get(name).await?;
    let mut exec = workspace
        .exec("printf 'starting\n'; sleep 1; printf 'finished\n'")
        .stream()
        .await?;
    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    while let Some(event) = exec.next().await {
        match event? {
            ExecEvent::Stdout(bytes) => stdout.write_all(&bytes).await?,
            ExecEvent::Stderr(bytes) => stderr.write_all(&bytes).await?,
            ExecEvent::Exit(code) => println!("exit: {code}"),
            _ => {}
        }
    }
    exec.wait().await?;
    Ok(())
}
