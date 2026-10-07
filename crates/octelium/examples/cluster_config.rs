#[allow(dead_code)]
mod support;

use octelium::apis::corev1;
use octelium::Client;
use support::{required, rpc, Result};

const HELP: &str = "cluster_config get\ncluster_config set-session-limit human|workload MAX\n\nMAX must be 1..1000. The update preserves the remaining ClusterConfig fields.";

pub async fn run(client: &Client, args: &[String]) -> Result<()> {
    let mut api = client.core_v1();
    match required(args, 0, "command")? {
        "get" => println!(
            "{:#?}",
            api.get_cluster_config(rpc(corev1::GetClusterConfigRequest {}))
                .await?
                .into_inner()
        ),
        "set-session-limit" => {
            let kind = required(args, 1, "TYPE")?;
            if !matches!(kind, "human" | "workload") {
                return Err("TYPE must be human or workload".into());
            }
            let max: u32 = required(args, 2, "MAX")?.parse()?;
            if !(1..=1000).contains(&max) {
                return Err("MAX must be 1..1000".into());
            }
            let mut config = api
                .get_cluster_config(rpc(corev1::GetClusterConfigRequest {}))
                .await?
                .into_inner();
            let session = config
                .spec
                .get_or_insert_default()
                .session
                .get_or_insert_default();
            match kind {
                "human" => session.human.get_or_insert_default().max_per_user = max,
                "workload" => session.workload.get_or_insert_default().max_per_user = max,
                _ => unreachable!(),
            }
            println!(
                "{:#?}",
                api.update_cluster_config(rpc(config)).await?.into_inner()
            );
        }
        _ => return Err(HELP.into()),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(args) = support::arguments(HELP) else {
        return Ok(());
    };
    let client = Client::from_env().await?;
    let result = run(&client, &args).await;
    client.shutdown().await;
    result
}
