#[allow(dead_code)]
mod support;

use octelium::apis::corev1;
use octelium::Client;
use support::{required, rpc, Result};

const HELP: &str = "users list\nusers get NAME\nusers create NAME human EMAIL [GROUPS]\nusers create NAME workload [GROUPS]\nusers update NAME email EMAIL\nusers update NAME groups GROUPS\nusers update NAME enabled true|false\nusers delete NAME\n\nGROUPS is a comma-separated list of existing groups; use - to clear it.";

pub async fn run(client: &Client, args: &[String]) -> Result<()> {
    let mut api = client.core_v1();
    match required(args, 0, "command")? {
        "list" => {
            let mut page = 0;
            loop {
                let response = api
                    .list_user(rpc(corev1::ListUserOptions {
                        common: Some(support::common(page)),
                    }))
                    .await?
                    .into_inner();
                for user in response.items {
                    println!("{user:#?}");
                }
                if !response
                    .list_response_meta
                    .is_some_and(|meta| meta.has_more)
                {
                    break;
                }
                page = support::next_page(page)?;
            }
        }
        "get" => println!(
            "{:#?}",
            api.get_user(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner()
        ),
        "create" => {
            let name = required(args, 1, "NAME")?;
            let (kind, email, groups_index) = match required(args, 2, "TYPE")? {
                "human" => (
                    corev1::user::spec::Type::Human,
                    required(args, 3, "EMAIL")?.to_string(),
                    4,
                ),
                "workload" => (corev1::user::spec::Type::Workload, String::new(), 3),
                _ => return Err("TYPE must be human or workload".into()),
            };
            let user = corev1::User {
                metadata: Some(support::metadata(name)),
                spec: Some(corev1::user::Spec {
                    r#type: kind as i32,
                    email,
                    groups: args
                        .get(groups_index)
                        .map(|value| support::csv(value))
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            println!("{:#?}", api.create_user(rpc(user)).await?.into_inner());
        }
        "update" => {
            let name = required(args, 1, "NAME")?;
            let field = required(args, 2, "FIELD")?;
            let value = required(args, 3, "VALUE")?;
            let mut user = api.get_user(rpc(support::get(name))).await?.into_inner();
            let spec = user.spec.get_or_insert_default();
            match field {
                "email" => {
                    if spec.r#type() != corev1::user::spec::Type::Human {
                        return Err("email applies only to human users".into());
                    }
                    spec.email = if value == "-" {
                        String::new()
                    } else {
                        value.into()
                    };
                }
                "groups" => spec.groups = support::csv(value),
                "enabled" => spec.is_disabled = !support::boolean(value)?,
                _ => return Err("FIELD must be email, groups or enabled".into()),
            }
            println!("{:#?}", api.update_user(rpc(user)).await?.into_inner());
        }
        "delete" => {
            api.delete_user(rpc(support::delete(required(args, 1, "NAME")?)))
                .await?;
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
