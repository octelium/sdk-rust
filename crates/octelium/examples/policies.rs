#[allow(dead_code)]
mod support;

use octelium::apis::corev1;
use octelium::Client;
use support::{required, rpc, Result};

const HELP: &str = "policies list\npolicies get NAME\npolicies create NAME CEL\npolicies update NAME RULE CEL\npolicies set-enabled NAME true|false\npolicies delete NAME\n\nCreation adds one named allow-access rule. Updates preserve every other rule and field.";

fn condition(expression: &str) -> corev1::Condition {
    corev1::Condition {
        r#type: Some(corev1::condition::Type::Match(expression.into())),
    }
}

pub async fn run(client: &Client, args: &[String]) -> Result<()> {
    let mut api = client.core_v1();
    match required(args, 0, "command")? {
        "list" => {
            let mut page = 0;
            loop {
                let response = api
                    .list_policy(rpc(corev1::ListPolicyOptions {
                        common: Some(support::common(page)),
                    }))
                    .await?
                    .into_inner();
                for policy in response.items {
                    println!("{policy:#?}");
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
            api.get_policy(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner()
        ),
        "create" => {
            let policy = corev1::Policy {
                metadata: Some(support::metadata(required(args, 1, "NAME")?)),
                spec: Some(corev1::policy::Spec {
                    rules: vec![corev1::policy::spec::Rule {
                        name: "allow-access".into(),
                        effect: corev1::policy::spec::rule::Effect::Allow as i32,
                        condition: Some(condition(required(args, 2, "CEL")?)),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            };
            println!("{:#?}", api.create_policy(rpc(policy)).await?.into_inner());
        }
        "update" => {
            let name = required(args, 1, "NAME")?;
            let rule_name = required(args, 2, "RULE")?;
            let expression = required(args, 3, "CEL")?;
            let mut policy = api.get_policy(rpc(support::get(name))).await?.into_inner();
            let rule = policy
                .spec
                .as_mut()
                .and_then(|spec| spec.rules.iter_mut().find(|rule| rule.name == rule_name))
                .ok_or("the named rule does not exist")?;
            rule.condition = Some(condition(expression));
            println!("{:#?}", api.update_policy(rpc(policy)).await?.into_inner());
        }
        "set-enabled" => {
            let enabled = support::boolean(required(args, 2, "ENABLED")?)?;
            let mut policy = api
                .get_policy(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner();
            policy.spec.get_or_insert_default().is_disabled = !enabled;
            println!("{:#?}", api.update_policy(rpc(policy)).await?.into_inner());
        }
        "delete" => {
            api.delete_policy(rpc(support::delete(required(args, 1, "NAME")?)))
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
