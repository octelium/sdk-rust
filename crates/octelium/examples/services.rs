#[allow(dead_code)]
mod support;

use octelium::apis::corev1;
use octelium::Client;
use support::{required, rpc, Result};

const HELP: &str = "services list\nservices get NAME\nservices create NAME HTTP_UPSTREAM POLICY [PUBLIC]\nservices update NAME upstream HTTP_UPSTREAM\nservices update NAME policies POLICIES\nservices update NAME enabled true|false\nservices delete NAME\n\nNAME includes its namespace, e.g. default.orders-api. PUBLIC defaults to false.\nPOLICIES is a comma-separated list; use - to clear it.";

fn upstream(value: &str) -> Result<corev1::service::spec::config::upstream::Type> {
    let uri: http::Uri = value.parse()?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none() {
        return Err("the upstream must be an absolute HTTP(S) URL".into());
    }
    Ok(corev1::service::spec::config::upstream::Type::Url(
        value.into(),
    ))
}

pub async fn run(client: &Client, args: &[String]) -> Result<()> {
    let mut api = client.core_v1();
    match required(args, 0, "command")? {
        "list" => {
            let mut page = 0;
            loop {
                let response = api
                    .list_service(rpc(corev1::ListServiceOptions {
                        common: Some(support::common(page)),
                        ..Default::default()
                    }))
                    .await?
                    .into_inner();
                for service in response.items {
                    println!("{service:#?}");
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
            api.get_service(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner()
        ),
        "create" => {
            let service = corev1::Service {
                metadata: Some(support::metadata(required(args, 1, "NAME")?)),
                spec: Some(corev1::service::Spec {
                    mode: corev1::service::spec::Mode::Http as i32,
                    is_public: args
                        .get(4)
                        .map(|value| support::boolean(value))
                        .transpose()?
                        .unwrap_or(false),
                    authorization: Some(corev1::service::spec::Authorization {
                        policies: vec![required(args, 3, "POLICY")?.into()],
                        ..Default::default()
                    }),
                    config: Some(corev1::service::spec::Config {
                        upstream: Some(corev1::service::spec::config::Upstream {
                            r#type: Some(upstream(required(args, 2, "HTTP_UPSTREAM")?)?),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            println!(
                "{:#?}",
                api.create_service(rpc(service)).await?.into_inner()
            );
        }
        "update" => {
            let name = required(args, 1, "NAME")?;
            let field = required(args, 2, "FIELD")?;
            let value = required(args, 3, "VALUE")?;
            let mut service = api.get_service(rpc(support::get(name))).await?.into_inner();
            let spec = service.spec.get_or_insert_default();
            match field {
                "upstream" => {
                    spec.config
                        .get_or_insert_default()
                        .upstream
                        .get_or_insert_default()
                        .r#type = Some(upstream(value)?)
                }
                "policies" => {
                    spec.authorization.get_or_insert_default().policies = support::csv(value)
                }
                "enabled" => spec.is_disabled = !support::boolean(value)?,
                _ => return Err("FIELD must be upstream, policies or enabled".into()),
            }
            println!(
                "{:#?}",
                api.update_service(rpc(service)).await?.into_inner()
            );
        }
        "delete" => {
            api.delete_service(rpc(support::delete(required(args, 1, "NAME")?)))
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
