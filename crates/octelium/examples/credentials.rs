#[allow(dead_code)]
mod support;

use std::time::{Duration, SystemTime};

use octelium::apis::{corev1, metav1};
use octelium::Client;
use support::{required, rpc, Result};

const HELP: &str = "credentials list [USER]\ncredentials get NAME\ncredentials create NAME USER auth-token|oauth2|access-token HOURS\ncredentials token NAME\ncredentials set-enabled NAME true|false\ncredentials delete NAME\n\nCreation returns a resource. The separate token command issues or rotates its secret.\nOAuth2 and access-token credentials require workload users. HOURS must be 1..17520.";

pub async fn run(client: &Client, args: &[String]) -> Result<()> {
    let mut api = client.core_v1();
    match required(args, 0, "command")? {
        "list" => {
            let mut page = 0;
            loop {
                let response = api
                    .list_credential(rpc(corev1::ListCredentialOptions {
                        common: Some(support::common(page)),
                        user_ref: args.get(1).map(|name| metav1::ObjectReference {
                            name: name.clone(),
                            ..Default::default()
                        }),
                    }))
                    .await?
                    .into_inner();
                for credential in response.items {
                    println!("{credential:#?}");
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
            api.get_credential(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner()
        ),
        "create" => {
            let kind = match required(args, 3, "TYPE")? {
                "auth-token" => corev1::credential::spec::Type::AuthToken,
                "oauth2" => corev1::credential::spec::Type::Oauth2,
                "access-token" => corev1::credential::spec::Type::AccessToken,
                _ => return Err("TYPE must be auth-token, oauth2 or access-token".into()),
            };
            let hours: u64 = required(args, 4, "HOURS")?.parse()?;
            if !(1..=17520).contains(&hours) {
                return Err("HOURS must be 1..17520".into());
            }
            let credential = corev1::Credential {
                metadata: Some(support::metadata(required(args, 1, "NAME")?)),
                spec: Some(corev1::credential::Spec {
                    r#type: kind as i32,
                    user: required(args, 2, "USER")?.into(),
                    session_type: corev1::session::status::Type::Clientless as i32,
                    max_authentications: if kind == corev1::credential::spec::Type::AuthToken {
                        1
                    } else {
                        0
                    },
                    expires_at: Some(
                        (SystemTime::now() + Duration::from_secs(hours * 3600)).into(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            };
            println!(
                "{:#?}",
                api.create_credential(rpc(credential)).await?.into_inner()
            );
        }
        "token" => {
            let response = api
                .generate_credential_token(rpc(corev1::GenerateCredentialTokenRequest {
                    credential_ref: Some(metav1::ObjectReference {
                        name: required(args, 1, "NAME")?.into(),
                        ..Default::default()
                    }),
                }))
                .await?
                .into_inner();
            match response
                .r#type
                .ok_or("the Cluster returned no credential token")?
            {
                corev1::credential_token::Type::AuthenticationToken(value) => {
                    println!("{}", value.authentication_token)
                }
                corev1::credential_token::Type::Oauth2Credentials(value) => println!(
                    "client_id={}\nclient_secret={}",
                    value.client_id, value.client_secret
                ),
                corev1::credential_token::Type::AccessToken(value) => {
                    println!("{}", value.access_token)
                }
            }
        }
        "set-enabled" => {
            let enabled = support::boolean(required(args, 2, "ENABLED")?)?;
            let mut credential = api
                .get_credential(rpc(support::get(required(args, 1, "NAME")?)))
                .await?
                .into_inner();
            credential.spec.get_or_insert_default().is_disabled = !enabled;
            println!(
                "{:#?}",
                api.update_credential(rpc(credential)).await?.into_inner()
            );
        }
        "delete" => {
            api.delete_credential(rpc(support::delete(required(args, 1, "NAME")?)))
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
