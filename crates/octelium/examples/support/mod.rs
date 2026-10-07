use std::time::Duration;

use octelium::apis::metav1;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn arguments(help: &str) -> Option<Vec<String>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty()
        || args
            .iter()
            .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        println!("{help}");
        None
    } else {
        Some(args)
    }
}

pub fn required<'a>(args: &'a [String], index: usize, name: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}; use --help for command syntax").into())
}

pub fn boolean(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err("boolean values must be true or false".into()),
    }
}

pub fn csv(value: &str) -> Vec<String> {
    if value == "-" {
        return Vec::new();
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn rpc<T>(value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request.set_timeout(Duration::from_secs(10));
    request
}

pub fn get(name: &str) -> metav1::GetOptions {
    metav1::GetOptions {
        name: name.into(),
        ..Default::default()
    }
}

pub fn delete(name: &str) -> metav1::DeleteOptions {
    metav1::DeleteOptions {
        name: name.into(),
        ..Default::default()
    }
}

pub fn metadata(name: &str) -> metav1::Metadata {
    metav1::Metadata {
        name: name.into(),
        ..Default::default()
    }
}

pub fn common(page: u32) -> metav1::CommonListOptions {
    metav1::CommonListOptions {
        page,
        items_per_page: 100,
        ..Default::default()
    }
}

pub fn next_page(page: u32) -> Result<u32> {
    page.checked_add(1)
        .ok_or_else(|| "pagination overflow".into())
}
