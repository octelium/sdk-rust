// Copyright Octelium Labs, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "http")]
use std::net::IpAddr;

use crate::error::{Error, Result};

/// The gRPC name resolver prefixes understood by the other Octelium SDKs. The
/// Rust SDK connects to the address directly, so they are stripped.
const RESOLVER_PREFIXES: &[&str] = &["dns:///", "passthrough:///", "ipv4:///", "ipv6:///"];

/// Normalizes a Cluster domain to its lowercase ASCII form.
pub(crate) fn normalize(domain: impl AsRef<str>) -> Result<String> {
    let domain = domain
        .as_ref()
        .trim()
        .strip_suffix('.')
        .unwrap_or(domain.as_ref().trim());

    if domain.is_empty() {
        return Err(Error::config("no Cluster domain was provided"));
    }
    if domain.contains("://") || domain.contains(['/', '?', '#', '@']) {
        return Err(Error::config("the Cluster domain must be a DNS hostname"));
    }
    if domain.contains(':') {
        return Err(Error::config("the Cluster domain must not contain a port"));
    }

    let ascii = idna::domain_to_ascii(domain)
        .map_err(|err| Error::config(format!("invalid Cluster domain {domain:?}: {err}")))?;

    if ascii.split('.').any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    }) {
        return Err(Error::config(
            "the Cluster domain contains an invalid DNS label",
        ));
    }
    if ascii.len() > 253 {
        return Err(Error::config("the Cluster domain is too long"));
    }

    Ok(ascii)
}

/// Normalizes an HTTP host, which may also be an IP address literal.
#[cfg(feature = "http")]
pub(crate) fn normalize_host(host: impl AsRef<str>) -> Result<String> {
    let host = host.as_ref().trim().trim_end_matches('.');
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);

    if host.is_empty() {
        return Err(Error::config("empty HTTP host"));
    }

    // An IPv6 literal reaches this function without its brackets.
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ip.to_string());
    }

    idna::domain_to_ascii(host)
        .map_err(|err| Error::config(format!("invalid HTTP host {host:?}: {err}")))
}

/// Normalizes a gRPC endpoint into a URI that tonic can dial.
pub(crate) fn normalize_endpoint(endpoint: &str) -> Result<String> {
    let mut endpoint = endpoint.trim();

    for prefix in RESOLVER_PREFIXES {
        if let Some(rest) = endpoint.strip_prefix(prefix) {
            endpoint = rest;
            break;
        }
    }

    if endpoint.is_empty() {
        return Err(Error::config("empty API endpoint"));
    }
    if endpoint.contains('#') {
        return Err(Error::config("API endpoints must not contain fragments"));
    }

    let endpoint = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    };
    let uri: ::http::Uri = endpoint
        .parse()
        .map_err(|_| Error::config("invalid API endpoint"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none()
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        || uri
            .path_and_query()
            .is_some_and(|path| path.as_str() != "/")
    {
        return Err(Error::config(
            "API endpoints must be HTTP(S) origins without userinfo, paths or queries",
        ));
    }
    Ok(format!(
        "{}://{}",
        uri.scheme_str().unwrap(),
        uri.authority().unwrap()
    ))
}

/// Reports whether `host` is the Cluster domain or one of its subdomains.
#[cfg(feature = "http")]
pub(crate) fn is_within(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(&format!(".{domain}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_are_normalized() {
        assert_eq!(normalize(" Example.COM. ").unwrap(), "example.com");
        assert_eq!(
            normalize("octelium.example.com").unwrap(),
            "octelium.example.com"
        );
        assert_eq!(normalize("münchen.de").unwrap(), "xn--mnchen-3ya.de");
    }

    #[test]
    fn invalid_domains_are_rejected() {
        for domain in [
            "",
            "   ",
            "https://example.com",
            "example.com/path",
            "example.com:443",
            "user@example.com",
            "example..com",
            "example.com..",
            "-example.com",
            "example-.com",
            "example_com",
        ] {
            assert!(
                normalize(domain).is_err(),
                "expected {domain:?} to be rejected"
            );
        }
    }

    #[test]
    fn endpoints_are_normalized() {
        assert_eq!(
            normalize_endpoint("octelium-api.example.com:443").unwrap(),
            "https://octelium-api.example.com:443"
        );
        assert_eq!(
            normalize_endpoint("dns:///octelium-api.example.com:443").unwrap(),
            "https://octelium-api.example.com:443"
        );
        assert_eq!(
            normalize_endpoint("http://localhost:8080").unwrap(),
            "http://localhost:8080"
        );
        assert!(normalize_endpoint("unix:///var/run/octelium.sock").is_err());
        assert!(normalize_endpoint("  ").is_err());
        for endpoint in [
            "https://user:secret@example.com",
            "https://example.com/path",
            "https://example.com/?q=secret",
            "https://example.com/#secret",
        ] {
            assert!(
                normalize_endpoint(endpoint).is_err(),
                "expected {endpoint:?} to be rejected"
            );
        }
    }

    #[cfg(feature = "http")]
    #[test]
    fn ipv6_hosts_are_normalized_with_or_without_brackets() {
        assert_eq!(normalize_host("[::1]").unwrap(), "::1");
        assert_eq!(normalize_host("::1").unwrap(), "::1");
    }

    #[cfg(feature = "http")]
    #[test]
    fn subdomains_belong_to_the_cluster() {
        assert!(is_within("example.com", "example.com"));
        assert!(is_within("api.example.com", "example.com"));
        assert!(!is_within("notexample.com", "example.com"));
        assert!(!is_within("example.com.evil.com", "example.com"));
    }
}
