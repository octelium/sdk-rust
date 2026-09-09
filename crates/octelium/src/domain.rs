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
    let domain = domain.as_ref().trim().trim_end_matches('.');

    if domain.is_empty() {
        return Err(Error::config("no Cluster domain was provided"));
    }
    if domain.contains("://") || domain.contains(['/', '?', '#', '@']) {
        return Err(Error::config(format!("invalid Cluster domain {domain:?}")));
    }
    if domain.contains(':') {
        return Err(Error::config("the Cluster domain must not contain a port"));
    }

    let ascii = idna::domain_to_ascii(domain)
        .map_err(|err| Error::config(format!("invalid Cluster domain {domain:?}: {err}")))?;

    if ascii.len() > 253 {
        return Err(Error::config("the Cluster domain is too long"));
    }

    Ok(ascii)
}

/// Normalizes an HTTP host, which may also be an IP address literal.
#[cfg(feature = "http")]
pub(crate) fn normalize_host(host: impl AsRef<str>) -> Result<String> {
    let host = host.as_ref().trim().trim_end_matches('.');

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

    if let Some((scheme, _)) = endpoint.split_once("://") {
        return match scheme.to_ascii_lowercase().as_str() {
            "http" | "https" => Ok(endpoint.to_string()),
            _ => Err(Error::config(format!(
                "unsupported API endpoint scheme {scheme:?}"
            ))),
        };
    }

    Ok(format!("https://{endpoint}"))
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
