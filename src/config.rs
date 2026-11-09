//! `dev.mcpg.transport.ngrok` configuration.
//!
//! ```yaml
//! gateway:
//!   server:
//!     transports:
//!       - kind: dev.mcpg.transport.ngrok
//!         config:
//!           authtoken: ${env.NGROK_AUTHTOKEN}
//!           domain: tools.example.ngrok.app      # optional: your reserved domain
//!           traffic_policy: |                    # optional: an ngrok Traffic Policy
//!             on_http_request:
//!               - expressions: ["!('com.anthropic.api' in conn.client_ip.categories)"]
//!                 actions: [{ type: deny, config: { status_code: 403 } }]
//! ```

use std::time::Duration;

use serde::Deserialize;

/// The largest request body the plugin reads from a client. The gateway
/// refuses larger ones too.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;

/// How long the plugin waits for the ngrok session and endpoint.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NgrokConfig {
    /// The ngrok authtoken. Source it with `${env.NGROK_AUTHTOKEN}`.
    pub authtoken: String,
    /// The endpoint's domain, e.g. a reserved `tools.example.ngrok.app` or,
    /// with `binding: internal`, `tools.internal`. Unset: ngrok assigns the
    /// account's dev domain.
    #[serde(default)]
    pub domain: Option<String>,
    /// An ngrok Traffic Policy that runs at ngrok's edge before a request
    /// reaches the gateway: a YAML or JSON string, or the policy as a map.
    #[serde(default, deserialize_with = "policy_text")]
    pub traffic_policy: Option<String>,
    /// `public` (default at ngrok), `internal` (reachable only through a
    /// cloud endpoint's `forward-internal`) or `kubernetes`.
    #[serde(default)]
    pub binding: Option<String>,
    /// Let several gateways serve the same domain; ngrok balances between
    /// them.
    #[serde(default)]
    pub pooling: bool,
    /// Opaque endpoint metadata shown in the ngrok dashboard and API.
    #[serde(default)]
    pub metadata: Option<String>,
    /// The `forwards_to` label shown in the ngrok dashboard.
    #[serde(default)]
    pub forwards_to: Option<String>,
    /// Source CIDRs ngrok's edge lets through; all others are refused.
    #[serde(default)]
    pub allow_cidrs: Vec<String>,
    /// Source CIDRs ngrok's edge refuses.
    #[serde(default)]
    pub deny_cidrs: Vec<String>,
    /// The ngrok session server (`host:port`). Unset: ngrok's default.
    #[serde(default)]
    pub server_addr: Option<String>,
    /// Seconds to wait for the session and the endpoint.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// The largest request body read from a client; larger ones get 413.
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
}

/// A policy string as written, or a policy map as its JSON text (ngrok
/// reads both).
fn policy_text<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => Ok(Some(text)),
        Some(map @ serde_json::Value::Object(_)) => Ok(Some(map.to_string())),
        Some(_) => Err(serde::de::Error::custom(
            "`traffic_policy` is a YAML/JSON string or a map",
        )),
    }
}

fn default_connect_timeout_secs() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECS
}

fn default_max_request_body_bytes() -> usize {
    DEFAULT_MAX_REQUEST_BODY_BYTES
}

impl std::fmt::Debug for NgrokConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NgrokConfig")
            .field("authtoken", &"<redacted>")
            .field("domain", &self.domain)
            .field("traffic_policy", &self.traffic_policy.is_some())
            .field("binding", &self.binding)
            .field("pooling", &self.pooling)
            .field("server_addr", &self.server_addr)
            .finish_non_exhaustive()
    }
}

const BINDINGS: [&str; 3] = ["public", "internal", "kubernetes"];

impl NgrokConfig {
    /// Parse and check a config value.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        let cfg: Self =
            serde_json::from_value(value.clone()).map_err(|e| format!("ngrok config: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), String> {
        if self.authtoken.trim().is_empty() {
            return Err(
                "ngrok config: `authtoken` is empty (set it from ${env.NGROK_AUTHTOKEN})".into(),
            );
        }
        if self.authtoken.contains("${") {
            return Err(
                "ngrok config: `authtoken` still holds a `${...}` reference; the variable is not set"
                    .into(),
            );
        }
        if let Some(binding) = &self.binding
            && !BINDINGS.contains(&binding.as_str())
        {
            return Err(format!(
                "ngrok config: `binding` is {binding:?}; use one of public, internal, kubernetes"
            ));
        }
        if let Some(domain) = &self.domain
            && (domain.is_empty() || domain.contains("://") || domain.contains('/'))
        {
            return Err(format!(
                "ngrok config: `domain` is {domain:?}; give a host name only, e.g. tools.example.ngrok.app"
            ));
        }
        if self.connect_timeout_secs == 0 {
            return Err("ngrok config: `connect_timeout_secs` must be at least 1".into());
        }
        if self.max_request_body_bytes == 0 {
            return Err("ngrok config: `max_request_body_bytes` must be at least 1".into());
        }
        Ok(())
    }

    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_minimal_config_takes_the_defaults() {
        let cfg = NgrokConfig::from_value(&json!({"authtoken": "tok"})).unwrap();
        assert_eq!(cfg.connect_timeout(), Duration::from_secs(30));
        assert_eq!(cfg.max_request_body_bytes, DEFAULT_MAX_REQUEST_BODY_BYTES);
        assert!(!cfg.pooling);
        assert!(cfg.domain.is_none());
    }

    #[test]
    fn the_debug_form_hides_the_authtoken() {
        let cfg = NgrokConfig::from_value(&json!({"authtoken": "s3cr3t-token"})).unwrap();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("s3cr3t-token"), "{shown}");
        assert!(shown.contains("<redacted>"));
    }

    #[test]
    fn bad_values_are_refused_with_the_field_name() {
        for (value, field) in [
            (json!({}), "authtoken"),
            (json!({"authtoken": " "}), "authtoken"),
            (json!({"authtoken": "${env.NGROK_AUTHTOKEN}"}), "authtoken"),
            (
                json!({"authtoken": "t", "binding": "everywhere"}),
                "binding",
            ),
            (
                json!({"authtoken": "t", "domain": "https://x.ngrok.app"}),
                "domain",
            ),
            (
                json!({"authtoken": "t", "connect_timeout_secs": 0}),
                "connect_timeout_secs",
            ),
            (
                json!({"authtoken": "t", "max_request_body_bytes": 0}),
                "max_request_body_bytes",
            ),
            (json!({"authtoken": "t", "unknown": 1}), "unknown"),
        ] {
            let err = NgrokConfig::from_value(&value).unwrap_err();
            assert!(err.contains(field), "{value}: {err}");
        }
    }

    #[test]
    fn a_traffic_policy_is_a_string_or_a_map() {
        let text = "on_http_request: []";
        let cfg =
            NgrokConfig::from_value(&json!({"authtoken": "t", "traffic_policy": text})).unwrap();
        assert_eq!(cfg.traffic_policy.as_deref(), Some(text));
        let map = json!({"on_http_request": [{"actions": [{"type": "deny"}]}]});
        let cfg =
            NgrokConfig::from_value(&json!({"authtoken": "t", "traffic_policy": map})).unwrap();
        let back: serde_json::Value =
            serde_json::from_str(cfg.traffic_policy.as_deref().unwrap()).unwrap();
        assert_eq!(back, map);
        let err =
            NgrokConfig::from_value(&json!({"authtoken": "t", "traffic_policy": 7})).unwrap_err();
        assert!(err.contains("traffic_policy"), "{err}");
    }

    #[test]
    fn every_ngrok_binding_name_is_accepted() {
        for binding in BINDINGS {
            NgrokConfig::from_value(&json!({"authtoken": "t", "binding": binding})).unwrap();
        }
    }
}
