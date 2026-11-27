//! Operator-supplied configuration — loaded from a TOML file at
//! startup. Mirrors the values the operator put in havn's
//! `[[channels.lark.accounts]]` block: same `account_id` on both
//! sides, same `adapter_token` (havn stores it age-encrypted, this
//! daemon stores it in plain TOML — keep the file readable only by
//! the daemon's user).

use std::path::Path;

use anyhow::{Context as _, Result};
use serde::Deserialize;

/// How Lark sends the app's events to the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Events {
    /// Over a WebSocket the daemon opens to Lark: no public URL needed.
    #[default]
    LongConnection,
    /// As HTTP requests to `POST /webhook` on `webhook_listen`.
    Webhook,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    /// Lark/Feishu app id (`cli_...`), from the Developer Console
    /// Credentials page. Held only in this daemon — havn never sees it.
    pub app_id: String,

    /// Lark/Feishu app secret, paired with `app_id` to mint a
    /// `tenant_access_token`. Held only in this daemon.
    pub app_secret: String,

    /// Lark Open API base URL. `https://open.larksuite.com` for Lark
    /// International (default), `https://open.feishu.cn` for Feishu
    /// (China). Both speak the identical API, long connection included;
    /// only the host differs.
    #[serde(default = "default_lark_base_url")]
    pub lark_base_url: String,

    /// How the app's events reach the daemon: `long_connection` (the
    /// default), a WebSocket the daemon opens to Lark, or `webhook`, HTTP
    /// requests to `webhook_listen`. It matches the subscription mode set
    /// on the Developer Console.
    #[serde(default)]
    pub events: Events,

    /// havn gateway WebSocket URL. e.g. `ws://127.0.0.1:8100` or
    /// `wss://havn.example.com`. The daemon appends
    /// `/api/v1/channel?channel=<channel_id>&account=<account_id>`.
    pub havn_gateway: String,

    /// Operator-generated random string. MUST match the value the
    /// operator stored in havn's credentials table under
    /// `(provider = "channel:<channel_id>", name = "<account_id>")`.
    /// havn compares constant-time at WS upgrade.
    pub adapter_token: String,

    /// Channel id matching havn's `[[channels.<channel_id>.accounts]]`
    /// table key. Defaults to `"lark"`.
    #[serde(default = "default_channel_id")]
    pub channel_id: String,

    /// Operator-supplied stable account handle, matching havn's
    /// `[[channels.<channel_id>.accounts]] id` value. e.g.
    /// `"acme-lark-bot"`.
    pub account_id: String,

    /// With `events = "webhook"`: the address the webhook HTTP server
    /// binds to. The operator points Lark's "Request URL" at
    /// `http(s)://<public-host>/webhook` and reverse-proxies it here.
    #[serde(default = "default_webhook_listen")]
    pub webhook_listen: String,

    /// With `events = "webhook"`, required: Lark's event "Verification
    /// Token". Every event carries it in `header.token` (or a top-level
    /// `token`); the daemon refuses events whose token does not match.
    #[serde(default)]
    pub verification_token: Option<String>,

    /// Lark's event "Encrypt Key", when one is set on the Developer
    /// Console: Lark then encrypts every event, and signs each webhook
    /// request with it; the daemon decrypts with it, and refuses a webhook
    /// event whose signature does not match.
    #[serde(default)]
    pub encrypt_key: Option<String>,

    /// OpenAI-compatible API key for Whisper STT of voice messages.
    /// Optional — without it, inbound audio is dropped with a warn.
    /// The key never leaves this daemon; havn never sees it.
    #[serde(default)]
    pub openai_api_key: Option<String>,

    /// Override the Whisper endpoint (default `https://api.openai.com/v1`).
    /// Groq's `https://api.groq.com/openai/v1` is the documented free-tier
    /// path.
    #[serde(default)]
    pub whisper_base_url: Option<String>,

    /// Override the Whisper model name (default `whisper-1`). Groq uses
    /// `whisper-large-v3-turbo`.
    #[serde(default)]
    pub whisper_model: Option<String>,
}

// The secrets are not shown, in a log or a panic.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("app_id", &self.app_id)
            .field("lark_base_url", &self.lark_base_url)
            .field("events", &self.events)
            .field("havn_gateway", &self.havn_gateway)
            .field("channel_id", &self.channel_id)
            .field("account_id", &self.account_id)
            .field("webhook_listen", &self.webhook_listen)
            .finish_non_exhaustive()
    }
}

fn default_lark_base_url() -> String {
    "https://open.larksuite.com".into()
}

fn default_channel_id() -> String {
    "lark".into()
}

fn default_webhook_listen() -> String {
    "0.0.0.0:8090".into()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        // The parser's message only: its excerpt of the file may show a secret.
        let cfg: Self = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("parsing {} as TOML: {}", path.display(), e.message()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("app_id", &self.app_id),
            ("app_secret", &self.app_secret),
            ("havn_gateway", &self.havn_gateway),
            ("adapter_token", &self.adapter_token),
            ("channel_id", &self.channel_id),
            ("account_id", &self.account_id),
            ("lark_base_url", &self.lark_base_url),
            ("webhook_listen", &self.webhook_listen),
        ] {
            if value.is_empty() {
                anyhow::bail!("{label} must not be empty");
            }
        }
        if self.events == Events::Webhook
            && self.verification_token.as_deref().is_none_or(str::is_empty)
        {
            anyhow::bail!("events = \"webhook\" needs verification_token");
        }
        url::Url::parse(&self.lark_base_url)
            .with_context(|| format!("lark_base_url {:?} is not a URL", self.lark_base_url))?;
        // Defence in depth: havn's channel_id_codec rejects the `|`
        // separator in any field that round-trips through channel_id.
        for (label, value) in [
            ("channel_id", &self.channel_id),
            ("account_id", &self.account_id),
        ] {
            if value.contains('|') {
                anyhow::bail!(
                    "{label} must not contain `|` (channel_id codec separator); got {value:?}"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(contents: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "havn-channel-lark-test-{}.toml",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        p
    }

    #[test]
    fn loads_minimal_config_with_defaults() {
        let p = write_temp(
            r#"
            app_id = "cli_abc"
            app_secret = "sec"
            havn_gateway = "ws://127.0.0.1:8100"
            adapter_token = "tok"
            account_id = "acme-lark-bot"
            verification_token = "vtok"
            "#,
        );
        let c = Config::load(&p).unwrap();
        assert_eq!(c.account_id, "acme-lark-bot");
        assert_eq!(c.channel_id, "lark"); // default
        assert_eq!(c.lark_base_url, "https://open.larksuite.com"); // default
        assert_eq!(c.webhook_listen, "0.0.0.0:8090"); // default
        assert_eq!(c.events, Events::LongConnection); // default
        assert!(c.encrypt_key.is_none());
    }

    #[test]
    fn a_long_connection_needs_no_verification_token_and_a_webhook_does() {
        let base = r#"
            app_id = "cli_abc"
            app_secret = "sec"
            havn_gateway = "ws://h"
            adapter_token = "tok"
            account_id = "bot"
            "#;
        let c = Config::load(&write_temp(base)).unwrap();
        assert!(c.verification_token.is_none());
        assert!(Config::load(&write_temp(&format!("{base}\nevents = \"webhook\"\n"))).is_err());
        let c = Config::load(&write_temp(&format!(
            "{base}\nevents = \"webhook\"\nverification_token = \"vtok\"\n"
        )))
        .unwrap();
        assert_eq!(c.events, Events::Webhook);
        assert!(Config::load(&write_temp(&format!("{base}\nevents = \"sse\"\n"))).is_err());
    }

    #[test]
    fn the_secrets_are_not_shown() {
        let c = Config::load(&write_temp(
            r#"
            app_id = "cli_abc"
            app_secret = "app-secret-value"
            havn_gateway = "ws://h"
            adapter_token = "adapter-token-value"
            account_id = "bot"
            encrypt_key = "encrypt-key-value"
            "#,
        ))
        .unwrap();
        let shown = format!("{c:?}");
        assert!(!shown.contains("secret-value") && !shown.contains("token-value"));
        assert!(!shown.contains("key-value"));
    }

    #[test]
    fn feishu_base_url_override() {
        let p = write_temp(
            r#"
            app_id = "cli_abc"
            app_secret = "sec"
            lark_base_url = "https://open.feishu.cn"
            havn_gateway = "ws://h"
            adapter_token = "tok"
            account_id = "bot"
            verification_token = "vtok"
            "#,
        );
        let c = Config::load(&p).unwrap();
        assert_eq!(c.lark_base_url, "https://open.feishu.cn");
    }

    #[test]
    fn rejects_pipe_in_account_id() {
        let p = write_temp(
            r#"
            app_id = "cli_abc"
            app_secret = "sec"
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice|bot"
            verification_token = "vtok"
            "#,
        );
        assert!(Config::load(&p).is_err());
    }

    #[test]
    fn rejects_empty_required_field() {
        let p = write_temp(
            r#"
            app_id = ""
            app_secret = "sec"
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice"
            verification_token = "vtok"
            "#,
        );
        assert!(Config::load(&p).is_err());
    }
}
