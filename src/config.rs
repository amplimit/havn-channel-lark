//! Operator-supplied configuration — loaded from a TOML file at
//! startup. Mirrors the values the operator put in havn's
//! `[[channels.lark.accounts]]` block: same `account_id` on both
//! sides, same `adapter_token` (havn stores it age-encrypted, this
//! daemon stores it in plain TOML — keep the file readable only by
//! the daemon's user).

use std::path::Path;

use anyhow::{Context as _, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Lark/Feishu app id (`cli_...`), from the Developer Console
    /// Credentials page. Held only in this daemon — havn never sees it.
    pub app_id: String,

    /// Lark/Feishu app secret, paired with `app_id` to mint a
    /// `tenant_access_token`. Held only in this daemon.
    pub app_secret: String,

    /// Lark Open API base URL. `https://open.larksuite.com` for Lark
    /// International (default), `https://open.feishu.cn` for Feishu
    /// (China). Both speak the identical API; only the host differs.
    #[serde(default = "default_lark_base_url")]
    pub lark_base_url: String,

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

    /// Address the webhook HTTP server binds to, receiving Lark event
    /// callbacks (`im.message.receive_v1`). The operator points Lark's
    /// "Request URL" at `http(s)://<public-host>/webhook` and reverse-
    /// proxies it to this address. Lark International has no WebSocket
    /// long-connection, so webhook is the only inbound path.
    #[serde(default = "default_webhook_listen")]
    pub webhook_listen: String,

    /// Lark event "Verification Token". Every event Lark pushes carries
    /// it in `header.token` (encrypted mode) or top-level `token`
    /// (plaintext mode); the daemon rejects events whose token doesn't
    /// match. Required.
    pub verification_token: String,

    /// Lark event "Encrypt Key". When set on the Developer Console,
    /// Lark AES-encrypts every event body; the daemon decrypts with it.
    /// Leave unset only if encryption is disabled on the console (not
    /// recommended).
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
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {} as TOML", path.display()))?;
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
            ("verification_token", &self.verification_token),
        ] {
            if value.is_empty() {
                anyhow::bail!("{label} must not be empty");
            }
        }
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
        assert!(c.encrypt_key.is_none());
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
