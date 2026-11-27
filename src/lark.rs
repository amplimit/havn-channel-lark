//! Lark / Feishu Open API client — the platform-specific half of the
//! adapter (the Telegram adapter's `telegram.rs` analogue).
//!
//! Responsibilities:
//! - Mint and proactively refresh a `tenant_access_token` from
//!   `app_id` + `app_secret` (valid 7200s; we refresh well before).
//! - Send / reply text + image + file messages via the IM v1 API.
//! - Download message resources (`file_key`) for voice STT.
//! - Upload images/files for native outbound delivery.
//!
//! Also defines the inbound event structs (`im.message.receive_v1`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Refresh the token this long before its stated expiry (default 7200s),
/// leaving a safety margin so an in-flight request never races expiry.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_mins(30);
/// On a refresh failure, retry this soon rather than waiting a full cycle.
const TOKEN_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Lark's answer to a call past its frequency limit, with HTTP 429 (or 400
/// from some older APIs), and `x-ogw-ratelimit-reset`, the seconds to wait
/// (Open Platform, api-call-guide/frequency-control).
const FREQUENCY_LIMIT: i64 = 99_991_400;
/// The send and reply APIs' own: "This operation triggers the frequency
/// limit" (5 messages a second to one user or group; im-v1/message/create).
const MESSAGE_FREQUENCY_LIMIT: i64 = 230_020;
/// Times a message is sent again when Lark says to slow down.
const SLOW_DOWN_TRIES: u32 = 4;
/// The longest wait for one try, whatever Lark asks: the chat waits, and
/// no other.
const SLOW_DOWN_FOR: Duration = Duration::from_secs(30);

pub struct LarkClient {
    http: reqwest::Client,
    base_url: String,
    app_id: String,
    app_secret: String,
    /// The live `tenant_access_token`. Read per request; replaced by the
    /// background refresher (and by a one-shot retry on a 401).
    token: RwLock<String>,
}

impl LarkClient {
    /// Build the client and fetch an initial token, so the daemon starts
    /// authenticated (and fails fast if the credentials are wrong). Returns
    /// the client and the initial token's validity (seconds) for the refresher.
    pub async fn new(
        base_url: String,
        app_id: String,
        app_secret: String,
    ) -> Result<(Arc<Self>, u64)> {
        let client = Self::unauthenticated(base_url, app_id, app_secret)?;
        let expire = client
            .refresh_token()
            .await
            .context("initial tenant_access_token")?;
        info!(expire_s = expire, "Lark tenant_access_token acquired");
        Ok((client, expire))
    }

    /// The client before it has a token.
    pub fn unauthenticated(
        base_url: String,
        app_id: String,
        app_secret: String,
    ) -> Result<Arc<Self>> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("building Lark HTTP client")?;
        Ok(Arc::new(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            app_id,
            app_secret,
            token: RwLock::new(String::new()),
        }))
    }

    /// Download arbitrary bytes from a URL (no Lark auth) — used to fetch an
    /// agent-supplied image/file URL before re-uploading it to Lark.
    pub async fn fetch_url(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("GET outbound url")?;
        if !resp.status().is_success() {
            bail!("fetch {url} -> HTTP {}", resp.status());
        }
        Ok(resp.bytes().await.context("read url bytes")?.to_vec())
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Fetch a fresh `tenant_access_token` and store it. Returns the
    /// stated validity (seconds) so the refresher can schedule the next run.
    pub async fn refresh_token(&self) -> Result<u64> {
        #[derive(Deserialize)]
        struct TokenResp {
            code: i64,
            msg: String,
            #[serde(default)]
            tenant_access_token: String,
            #[serde(default)]
            expire: u64,
        }
        let resp: TokenResp = self
            .http
            .post(self.url("/open-apis/auth/v3/tenant_access_token/internal"))
            .json(&json!({ "app_id": self.app_id, "app_secret": self.app_secret }))
            .send()
            .await
            .context("POST tenant_access_token/internal")?
            .json()
            .await
            .context("decode tenant_access_token response")?;
        if resp.code != 0 || resp.tenant_access_token.is_empty() {
            bail!(
                "tenant_access_token failed: code={} msg={}",
                resp.code,
                resp.msg
            );
        }
        *self.token.write().await = resp.tenant_access_token;
        Ok(resp.expire)
    }

    async fn bearer(&self) -> String {
        format!("Bearer {}", self.token.read().await)
    }

    /// Send a message to a recipient. `receive_id_type` is e.g. `open_id`
    /// or `chat_id`; `content` is the already-serialized JSON string Lark
    /// expects (e.g. `{"text":"hi"}`). Returns the new `message_id`.
    pub async fn send_message(
        &self,
        receive_id_type: &str,
        receive_id: &str,
        msg_type: &str,
        content: &str,
    ) -> Result<String> {
        let body = json!({
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": content,
        });
        let req = self
            .http
            .post(self.url("/open-apis/im/v1/messages"))
            .query(&[("receive_id_type", receive_id_type)])
            .json(&body);
        self.send_message_req(req).await
    }

    /// Reply to an existing message (threads under it). Returns the new id.
    pub async fn reply_message(
        &self,
        message_id: &str,
        msg_type: &str,
        content: &str,
    ) -> Result<String> {
        let body = json!({ "msg_type": msg_type, "content": content });
        let req = self
            .http
            .post(self.url(&format!("/open-apis/im/v1/messages/{message_id}/reply")))
            .json(&body);
        self.send_message_req(req).await
    }

    /// Execute a send/reply request: the token refreshed once when Lark
    /// says it expired, and sent again after the wait Lark asks for when it
    /// says to slow down ([`slowed_down`]), up to [`SLOW_DOWN_TRIES`] times.
    async fn send_message_req(&self, req: reqwest::RequestBuilder) -> Result<String> {
        #[derive(Deserialize)]
        struct SendResp {
            code: i64,
            #[serde(default)]
            msg: String,
            #[serde(default)]
            data: Option<SendData>,
        }
        #[derive(Deserialize)]
        struct SendData {
            message_id: String,
        }
        let mut refreshed = false;
        let mut slowed = 0u32;
        loop {
            let attempt = req
                .try_clone()
                .context("a send request that cannot be sent again")?;
            let resp = attempt
                .header("Authorization", self.bearer().await)
                .send()
                .await
                .context("POST im/v1 message")?;
            let status = resp.status();
            let reset = resp
                .headers()
                .get("x-ogw-ratelimit-reset")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok());
            let body = resp.text().await.context("read send response")?;
            let parsed: SendResp = match serde_json::from_str(&body) {
                Ok(p) => p,
                // A 429 may come without Lark's body.
                Err(_) if status == reqwest::StatusCode::TOO_MANY_REQUESTS => SendResp {
                    code: FREQUENCY_LIMIT,
                    msg: String::new(),
                    data: None,
                },
                Err(e) => {
                    return Err(e).with_context(|| format!("decode send response (HTTP {status})"));
                }
            };
            // Lark usually returns HTTP 200 with code 99991663 (token
            // expired) / 99991664 (token invalid) rather than a real 401.
            if !refreshed
                && (status == reqwest::StatusCode::UNAUTHORIZED
                    || matches!(parsed.code, 99_991_663 | 99_991_664))
            {
                warn!("Lark send got auth failure; refreshing token and retrying");
                self.refresh_token().await.context("token refresh on 401")?;
                refreshed = true;
                continue;
            }
            if let Some(wait) = slowed_down(status, parsed.code, reset, slowed) {
                warn!(
                    code = parsed.code,
                    wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                    "Lark asks to slow down; sending again after the wait"
                );
                tokio::time::sleep(wait).await;
                slowed += 1;
                continue;
            }
            if parsed.code != 0 {
                bail!("Lark send failed: code={} msg={}", parsed.code, parsed.msg);
            }
            return parsed
                .data
                .map(|d| d.message_id)
                .context("send response missing message_id");
        }
    }

    /// Download a message resource (`file_key`) — used for voice STT and
    /// for inspecting inbound images/files. `kind` is `file` or `image`.
    pub async fn download_resource(
        &self,
        message_id: &str,
        file_key: &str,
        kind: &str,
    ) -> Result<Vec<u8>> {
        let resp = self
            .http
            .get(self.url(&format!(
                "/open-apis/im/v1/messages/{message_id}/resources/{file_key}"
            )))
            .query(&[("type", kind)])
            .header("Authorization", self.bearer().await)
            .send()
            .await
            .context("GET message resource")?;
        if !resp.status().is_success() {
            bail!("resource download HTTP {}", resp.status());
        }
        Ok(resp.bytes().await.context("read resource bytes")?.to_vec())
    }

    /// Upload an image, returning its `image_key` for an outbound image
    /// message.
    pub async fn upload_image(&self, bytes: Vec<u8>) -> Result<String> {
        #[derive(Deserialize)]
        struct Resp {
            code: i64,
            msg: String,
            #[serde(default)]
            data: Option<ImageData>,
        }
        #[derive(Deserialize)]
        struct ImageData {
            image_key: String,
        }
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part(
                "image",
                reqwest::multipart::Part::bytes(bytes).file_name("image"),
            );
        let parsed: Resp = self
            .http
            .post(self.url("/open-apis/im/v1/images"))
            .header("Authorization", self.bearer().await)
            .multipart(form)
            .send()
            .await
            .context("POST im/v1/images")?
            .json()
            .await
            .context("decode image upload response")?;
        if parsed.code != 0 {
            bail!(
                "image upload failed: code={} msg={}",
                parsed.code,
                parsed.msg
            );
        }
        parsed
            .data
            .map(|d| d.image_key)
            .context("image upload missing image_key")
    }

    /// Upload a generic file, returning its `file_key`.
    pub async fn upload_file(&self, bytes: Vec<u8>, file_name: &str) -> Result<String> {
        #[derive(Deserialize)]
        struct Resp {
            code: i64,
            msg: String,
            #[serde(default)]
            data: Option<FileData>,
        }
        #[derive(Deserialize)]
        struct FileData {
            file_key: String,
        }
        let form = reqwest::multipart::Form::new()
            .text("file_type", "stream")
            .text("file_name", file_name.to_string())
            .part(
                "file",
                reqwest::multipart::Part::bytes(bytes).file_name(file_name.to_string()),
            );
        let parsed: Resp = self
            .http
            .post(self.url("/open-apis/im/v1/files"))
            .header("Authorization", self.bearer().await)
            .multipart(form)
            .send()
            .await
            .context("POST im/v1/files")?
            .json()
            .await
            .context("decode file upload response")?;
        if parsed.code != 0 {
            bail!(
                "file upload failed: code={} msg={}",
                parsed.code,
                parsed.msg
            );
        }
        parsed
            .data
            .map(|d| d.file_key)
            .context("file upload missing file_key")
    }
}

/// Spawn the proactive token refresher. Refreshes `TOKEN_REFRESH_MARGIN`
/// before each token's stated expiry; on failure retries soon. Runs for
/// the process lifetime.
pub fn spawn_token_refresher(client: Arc<LarkClient>, initial_expire_s: u64) {
    tokio::spawn(async move {
        let mut next = sleep_for(initial_expire_s);
        loop {
            tokio::time::sleep(next).await;
            match client.refresh_token().await {
                Ok(expire) => {
                    debug!(expire_s = expire, "refreshed Lark tenant_access_token");
                    next = sleep_for(expire);
                }
                Err(e) => {
                    warn!(error = %e, "Lark token refresh failed; retrying soon");
                    next = TOKEN_RETRY_BACKOFF;
                }
            }
        }
    });
}

/// How long to wait before sending again, when Lark said to slow down
/// (HTTP 429, or one of its frequency-limit codes) and the message was sent
/// again `tries` times so far: the seconds Lark asks for, else 1, 2, 4, ...;
/// `None` when it did not say so, or after [`SLOW_DOWN_TRIES`].
fn slowed_down(
    status: reqwest::StatusCode,
    code: i64,
    reset_s: Option<u64>,
    tries: u32,
) -> Option<Duration> {
    let limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || matches!(code, FREQUENCY_LIMIT | MESSAGE_FREQUENCY_LIMIT);
    if !limited || tries >= SLOW_DOWN_TRIES {
        return None;
    }
    let wait = reset_s.map_or_else(
        || Duration::from_secs(1u64 << tries.min(5)),
        |s| Duration::from_secs(s.max(1)),
    );
    Some(wait.min(SLOW_DOWN_FOR))
}

fn sleep_for(expire_s: u64) -> Duration {
    Duration::from_secs(expire_s)
        .checked_sub(TOKEN_REFRESH_MARGIN)
        .filter(|d| !d.is_zero())
        .unwrap_or(TOKEN_RETRY_BACKOFF)
}

// ─────────────────────────── inbound event types ───────────────────────────

/// Decrypted event envelope (event schema 2.0). We only act on
/// `im.message.receive_v1`; other event types are accepted and ignored.
/// Unused wire fields (`schema`, `app_id`, `tenant_key`, …) are dropped at
/// deserialize — the full payload is preserved in `ChannelInbound.raw`.
#[derive(Debug, Deserialize)]
pub struct EventEnvelope {
    pub header: EventHeader,
    pub event: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct EventHeader {
    /// The same for each time Lark sends the event again.
    #[serde(default)]
    pub event_id: String,
    pub event_type: String,
    /// The app's Verification Token.
    #[serde(default)]
    pub token: String,
}

/// `im.message.receive_v1` `event` body.
#[derive(Debug, Deserialize)]
pub struct MessageReceiveEvent {
    pub sender: Sender,
    pub message: LarkMessage,
}

#[derive(Debug, Deserialize)]
pub struct Sender {
    pub sender_id: SenderId,
}

/// The sender's ids (havn issue #262): `open_id` is the person in this
/// app only; `union_id` the same person in every app of the same
/// developer, which for an app made in a tenant (a custom app) is every
/// custom app of the tenant.
#[derive(Debug, Deserialize)]
pub struct SenderId {
    pub open_id: String,
    #[serde(default)]
    pub union_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LarkMessage {
    pub message_id: String,
    pub chat_id: String,
    pub chat_type: String,
    pub message_type: String,
    /// JSON-serialized string; shape depends on `message_type`.
    pub content: String,
    #[serde(default)]
    pub create_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_lark_asks_to_slow_down_for_is_sent_again_after_the_wait() {
        let ok = reqwest::StatusCode::OK;
        let many = reqwest::StatusCode::TOO_MANY_REQUESTS;
        let bad = reqwest::StatusCode::BAD_REQUEST;
        // The seconds Lark asks for, at most SLOW_DOWN_FOR.
        assert_eq!(
            slowed_down(many, FREQUENCY_LIMIT, Some(3), 0),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            slowed_down(many, FREQUENCY_LIMIT, Some(500), 0),
            Some(SLOW_DOWN_FOR)
        );
        assert_eq!(
            slowed_down(many, 0, Some(0), 0),
            Some(Duration::from_secs(1))
        );
        // The codes alone, as with an older API's HTTP 400; else 1, 2, 4 s.
        assert_eq!(
            slowed_down(bad, MESSAGE_FREQUENCY_LIMIT, None, 0),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            slowed_down(bad, FREQUENCY_LIMIT, None, 2),
            Some(Duration::from_secs(4))
        );
        // Not a limit, or tried enough.
        assert_eq!(slowed_down(ok, 0, None, 0), None);
        assert_eq!(slowed_down(bad, 230_013, Some(1), 0), None);
        assert_eq!(
            slowed_down(many, FREQUENCY_LIMIT, Some(1), SLOW_DOWN_TRIES),
            None
        );
    }
}
