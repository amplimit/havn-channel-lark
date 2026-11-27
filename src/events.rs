//! Lark's events, as they come by either transport: the long connection
//! ([`crate::longconn`]) or the webhook, whose HTTP server is here.
//!
//! Both hand an event's JSON to [`Incoming::take`], which:
//! - decrypts it when it comes encrypted (the app has an Encrypt Key),
//! - checks the Verification Token, when one is configured,
//! - drops one already taken, by `event_id` and by `message_id`, so Lark's
//!   retries make no second turn,
//! - translates each inbound message into a havn `ChannelInbound` and
//!   forwards it over the gateway WS, without the payload's Verification
//!   Token and tenant key.
//!
//! The webhook also answers the one-time `url_verification` challenge and,
//! with an Encrypt Key, refuses a request whose signature does not match or
//! is older than [`SIGNED_FOR`].
//!
//! Lark wants an answer within 3 seconds, so translation (which may do a
//! resource download + STT) runs in a worker of the message's chat, which
//! takes the chat's messages in the order they came; the event is answered
//! at once.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use aes::Aes256;
use aes::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use anyhow::{Context as _, Result, anyhow, bail};
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::lark::{EventEnvelope, LarkClient, MessageReceiveEvent, SenderId};
use crate::types::{ChannelFrame, ChannelInbound, ChannelMessageContent};
use crate::whisper::Whisper;

type Aes256CbcDec = cbc::Decryptor<Aes256>;

/// The prefix of the sender ids this adapter gives.
pub const PREFIX: &str = "lark:";

/// Events and messages remembered as taken, each; past that, the oldest
/// are let go. Lark retries an event it got no answer to for up to a few
/// hours, far fewer than this many events.
const SEEN_KEPT: usize = 4096;

/// Messages of a chat waiting for its worker at most; past that, a new one
/// is dropped.
const CHAT_WAITING: usize = 64;
/// A chat's worker with nothing to do for this long ends; the next message
/// starts another.
const CHAT_IDLE: std::time::Duration = std::time::Duration::from_secs(240);
/// A signed webhook request older or newer than this is refused, so one
/// seen on the way cannot be sent again later.
pub const SIGNED_FOR: std::time::Duration = std::time::Duration::from_secs(300);

/// The workers of the chats messages came in, by chat: each takes its
/// chat's messages in order.
pub type Chats = Arc<Mutex<HashMap<String, mpsc::Sender<(MessageReceiveEvent, Value)>>>>;

/// What takes Lark's events, from either transport.
#[derive(Clone)]
pub struct Incoming {
    pub lark: Arc<LarkClient>,
    pub whisper: Option<Whisper>,
    pub outbound_tx: mpsc::Sender<ChannelFrame>,
    pub account_id: String,
    pub seq: Arc<AtomicU64>,
    pub seen: Arc<Mutex<Seen>>,
    pub verification_token: Option<String>,
    pub encrypt_key: Option<String>,
    pub chats: Chats,
}

/// What became of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Taken {
    /// Taken, or one not acted on (another kind, or taken before).
    Done,
    /// Not Lark's: its Verification Token does not match.
    Refused,
    /// Not readable: not JSON, or encrypted with another key.
    Unreadable,
}

/// Keys remembered up to [`SEEN_KEPT`] each, the oldest let go first.
#[derive(Debug, Default)]
pub struct Seen {
    events: Bounded,
    messages: Bounded,
}

#[derive(Debug, Default)]
struct Bounded {
    keys: HashSet<String>,
    order: VecDeque<String>,
}

impl Bounded {
    /// Whether `key` is new; it is remembered either way.
    fn first(&mut self, key: &str) -> bool {
        if self.keys.contains(key) {
            return false;
        }
        self.keys.insert(key.to_string());
        self.order.push_back(key.to_string());
        while self.order.len() > SEEN_KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.keys.remove(&oldest);
            }
        }
        true
    }
}

impl Seen {
    /// Whether the event `event_id` is new.
    pub fn first_event(&mut self, event_id: &str) -> bool {
        event_id.is_empty() || self.events.first(event_id)
    }

    /// Whether the message `message_id` is new.
    pub fn first_message(&mut self, message_id: &str) -> bool {
        self.messages.first(message_id)
    }
}

impl Incoming {
    fn seen(&self) -> std::sync::MutexGuard<'_, Seen> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Take one event, as Lark sent it (encrypted or not).
    pub fn take(&self, raw: Value) -> Taken {
        let payload = match self.open(raw) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "lark: an event could not be read");
                return Taken::Unreadable;
            }
        };
        self.take_open(payload)
    }

    /// `raw` decrypted when it is encrypted.
    fn open(&self, raw: Value) -> Result<Value> {
        match raw.get("encrypt").and_then(Value::as_str) {
            Some(enc) => {
                let key = self.encrypt_key.as_deref().context(
                    "it is encrypted, and no encrypt_key is configured: set the app's Encrypt Key",
                )?;
                decrypt(key, enc)
            }
            None => Ok(raw),
        }
    }

    fn take_open(&self, payload: Value) -> Taken {
        // Event schema 2.0. One of 1.0 carries no header: not acted on.
        let env: EventEnvelope = match serde_json::from_value(payload.clone()) {
            Ok(e) => e,
            Err(e) => {
                debug!(error = %e, "lark: not a schema-2.0 event envelope; ignoring");
                return Taken::Done;
            }
        };
        if let Some(expected) = &self.verification_token
            && !same(env.header.token.as_bytes(), expected.as_bytes())
        {
            warn!("lark: event verification token mismatch");
            return Taken::Refused;
        }
        if !self.seen().first_event(&env.header.event_id) {
            debug!(event_id = %env.header.event_id, "lark: an event taken before; dropping");
            return Taken::Done;
        }
        if env.header.event_type != "im.message.receive_v1" {
            debug!(event_type = %env.header.event_type, "lark: ignoring non-message event");
            return Taken::Done;
        }
        let ev = match serde_json::from_value::<MessageReceiveEvent>(env.event) {
            Ok(ev) => ev,
            Err(e) => {
                warn!(error = %e, "lark: malformed im.message.receive_v1 event");
                return Taken::Done;
            }
        };
        // The same message under another event id is dropped too.
        if !self.seen().first_message(&ev.message.message_id) {
            debug!(message_id = %ev.message.message_id, "lark: a message taken before; dropping");
            return Taken::Done;
        }
        // Translate + forward off the answer's path (Lark wants it within
        // 3s; STT/download can take longer), in the chat's order.
        self.to_chat(ev, without_secrets(payload));
        Taken::Done
    }

    /// Hand `ev` to the worker of its chat, started when it has none.
    fn to_chat(&self, ev: MessageReceiveEvent, raw: Value) {
        let chat = ev.message.chat_id.clone();
        let mut chats = self
            .chats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tx = chats.entry(chat.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(CHAT_WAITING);
            tokio::spawn(chat_worker(self.clone(), chat.clone(), rx));
            tx
        });
        if tx.try_send((ev, raw)).is_err() {
            warn!(chat = %chat, "lark: a chat has as many messages waiting as it may; dropping one");
        }
    }
}

/// Translate and forward the messages of `chat` one at a time, in the order
/// they came. It ends after [`CHAT_IDLE`] with nothing to do, under the lock
/// [`Incoming::to_chat`] adds with, so no message is left behind.
async fn chat_worker(
    incoming: Incoming,
    chat: String,
    mut rx: mpsc::Receiver<(MessageReceiveEvent, Value)>,
) {
    loop {
        let next = match tokio::time::timeout(CHAT_IDLE, rx.recv()).await {
            Ok(Some(next)) => next,
            Ok(None) => return,
            Err(_) => {
                let mut chats = incoming
                    .chats
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Ok(next) = rx.try_recv() {
                    next
                } else {
                    chats.remove(&chat);
                    return;
                }
            }
        };
        let (ev, raw) = next;
        translate_and_forward(incoming.clone(), ev, raw).await;
    }
}

/// `payload` without what is not the agent's to see: the app's
/// Verification Token and the tenant's key.
fn without_secrets(mut payload: Value) -> Value {
    if let Some(header) = payload.get_mut("header").and_then(Value::as_object_mut) {
        header.remove("token");
        header.remove("tenant_key");
    }
    if let Some(sender) = payload
        .pointer_mut("/event/sender")
        .and_then(Value::as_object_mut)
    {
        sender.remove("tenant_key");
    }
    payload
}

/// Build the webhook router. The operator points Lark's "Request URL" at
/// `http(s)://<host>/webhook`.
pub fn router(incoming: Incoming) -> Router {
    Router::new()
        .route("/webhook", post(handle))
        .with_state(incoming)
}

async fn handle(
    State(incoming): State<Incoming>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // A signed request is checked before anything else is read of it.
    let signature = headers.contains_key("X-Lark-Signature");
    if let Some(key) = incoming.encrypt_key.as_deref()
        && signature
    {
        if !signed(&headers, key, &body) {
            warn!("webhook: wrong signature; refusing the request");
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if !recent(&headers, std::time::SystemTime::now()) {
            warn!("webhook: a signed request too old or too new; refusing it");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let raw: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "webhook: body is not JSON");
            return (StatusCode::BAD_REQUEST, "bad json").into_response();
        }
    };
    let payload = match incoming.open(raw) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "webhook: an event could not be read");
            return (StatusCode::BAD_REQUEST, "unreadable").into_response();
        }
    };

    // URL-verification handshake (sent once when the Request URL is set).
    if payload.get("type").and_then(Value::as_str) == Some("url_verification") {
        let token = payload.get("token").and_then(Value::as_str).unwrap_or("");
        let expected = incoming.verification_token.as_deref().unwrap_or("");
        if expected.is_empty() || !same(token.as_bytes(), expected.as_bytes()) {
            warn!("webhook: url_verification token mismatch");
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let challenge = payload
            .get("challenge")
            .and_then(Value::as_str)
            .unwrap_or_default();
        info!("webhook: url_verification handshake OK");
        return axum::Json(json!({ "challenge": challenge })).into_response();
    }

    // With an Encrypt Key, Lark signs every event it sends.
    if incoming.encrypt_key.is_some() && !signature {
        warn!("webhook: an event without a signature; refusing it");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match incoming.take_open(payload) {
        Taken::Done => StatusCode::OK.into_response(),
        Taken::Refused => StatusCode::UNAUTHORIZED.into_response(),
        Taken::Unreadable => StatusCode::BAD_REQUEST.into_response(),
    }
}

/// Whether the request carries Lark's signature of `body` with
/// `encrypt_key`: the hex SHA-256 of its timestamp, nonce, the key and the
/// body, in that order.
fn signed(headers: &HeaderMap, encrypt_key: &str, body: &[u8]) -> bool {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let (Some(timestamp), Some(nonce), Some(signature)) = (
        header("X-Lark-Request-Timestamp"),
        header("X-Lark-Request-Nonce"),
        header("X-Lark-Signature"),
    ) else {
        return false;
    };
    let expected = signature_of(timestamp, nonce, encrypt_key, body);
    same(
        expected.as_bytes(),
        signature.to_ascii_lowercase().as_bytes(),
    )
}

/// Whether the request's timestamp (seconds) is within [`SIGNED_FOR`] of
/// `now`.
fn recent(headers: &HeaderMap, now: std::time::SystemTime) -> bool {
    let Some(at) = headers
        .get("X-Lark-Request-Timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
    else {
        return false;
    };
    let now = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now.abs_diff(at) <= SIGNED_FOR.as_secs()
}

fn signature_of(timestamp: &str, nonce: &str, encrypt_key: &str, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(timestamp.as_bytes());
    hasher.update(nonce.as_bytes());
    hasher.update(encrypt_key.as_bytes());
    hasher.update(body);
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// `a == b`, in time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// AES-256-CBC decrypt of Lark's `encrypt` envelope. Key is `SHA256(encrypt_key)`,
/// the IV is the first 16 bytes of the base64-decoded payload, `PKCS7` padding.
fn decrypt(encrypt_key: &str, b64: &str) -> Result<Value> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .context("base64 decode")?;
    if data.len() < 16 {
        bail!("ciphertext shorter than IV");
    }
    let key = Sha256::digest(encrypt_key.as_bytes());
    let (iv, ct) = data.split_at(16);
    let mut buf = ct.to_vec();
    let plaintext = Aes256CbcDec::new(key.as_slice().into(), iv.into())
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|e| anyhow!("AES-CBC decrypt: {e}"))?;
    let v: Value = serde_json::from_slice(plaintext).context("decrypted body is not JSON")?;
    Ok(v)
}

/// The sender's id and its aliases (havn issue #262): the `union_id`, the
/// same person in every app of the tenant's, with the per-app `open_id` an
/// older adapter gave as its alias; the `open_id` alone when Lark gives no
/// `union_id`.
pub fn sender_ids(id: &SenderId) -> (String, Vec<String>) {
    let open = format!("{PREFIX}{}", id.open_id);
    match id.union_id.as_deref().filter(|u| !u.is_empty()) {
        Some(union) => (format!("{PREFIX}{union}"), vec![open]),
        None => (open, Vec::new()),
    }
}

/// Translate one inbound Lark message into a havn `ChannelInbound` and
/// send it over the WS. 1:1 (`p2p`) chats only.
async fn translate_and_forward(incoming: Incoming, ev: MessageReceiveEvent, raw: Value) {
    let m = &ev.message;
    if m.chat_type != "p2p" {
        debug!(chat_type = %m.chat_type, chat_id = %m.chat_id, "dropping non-p2p message (1:1 chats only)");
        return;
    }

    let Some(content) = build_content(&incoming, ev_message_payload(m), m).await else {
        return;
    };

    let timestamp = m
        .create_time
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .unwrap_or_else(chrono::Utc::now);

    let (sender_id, sender_aliases) = sender_ids(&ev.sender.sender_id);
    let inbound = ChannelInbound {
        seq: incoming.seq.fetch_add(1, Ordering::Relaxed),
        account_id: incoming.account_id.clone(),
        // Opaque platform message id; comes back as `in_reply_to` for replies.
        channel_message_id: m.message_id.clone(),
        sender_id,
        content,
        timestamp,
        raw: Some(raw),
        sender_aliases,
    };
    if incoming
        .outbound_tx
        .send(ChannelFrame::Inbound(inbound))
        .await
        .is_err()
    {
        warn!("lark: havn outbound channel closed; inbound dropped");
    }
}

/// Parse the `content` JSON string of a message, returning the inner object.
fn ev_message_payload(m: &crate::lark::LarkMessage) -> Value {
    serde_json::from_str(&m.content).unwrap_or(Value::Null)
}

/// Map a Lark message's parsed content into a havn `ChannelMessageContent`.
/// Voice is downloaded + transcribed (native). Location is native. Image /
/// file are surfaced as a text marker: havn's content model is URL-based and
/// has no file host, while Lark resources require the adapter's token to
/// fetch — so the agent can't retrieve them directly. (Outbound image/file
/// IS native — see `main.rs`.)
async fn build_content(
    incoming: &Incoming,
    content: Value,
    m: &crate::lark::LarkMessage,
) -> Option<ChannelMessageContent> {
    match m.message_type.as_str() {
        "text" => {
            let text = content.get("text").and_then(Value::as_str)?.to_string();
            Some(ChannelMessageContent::Text { text })
        }
        "post" => {
            // Rich text: flatten to the plain text we can extract.
            let text = flatten_post(&content);
            Some(ChannelMessageContent::Text { text })
        }
        "audio" => {
            let file_key = content.get("file_key").and_then(Value::as_str)?;
            let duration_ms = content.get("duration").and_then(Value::as_u64);
            translate_audio(incoming, &m.message_id, file_key, duration_ms).await
        }
        "location" => {
            let lat = content.get("latitude").and_then(parse_coord)?;
            let lon = content.get("longitude").and_then(parse_coord)?;
            Some(ChannelMessageContent::Location {
                latitude: lat,
                longitude: lon,
                accuracy_meters: None,
                name: content
                    .get("name")
                    .and_then(Value::as_str)
                    .map(String::from),
                address: None,
            })
        }
        "image" => Some(ChannelMessageContent::Text {
            text: "[image received — inbound images aren't fetchable by the agent yet]".into(),
        }),
        "file" => {
            let name = content
                .get("file_name")
                .and_then(Value::as_str)
                .unwrap_or("file");
            Some(ChannelMessageContent::Text {
                text: format!("[file received: {name}]"),
            })
        }
        other => {
            debug!(
                message_type = other,
                "dropping unsupported inbound message type"
            );
            None
        }
    }
}

async fn translate_audio(
    incoming: &Incoming,
    message_id: &str,
    file_key: &str,
    duration_ms: Option<u64>,
) -> Option<ChannelMessageContent> {
    let Some(whisper) = incoming.whisper.as_ref() else {
        warn!(
            file_key,
            "received audio but no openai_api_key configured; dropping"
        );
        return None;
    };
    let bytes = match incoming
        .lark
        .download_resource(message_id, file_key, "file")
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!(file_key, error = %e, "downloading Lark audio failed");
            return None;
        }
    };
    let transcript = match whisper.transcribe(bytes, "voice.opus").await {
        Ok(t) => t,
        Err(e) => {
            warn!(file_key, error = %e, "whisper transcription failed");
            return None;
        }
    };
    Some(ChannelMessageContent::Audio {
        transcript,
        duration_seconds: duration_ms.map(|ms| u32::try_from(ms / 1000).unwrap_or(0)),
        url: None,
    })
}

/// Extract plain text from a Lark `post` (rich text) content object.
fn flatten_post(content: &Value) -> String {
    // Shape: { "<lang>": { "title": "...", "content": [[ {tag,text/...}, ... ], ...] } }
    let mut out = String::new();
    // First language block only.
    if let Some(lang) = content.as_object().and_then(|o| o.values().next()) {
        if let Some(title) = lang.get("title").and_then(Value::as_str)
            && !title.is_empty()
        {
            out.push_str(title);
            out.push('\n');
        }
        if let Some(lines) = lang.get("content").and_then(Value::as_array) {
            for line in lines {
                if let Some(elems) = line.as_array() {
                    for el in elems {
                        // `text`/`a` carry their text in `text`; `at` mentions
                        // carry the display name in `user_name`.
                        if let Some(t) = el
                            .get("text")
                            .or_else(|| el.get("user_name"))
                            .and_then(Value::as_str)
                        {
                            out.push_str(t);
                        }
                    }
                }
                out.push('\n');
            }
        }
    }
    out.trim().to_string()
}

fn parse_coord(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_post_extracts_text() {
        let v: Value = serde_json::from_str(
            r#"{"en_us":{"title":"Hi","content":[[{"tag":"text","text":"hello "},{"tag":"text","text":"world"}]]}}"#,
        )
        .unwrap();
        assert_eq!(flatten_post(&v), "Hi\nhello world");
    }

    #[test]
    fn flatten_post_includes_mentions() {
        let v: Value = serde_json::from_str(
            r#"{"en_us":{"content":[[{"tag":"at","user_id":"ou_x","user_name":"@alice"},{"tag":"text","text":" hi"}]]}}"#,
        )
        .unwrap();
        assert_eq!(flatten_post(&v), "@alice hi");
    }

    #[test]
    fn parse_coord_handles_string_and_number() {
        assert_eq!(parse_coord(&json!("12.34")), Some(12.34));
        assert_eq!(parse_coord(&json!(56.78)), Some(56.78));
        assert_eq!(parse_coord(&json!(null)), None);
    }

    fn encrypt(encrypt_key: &str, plaintext: &[u8]) -> String {
        use aes::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
        type Aes256CbcEnc = cbc::Encryptor<Aes256>;
        let key = Sha256::digest(encrypt_key.as_bytes());
        let iv = [7u8; 16];
        let mut buf = vec![0u8; plaintext.len() + 16];
        let ct = Aes256CbcEnc::new(key.as_slice().into(), (&iv).into())
            .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
            .unwrap();
        let mut payload = iv.to_vec();
        payload.extend_from_slice(ct);
        base64::engine::general_purpose::STANDARD.encode(&payload)
    }

    #[test]
    fn aes_decrypt_round_trips_lark_scheme() {
        // Encrypt exactly the way Lark does (key = SHA256(encrypt_key),
        // IV prepended, AES-256-CBC, PKCS7, base64) and confirm `decrypt`
        // recovers it.
        let encrypt_key = "test-encrypt-key";
        let b64 = encrypt(
            encrypt_key,
            br#"{"type":"url_verification","challenge":"abc123","token":"vtok"}"#,
        );
        let got = decrypt(encrypt_key, &b64).unwrap();
        assert_eq!(got["type"], "url_verification");
        assert_eq!(got["challenge"], "abc123");
        assert_eq!(got["token"], "vtok");
        // Wrong key must fail, not silently mis-decode.
        assert!(decrypt("wrong-key", &b64).is_err());
    }

    #[test]
    fn a_webhook_request_is_signed_with_the_encrypt_key() {
        // As Lark's SDKs compute it.
        let body = br#"{"encrypt":"abc"}"#;
        let sig = signature_of("1700000000", "nonce1", "ekey", body);
        let mut hasher = Sha256::new();
        hasher.update(b"1700000000nonce1ekey");
        hasher.update(body);
        assert_eq!(sig, hex(&hasher.finalize()));
        assert_eq!(hex(&[0x0a, 0xff]), "0aff");
        let mut headers = HeaderMap::new();
        headers.insert("X-Lark-Request-Timestamp", "1700000000".parse().unwrap());
        headers.insert("X-Lark-Request-Nonce", "nonce1".parse().unwrap());
        headers.insert("X-Lark-Signature", sig.parse().unwrap());
        assert!(signed(&headers, "ekey", body));
        assert!(!signed(&headers, "other", body));
        assert!(!signed(&headers, "ekey", br#"{"encrypt":"abd"}"#));
        headers.remove("X-Lark-Signature");
        assert!(!signed(&headers, "ekey", body));
    }

    #[test]
    fn a_signed_request_is_taken_within_five_minutes_of_its_timestamp() {
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let mut headers = HeaderMap::new();
        headers.insert("X-Lark-Request-Timestamp", "1700000000".parse().unwrap());
        assert!(recent(&headers, at));
        assert!(recent(&headers, at + SIGNED_FOR));
        assert!(!recent(
            &headers,
            at + SIGNED_FOR + std::time::Duration::from_secs(1)
        ));
        assert!(!recent(
            &headers,
            at - SIGNED_FOR - std::time::Duration::from_secs(1)
        ));
        headers.insert("X-Lark-Request-Timestamp", "soon".parse().unwrap());
        assert!(!recent(&headers, at));
    }

    #[test]
    fn seen_events_and_messages_are_taken_once_up_to_a_bound() {
        let mut seen = Seen::default();
        assert!(seen.first_event("e1"));
        assert!(!seen.first_event("e1"));
        assert!(seen.first_message("om_1"));
        assert!(!seen.first_message("om_1"));
        // An event id and a message id do not collide.
        assert!(seen.first_message("e1"));
        // An event without an id is never taken for another.
        assert!(seen.first_event("") && seen.first_event(""));
        for n in 0..SEEN_KEPT {
            assert!(seen.first_event(&format!("x{n}")));
        }
        assert_eq!(seen.events.keys.len(), SEEN_KEPT);
        // The oldest was let go.
        assert!(seen.first_event("e1"));
        assert!(!seen.first_event(&format!("x{}", SEEN_KEPT - 1)));
    }

    #[test]
    fn a_sender_goes_by_union_id_with_its_open_id_as_alias() {
        let id: SenderId =
            serde_json::from_value(json!({"open_id": "ou_a", "union_id": "on_b", "user_id": "u1"}))
                .unwrap();
        assert_eq!(
            sender_ids(&id),
            ("lark:on_b".to_string(), vec!["lark:ou_a".to_string()])
        );
        let id: SenderId = serde_json::from_value(json!({"open_id": "ou_a"})).unwrap();
        assert_eq!(sender_ids(&id), ("lark:ou_a".to_string(), Vec::new()));
    }

    fn incoming(
        token: Option<&str>,
        key: Option<&str>,
    ) -> (Incoming, mpsc::Receiver<ChannelFrame>) {
        let (tx, rx) = mpsc::channel(8);
        let lark = LarkClient::unauthenticated("http://127.0.0.1:9".into(), "a".into(), "s".into())
            .unwrap();
        (
            Incoming {
                lark,
                whisper: None,
                outbound_tx: tx,
                account_id: "bot".into(),
                seq: Arc::new(AtomicU64::new(1)),
                seen: Arc::new(Mutex::new(Seen::default())),
                verification_token: token.map(String::from),
                encrypt_key: key.map(String::from),
                chats: Chats::default(),
            },
            rx,
        )
    }

    fn message_event(event_id: &str, message_id: &str, token: &str) -> Value {
        json!({
            "schema": "2.0",
            "header": {"event_id": event_id, "event_type": "im.message.receive_v1",
                       "token": token, "create_time": "1700000000000", "app_id": "a", "tenant_key": "t"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_a", "union_id": "on_b"}, "sender_type": "user"},
                "message": {"message_id": message_id, "chat_id": "oc_1", "chat_type": "p2p",
                            "message_type": "text", "content": "{\"text\":\"hello\"}",
                            "create_time": "1700000000000"}
            }
        })
    }

    #[tokio::test]
    async fn a_retried_event_makes_one_inbound() {
        let (incoming, mut rx) = incoming(Some("vtok"), None);
        assert_eq!(
            incoming.take(message_event("e1", "om_1", "vtok")),
            Taken::Done
        );
        // Lark's retry: the same event.
        assert_eq!(
            incoming.take(message_event("e1", "om_1", "vtok")),
            Taken::Done
        );
        // The same message under another event id.
        assert_eq!(
            incoming.take(message_event("e2", "om_1", "vtok")),
            Taken::Done
        );
        assert_eq!(
            incoming.take(message_event("e3", "om_2", "vtok")),
            Taken::Done
        );
        let mut got = Vec::new();
        for _ in 0..2 {
            match rx.recv().await {
                Some(ChannelFrame::Inbound(inb)) => got.push(inb),
                other => panic!("expected an inbound, got {other:?}"),
            }
        }
        got.sort_by(|a, b| a.channel_message_id.cmp(&b.channel_message_id));
        assert_eq!(got[0].channel_message_id, "om_1");
        assert_eq!(got[1].channel_message_id, "om_2");
        assert_eq!(got[0].sender_id, "lark:on_b");
        assert_eq!(got[0].sender_aliases, vec!["lark:ou_a".to_string()]);
        // Without what is not the agent's to see.
        let raw = got[0].raw.as_ref().unwrap();
        assert!(raw["header"].get("token").is_none() && raw["header"].get("tenant_key").is_none());
        assert_eq!(raw["header"]["event_id"], "e1");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "no third inbound");
    }

    #[tokio::test]
    async fn a_chat_s_messages_go_on_in_the_order_they_came() {
        let (incoming, mut rx) = incoming(None, None);
        for n in 0..40 {
            let mut event = message_event(&format!("e{n}"), &format!("om_{n}"), "");
            // Two chats, interleaved.
            event["event"]["message"]["chat_id"] = json!(format!("oc_{}", n % 2));
            assert_eq!(incoming.take(event), Taken::Done);
        }
        let mut seen: HashMap<String, Vec<u32>> = HashMap::new();
        for _ in 0..40 {
            let Some(ChannelFrame::Inbound(inb)) = rx.recv().await else {
                panic!("expected an inbound");
            };
            let n: u32 = inb
                .channel_message_id
                .trim_start_matches("om_")
                .parse()
                .unwrap();
            seen.entry(format!("oc_{}", n % 2)).or_default().push(n);
        }
        for (chat, got) in &seen {
            let mut sorted = got.clone();
            sorted.sort_unstable();
            assert_eq!(got, &sorted, "{chat} in order");
            assert_eq!(got.len(), 20);
        }
        assert_eq!(incoming.chats.lock().unwrap().len(), 2, "a worker per chat");
    }

    #[tokio::test]
    async fn an_event_with_another_token_or_key_is_refused() {
        let (incoming, mut rx) = incoming(Some("vtok"), Some("ekey"));
        assert_eq!(
            incoming.take(message_event("e1", "om_1", "other")),
            Taken::Refused
        );
        let sealed = encrypt(
            "ekey",
            message_event("e2", "om_2", "vtok").to_string().as_bytes(),
        );
        assert_eq!(incoming.take(json!({ "encrypt": sealed })), Taken::Done);
        let wrong = encrypt(
            "nope",
            message_event("e3", "om_3", "vtok").to_string().as_bytes(),
        );
        assert_eq!(
            incoming.take(json!({ "encrypt": wrong })),
            Taken::Unreadable
        );
        match rx.recv().await {
            Some(ChannelFrame::Inbound(inb)) => assert_eq!(inb.channel_message_id, "om_2"),
            other => panic!("expected an inbound, got {other:?}"),
        }
        // No verification token configured (a long connection): any token.
        let (incoming, mut rx) = self::incoming(None, None);
        assert_eq!(incoming.take(message_event("e1", "om_1", "")), Taken::Done);
        assert!(matches!(rx.recv().await, Some(ChannelFrame::Inbound(_))));
    }
}
