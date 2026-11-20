//! Inbound webhook HTTP server — Lark's event-subscription delivery.
//!
//! The adapter opens no long connection to Lark, so events arrive as HTTP
//! POSTs (`im.message.receive_v1`). This module:
//! - serves `POST /webhook`,
//! - answers the one-time `url_verification` challenge,
//! - AES-decrypts the body when an Encrypt Key is configured,
//! - checks the Verification Token,
//! - translates each inbound message into a havn `ChannelInbound` and
//!   forwards it over the gateway WS.
//!
//! Lark requires a 200 within 3 seconds, so translation (which may do a
//! resource download + STT) runs in a spawned task; the handler returns
//! 200 immediately.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aes::Aes256;
use aes::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use anyhow::{Context as _, Result, anyhow, bail};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::lark::{EventEnvelope, LarkClient, MessageReceiveEvent};
use crate::types::{ChannelFrame, ChannelInbound, ChannelMessageContent};
use crate::whisper::Whisper;

type Aes256CbcDec = cbc::Decryptor<Aes256>;

#[derive(Clone)]
pub struct WebhookState {
    pub lark: Arc<LarkClient>,
    pub whisper: Option<Whisper>,
    pub outbound_tx: mpsc::Sender<ChannelFrame>,
    pub account_id: String,
    pub seq: Arc<AtomicU64>,
    pub verification_token: String,
    pub encrypt_key: Option<String>,
}

/// Build the webhook router. The operator points Lark's "Request URL" at
/// `http(s)://<host>/webhook`.
pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/webhook", post(handle))
        .with_state(state)
}

async fn handle(State(st): State<WebhookState>, body: axum::body::Bytes) -> impl IntoResponse {
    let raw: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "webhook: body is not JSON");
            return (StatusCode::BAD_REQUEST, "bad json").into_response();
        }
    };

    // Decrypt the envelope if an Encrypt Key is configured AND the payload
    // is wrapped. (Lark only sends `{"encrypt":...}` when encryption is on.)
    let payload: Value = match raw.get("encrypt").and_then(Value::as_str) {
        Some(enc) => {
            let Some(key) = st.encrypt_key.as_deref() else {
                warn!("webhook: received encrypted event but no encrypt_key configured");
                return (StatusCode::BAD_REQUEST, "encryption not configured").into_response();
            };
            match decrypt(key, enc) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "webhook: decrypt failed");
                    return (StatusCode::BAD_REQUEST, "decrypt failed").into_response();
                }
            }
        }
        None => raw,
    };

    // URL-verification handshake (sent once when the Request URL is set).
    if payload.get("type").and_then(Value::as_str) == Some("url_verification") {
        let token_ok = payload.get("token").and_then(Value::as_str) == Some(&st.verification_token);
        if !token_ok {
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

    // Event (schema 2.0). Validate the verification token, then dispatch.
    let env: EventEnvelope = match serde_json::from_value(payload.clone()) {
        Ok(e) => e,
        Err(e) => {
            debug!(error = %e, "webhook: not a schema-2.0 event envelope; ignoring");
            return StatusCode::OK.into_response();
        }
    };
    if env.header.token != st.verification_token {
        warn!("webhook: event verification token mismatch");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    if env.header.event_type == "im.message.receive_v1" {
        match serde_json::from_value::<MessageReceiveEvent>(env.event) {
            Ok(ev) => {
                // Translate + forward off the response path (Lark wants a
                // 200 within 3s; STT/download can take longer).
                tokio::spawn(translate_and_forward(st, ev, payload));
            }
            Err(e) => warn!(error = %e, "webhook: malformed im.message.receive_v1 event"),
        }
    } else {
        debug!(event_type = %env.header.event_type, "webhook: ignoring non-message event");
    }

    StatusCode::OK.into_response()
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

/// Translate one inbound Lark message into a havn `ChannelInbound` and
/// send it over the WS. v1 handles 1:1 (`p2p`) chats only.
async fn translate_and_forward(st: WebhookState, ev: MessageReceiveEvent, raw: Value) {
    let m = &ev.message;
    if m.chat_type != "p2p" {
        debug!(chat_type = %m.chat_type, chat_id = %m.chat_id, "dropping non-p2p message (v1 = 1:1 DMs)");
        return;
    }

    let Some(content) = build_content(&st, ev_message_payload(m), m).await else {
        return;
    };

    let timestamp = m
        .create_time
        .as_deref()
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .unwrap_or_else(chrono::Utc::now);

    let inbound = ChannelInbound {
        seq: st.seq.fetch_add(1, Ordering::Relaxed),
        account_id: st.account_id.clone(),
        // Opaque platform message id; comes back as `in_reply_to` for replies.
        channel_message_id: m.message_id.clone(),
        sender_id: format!("lark:{}", ev.sender.sender_id.open_id),
        content,
        timestamp,
        raw: Some(raw),
    };
    if st
        .outbound_tx
        .send(ChannelFrame::Inbound(inbound))
        .await
        .is_err()
    {
        warn!("webhook: havn outbound channel closed; inbound dropped");
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
    st: &WebhookState,
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
            translate_audio(st, &m.message_id, file_key, duration_ms).await
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
    st: &WebhookState,
    message_id: &str,
    file_key: &str,
    duration_ms: Option<u64>,
) -> Option<ChannelMessageContent> {
    let Some(whisper) = st.whisper.as_ref() else {
        warn!(
            file_key,
            "received audio but no openai_api_key configured; dropping"
        );
        return None;
    };
    let bytes = match st
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

    #[test]
    fn aes_decrypt_round_trips_lark_scheme() {
        // Encrypt exactly the way Lark does (key = SHA256(encrypt_key),
        // IV prepended, AES-256-CBC, PKCS7, base64) and confirm `decrypt`
        // recovers it. This is the riskiest, hardest-to-test-online bit.
        use aes::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
        type Aes256CbcEnc = cbc::Encryptor<Aes256>;

        let encrypt_key = "test-encrypt-key";
        let key = Sha256::digest(encrypt_key.as_bytes());
        let iv = [7u8; 16];
        let plaintext = br#"{"type":"url_verification","challenge":"abc123","token":"vtok"}"#;

        let mut buf = vec![0u8; plaintext.len() + 16];
        let ct = Aes256CbcEnc::new(key.as_slice().into(), (&iv).into())
            .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
            .unwrap();
        let mut payload = iv.to_vec();
        payload.extend_from_slice(ct);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&payload);

        let got = decrypt(encrypt_key, &b64).unwrap();
        assert_eq!(got["type"], "url_verification");
        assert_eq!(got["challenge"], "abc123");
        assert_eq!(got["token"], "vtok");
        // Wrong key must fail, not silently mis-decode.
        assert!(decrypt("wrong-key", &b64).is_err());
    }
}
