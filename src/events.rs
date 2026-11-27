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
//!   Token and tenant key, its files first given to the gateway;
//! - takes a press on one of the bot's cards (`card.action.trigger`) and
//!   an item of the bot's menu (`application.bot.menu_v6`) as what the
//!   person typed, answering a press with the card without its buttons.
//!
//! The webhook also answers the one-time `url_verification` challenge and,
//! with an Encrypt Key, refuses a request whose signature does not match or
//! is older than [`SIGNED_FOR`].
//!
//! Lark wants an answer within 3 seconds, so translation (which may
//! download and upload a file) runs in a worker of the message's chat,
//! which takes the chat's messages in the order they came; the event is
//! answered at once.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
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

use crate::card;
use crate::gateway::Gateway;
use crate::havn_ws::Delivered;
use crate::lark::{
    CardAction, EventEnvelope, LarkClient, MenuEvent, MessageReceiveEvent, SenderId,
};
use crate::types::{Attachment, ChannelFrame, ChannelInbound, ChannelMessageContent};

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
pub type Chats = Arc<Mutex<HashMap<String, mpsc::Sender<Arrival>>>>;

/// What a chat's worker passes on, in order.
pub enum Arrival {
    /// A message a sender sent.
    Message(Box<MessageReceiveEvent>, Value),
    /// What a press or the bot's menu sends, as if the person typed it;
    /// `react_on`, the card pressed, shows that its reply is due.
    Said {
        ids: SenderId,
        message_id: String,
        text: String,
        raw: Value,
        react_on: Option<String>,
    },
}

/// The start of the message id of a press, which is no message to reply
/// to: its reply goes to the person, in their private chat.
pub const PRESS_PREFIX: &str = "press:";

/// People whose private chats with the bot are known, at most; past that,
/// the oldest are let go.
const PEOPLE_KEPT: usize = 4096;

/// Each person's private chat with the bot, by their ids, as their messages
/// showed it.
#[derive(Debug, Default)]
pub struct People {
    chats: HashMap<String, String>,
    order: VecDeque<String>,
}

impl People {
    fn note(&mut self, ids: &[String], chat: &str) {
        for id in ids {
            if self.chats.insert(id.clone(), chat.to_string()).is_none() {
                self.order.push_back(id.clone());
            }
        }
        while self.order.len() > PEOPLE_KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.chats.remove(&oldest);
            }
        }
    }

    fn chat_of(&self, ids: &[String]) -> Option<String> {
        ids.iter().find_map(|id| self.chats.get(id).cloned())
    }
}

/// The start of the message id of an item of the bot's menu chosen, which
/// is no message: a reply to it is sent to the person.
pub const MENU_PREFIX: &str = "menu:";

/// What takes Lark's events, from either transport.
#[derive(Clone)]
pub struct Incoming {
    pub lark: Arc<LarkClient>,
    pub gateway: Gateway,
    pub delivered: Arc<Delivered>,
    pub working: Working,
    pub cards: Arc<Mutex<card::Cards>>,
    pub people: Arc<Mutex<People>>,
    pub outbound_tx: mpsc::Sender<ChannelFrame>,
    pub account_id: String,
    pub seq: Arc<AtomicU64>,
    pub seen: Arc<Mutex<Seen>>,
    pub verification_token: Option<String>,
    pub encrypt_key: Option<String>,
    pub chats: Chats,
}

/// What became of an event.
#[derive(Debug, Clone, PartialEq)]
pub enum Taken {
    /// Taken, or one not acted on (another kind, or taken before).
    Done,
    /// Not Lark's: its Verification Token does not match.
    Refused,
    /// Not readable: not JSON, or encrypted with another key.
    Unreadable,
    /// A press on a card, answered with this.
    Answer(Value),
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
        match env.header.event_type.as_str() {
            "im.message.receive_v1" => {}
            "card.action.trigger" => {
                return Taken::Answer(self.press(env.event, without_secrets(payload)));
            }
            "application.bot.menu_v6" => {
                self.menu(env.event, without_secrets(payload), &env.header.event_id);
                return Taken::Done;
            }
            other => {
                debug!(event_type = %other, "lark: ignoring an event of another kind");
                return Taken::Done;
            }
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
        let chat = ev.message.chat_id.clone();
        if ev.message.chat_type == "p2p" {
            let (sender, aliases) = sender_ids(&ev.sender.sender_id);
            let mut ids = aliases;
            ids.push(sender);
            self.people().note(&ids, &chat);
        }
        let _ = self.to_chat(
            chat,
            Arrival::Message(Box::new(ev), without_secrets(payload)),
        );
        Taken::Done
    }

    fn people(&self) -> std::sync::MutexGuard<'_, People> {
        self.people
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Hand `arrival` to the worker of `chat`, started when it has none:
    /// whether it was, which it is not when the chat has as many waiting as
    /// it may.
    fn to_chat(&self, chat: String, arrival: Arrival) -> bool {
        let mut chats = self
            .chats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tx = chats.entry(chat.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(CHAT_WAITING);
            tokio::spawn(chat_worker(self.clone(), chat.clone(), rx));
            tx
        });
        if tx.try_send(arrival).is_err() {
            warn!(chat = %chat, "lark: a chat has as many messages waiting as it may; dropping one");
            return false;
        }
        true
    }
}

/// Translate and forward the messages of `chat` one at a time, in the order
/// they came. It ends after [`CHAT_IDLE`] with nothing to do, under the lock
/// [`Incoming::to_chat`] adds with, so no message is left behind.
async fn chat_worker(incoming: Incoming, chat: String, mut rx: mpsc::Receiver<Arrival>) {
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
        translate_and_forward(incoming.clone(), next).await;
    }
}

/// `payload` without what is not the agent's to see: the app's
/// Verification Token, the tenant's key, and a press's token.
fn without_secrets(mut payload: Value) -> Value {
    if let Some(header) = payload.get_mut("header").and_then(Value::as_object_mut) {
        header.remove("token");
        header.remove("tenant_key");
    }
    for who in ["/event/sender", "/event/operator"] {
        if let Some(who) = payload.pointer_mut(who).and_then(Value::as_object_mut) {
            who.remove("tenant_key");
        }
    }
    // A press's token, which updates its card.
    if let Some(event) = payload.get_mut("event").and_then(Value::as_object_mut) {
        event.remove("token");
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
        Taken::Answer(answer) => axum::Json(answer).into_response(),
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

impl Incoming {
    /// The sender's id and aliases as the gateway takes them: the
    /// `open_id` alone for one that never took aliases.
    fn ids(&self, id: &SenderId) -> (String, Vec<String>) {
        if self.delivered.takes_aliases() {
            sender_ids(id)
        } else {
            (format!("{PREFIX}{}", id.open_id), Vec::new())
        }
    }

    /// A press on one of the bot's cards (`card.action.trigger`): what it
    /// sends goes on as the person who pressed it typed it, once, and Lark
    /// is answered with the card without its buttons. Only the person the
    /// card was sent to may press it: one forwarded to someone else is not
    /// theirs.
    fn press(&self, event: Value, raw: Value) -> Value {
        let ev: CardAction = match serde_json::from_value(event) {
            Ok(ev) => ev,
            Err(e) => {
                warn!(error = %e, "lark: malformed card.action.trigger");
                return json!({});
            }
        };
        let value = &ev.action.value;
        let ids = SenderId {
            open_id: ev.operator.open_id.clone(),
            union_id: ev.operator.union_id.clone(),
        };
        let (sender, aliases) = sender_ids(&ids);
        let to = value
            .get(card::TO)
            .and_then(Value::as_str)
            .unwrap_or_default();
        if to != sender && !aliases.iter().any(|a| a == to) {
            warn!(message = %ev.context.open_message_id, "lark: a card pressed by someone it was not sent to");
            return toast("error", "This is not yours to answer.");
        }
        let said = if value.get(card::KIND).and_then(Value::as_str) == Some(card::QUESTION) {
            let numbers: Vec<String> = ev
                .action
                .form_value
                .get(card::CHOICES)
                .and_then(Value::as_array)
                .map(|chosen| {
                    chosen
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|n| n.parse::<u32>().is_ok())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            if numbers.is_empty() {
                return toast("warning", "Choose at least one.");
            }
            let answer = value
                .get(card::ANSWER)
                .and_then(Value::as_str)
                .unwrap_or_default();
            format!("{answer} {}", numbers.join(","))
        } else {
            match value.get(card::SAID).and_then(Value::as_str) {
                Some(said) if !said.is_empty() => said.to_string(),
                _ => return json!({}),
            }
        };
        let message_id = ev.context.open_message_id.clone();
        let mut ids_of_sender = aliases;
        ids_of_sender.push(sender.clone());
        let private = self.people().chat_of(&ids_of_sender);
        // With the cards' lock held, so a card is marked pressed only once,
        // and only once its press is on its way.
        let mut cards = self
            .cards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cards.was_pressed(&message_id) {
            return toast("info", "This was answered already.");
        }
        // One of the bot's cards it knows, or one in the person's private
        // chat with it: a copy forwarded into a group, pressed there, is
        // not taken, so the agent's private reply goes to no group.
        let ours = cards.knows(&message_id);
        let in_private =
            private.is_some() && private.as_deref() == Some(ev.context.open_chat_id.as_str());
        if !ours && !in_private {
            warn!(message = %message_id, "lark: a card pressed outside the person's private chat with the bot");
            return toast("error", "Press it in your chat with the bot.");
        }
        // The person's private chat, where their messages wait in turn.
        let chat = private.unwrap_or_else(|| sender.clone());
        let queued = self.to_chat(
            chat,
            Arrival::Said {
                ids,
                message_id: format!("{PRESS_PREFIX}{message_id}"),
                text: said,
                raw,
                react_on: Some(message_id.clone()),
            },
        );
        if !queued {
            return toast("warning", "Busy; try again in a moment.");
        }
        // The card without its buttons, so it is pressed once; one sent
        // before a restart keeps them.
        match cards.press(&message_id) {
            card::Pressed::Now(Some(markdown)) => {
                json!({ "card": { "type": "raw", "data": card::bare(&markdown) } })
            }
            _ => json!({}),
        }
    }

    /// An item of the bot's menu (`application.bot.menu_v6`), set up on the
    /// Developer Console with a command's name as its event key: the command
    /// goes on as the person typed it.
    fn menu(&self, event: Value, raw: Value, event_id: &str) {
        let ev: MenuEvent = match serde_json::from_value(event) {
            Ok(ev) => ev,
            Err(e) => {
                warn!(error = %e, "lark: malformed application.bot.menu_v6");
                return;
            }
        };
        let key = ev.event_key.trim();
        if key.is_empty() {
            return;
        }
        let text = if key.starts_with('/') {
            key.to_string()
        } else {
            format!("/{key}")
        };
        let (sender, aliases) = sender_ids(&ev.operator.operator_id);
        let mut ids = aliases;
        ids.push(sender.clone());
        // In turn with the person's messages, in their private chat; Lark
        // names no chat for a menu item, so when none of their messages
        // showed it yet, the menu's items wait in a line of their own.
        let chat = self.people().chat_of(&ids).unwrap_or(sender);
        let _ = self.to_chat(
            chat,
            Arrival::Said {
                ids: ev.operator.operator_id,
                message_id: format!("{MENU_PREFIX}{event_id}"),
                text,
                raw,
                react_on: None,
            },
        );
    }
}

/// A toast answering a press, of Lark's `type` (info, success, warning,
/// error).
fn toast(kind: &str, content: &str) -> Value {
    json!({ "toast": { "type": kind, "content": content } })
}

/// Translate one arrival into a havn `ChannelInbound` and send it over the
/// WS. 1:1 (`p2p`) chats only.
async fn translate_and_forward(incoming: Incoming, arrival: Arrival) {
    let (ids, message_id, content, attachments, raw, timestamp) = match arrival {
        Arrival::Message(ev, raw) => {
            let m = &ev.message;
            if m.chat_type != "p2p" {
                debug!(chat_type = %m.chat_type, chat_id = %m.chat_id, "dropping non-p2p message (1:1 chats only)");
                return;
            }
            let (sender, aliases) = incoming.ids(&ev.sender.sender_id);
            let mut attachments = Vec::new();
            let Some(content) = build_content(
                &incoming,
                ev_message_payload(m),
                m,
                (&sender, &aliases),
                &mut attachments,
            )
            .await
            else {
                return;
            };
            incoming
                .working
                .start(&sender, &m.message_id, &m.message_id);
            let timestamp = m
                .create_time
                .as_deref()
                .and_then(|s| s.parse::<i64>().ok())
                .and_then(chrono::DateTime::from_timestamp_millis)
                .unwrap_or_else(chrono::Utc::now);
            (
                ev.sender.sender_id,
                m.message_id.clone(),
                content,
                attachments,
                raw,
                timestamp,
            )
        }
        Arrival::Said {
            ids,
            message_id,
            text,
            raw,
            react_on,
        } => {
            if let Some(react_on) = react_on {
                let (sender, _) = incoming.ids(&ids);
                incoming.working.start(&sender, &react_on, &message_id);
            }
            (
                ids,
                message_id,
                ChannelMessageContent::Text { text },
                Vec::new(),
                raw,
                chrono::Utc::now(),
            )
        }
    };
    let (sender_id, sender_aliases) = incoming.ids(&ids);
    let inbound = ChannelInbound {
        seq: incoming.seq.fetch_add(1, Ordering::Relaxed),
        account_id: incoming.account_id.clone(),
        // Opaque platform message id; comes back as `in_reply_to` for replies.
        channel_message_id: message_id,
        sender_id,
        content,
        timestamp,
        raw: Some(raw),
        sender_aliases,
        attachments,
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
/// Text, rich text and locations as such. An image, a file, a voice
/// message or a video is downloaded from Lark and given to the gateway
/// (havn issue #106), which keeps it in the agent's inbox and transcribes a
/// voice message when it is set up to (havn issue #164); the message names
/// it, or says it could not be received. A sticker, which Lark lets no bot
/// download, is a line saying so.
async fn build_content(
    incoming: &Incoming,
    content: Value,
    m: &crate::lark::LarkMessage,
    sender: (&str, &[String]),
    attachments: &mut Vec<Attachment>,
) -> Option<ChannelMessageContent> {
    let key = |name: &str| content.get(name).and_then(Value::as_str).map(String::from);
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
        "image" => {
            let image_key = key("image_key")?;
            let received = receive(incoming, m, &image_key, "image", sender, None).await;
            let name = received
                .as_ref()
                .map_or_else(|_| "image".to_string(), |a| a.name.clone());
            Some(file_message(received, None, &name, attachments))
        }
        "file" => {
            let file_key = key("file_key")?;
            let name = key("file_name").unwrap_or_else(|| "file".into());
            let received = receive(incoming, m, &file_key, "file", sender, Some(&name)).await;
            Some(file_message(received, None, &name, attachments))
        }
        "audio" => {
            let file_key = key("file_key")?;
            let seconds = content.get("duration").and_then(Value::as_u64).unwrap_or(0) / 1000;
            let received = receive(incoming, m, &file_key, "audio", sender, Some(VOICE_NAME)).await;
            let note = format!("[A voice message of {seconds} seconds.]");
            Some(file_message(received, Some(&note), VOICE_NAME, attachments))
        }
        "media" => {
            let file_key = key("file_key")?;
            let name = key("file_name").unwrap_or_else(|| "video.mp4".into());
            let received = receive(incoming, m, &file_key, "video", sender, Some(&name)).await;
            Some(file_message(received, None, &name, attachments))
        }
        "sticker" => Some(ChannelMessageContent::Text {
            text: "[The sender sent a sticker, which Lark lets no bot see.]".into(),
        }),
        other => {
            debug!(
                message_type = other,
                "dropping unsupported inbound message type"
            );
            None
        }
    }
}

/// The name a voice message goes to the gateway by.
const VOICE_NAME: &str = "voice.ogg";

/// Download resource `key` of message `m` from Lark and give it to the
/// gateway as the sender's: `kind` says what it is (`image`, `audio`,
/// `video`, `file`), `name` what to call it (an image is named by what its
/// bytes are).
async fn receive(
    incoming: &Incoming,
    m: &crate::lark::LarkMessage,
    key: &str,
    kind: &str,
    (sender, aliases): (&str, &[String]),
    name: Option<&str>,
) -> Result<Attachment> {
    let resource = if kind == "image" { "image" } else { "file" };
    let bytes = incoming
        .lark
        .download_resource(&m.message_id, key, resource)
        .await
        .context("downloading from Lark")?;
    let (name, media_type) = match kind {
        "image" => {
            let (extension, media_type) =
                image_type(&bytes).context("not a PNG, JPEG, GIF or WebP image")?;
            (format!("image.{extension}"), media_type.to_string())
        }
        "audio" => (VOICE_NAME.to_string(), "audio/ogg".to_string()),
        "video" => (
            name.unwrap_or("video.mp4").to_string(),
            "video/mp4".to_string(),
        ),
        // The gateway takes its type from its name.
        _ => (
            name.unwrap_or("file").to_string(),
            "application/octet-stream".to_string(),
        ),
    };
    incoming
        .gateway
        .upload(sender, aliases, &name, &media_type, bytes)
        .await
}

/// The extension and media type of an image Lark and the gateway both
/// show, by its bytes.
pub fn image_type(bytes: &[u8]) -> Option<(&'static str, &'static str)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(("png", "image/png"))
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some(("jpg", "image/jpeg"))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some(("gif", "image/gif"))
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some(("webp", "image/webp"))
    } else {
        None
    }
}

/// The text of a message that sent file `name`: `note`, and when the file
/// could not be passed on, a line saying so, for the agent to tell the
/// sender.
fn file_message(
    received: Result<Attachment>,
    note: Option<&str>,
    name: &str,
    attachments: &mut Vec<Attachment>,
) -> ChannelMessageContent {
    let mut text = note.unwrap_or_default().to_string();
    match received {
        Ok(attachment) => attachments.push(attachment),
        Err(e) => {
            warn!(name, error = %format!("{e:#}"), "could not pass a file on to havn");
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            // Only what went wrong, not the error itself.
            let _ = write!(
                text,
                "[The sender sent a file, {name}, that could not be received: {}]",
                failure(&e)
            );
        }
    }
    ChannelMessageContent::Text { text }
}

/// What kept a file from the agent, for the message to say.
fn failure(e: &anyhow::Error) -> &'static str {
    let chain = format!("{e:#}");
    // The gateway's reasons first: several share a status.
    if chain.contains("inbox is full") {
        "the agent's inbox is full"
    } else if chain.contains("disk space") {
        "the agent's host is out of disk space"
    } else if chain.contains("in the last hour") {
        "this sender sent too many files in the last hour"
    } else if chain.contains("larger than") || chain.contains("over 25 MB") {
        "it is larger than 25 MB"
    } else if chain.contains("(403") {
        "the agent does not take files from this sender"
    } else if chain.contains("still running") {
        "too many files at once; it can be sent again in a while"
    } else if chain.contains("not a PNG") {
        "it is an image of a kind the agent cannot see"
    } else {
        "it could not be downloaded from Lark or saved"
    }
}

/// The reaction shown on each message whose reply is due, as a typing
/// indicator: Lark's `Typing`, put on as the message goes to the gateway and
/// taken off as its reply comes. The gateway does not say which reply
/// answers which message, so a chat's messages are answered in turn, the
/// oldest first; a part of a reply with more to come leaves it on. One that
/// gets no reply goes after [`WORKING_FOR`].
#[derive(Clone)]
pub struct Working {
    lark: Arc<LarkClient>,
    chats: Arc<Mutex<HashMap<String, VecDeque<Shown>>>>,
    next: Arc<AtomicU64>,
}

/// A message with the reaction on, or being put on.
struct Shown {
    id: u64,
    /// The message the reaction is on.
    message_id: String,
    /// The message id its reply names in `in_reply_to`.
    answered_by: String,
    reaction: Option<String>,
}

const WORKING_EMOJI: &str = "Typing";
const WORKING_FOR: std::time::Duration = std::time::Duration::from_mins(5);

impl Working {
    pub fn new(lark: Arc<LarkClient>) -> Self {
        Self {
            lark,
            chats: Arc::default(),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    fn chats(&self) -> std::sync::MutexGuard<'_, HashMap<String, VecDeque<Shown>>> {
        self.chats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Message `message_id` came from `to` (the id replies go to): its reply
    /// is due, named `answered_by` as its reply answers it.
    pub fn start(&self, to: &str, message_id: &str, answered_by: &str) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.chats()
            .entry(to.to_string())
            .or_default()
            .push_back(Shown {
                id,
                message_id: message_id.to_string(),
                answered_by: answered_by.to_string(),
                reaction: None,
            });
        let this = self.clone();
        let (to, message_id) = (to.to_string(), message_id.to_string());
        tokio::spawn(async move {
            match this.lark.add_reaction(&message_id, WORKING_EMOJI).await {
                Ok(reaction) => {
                    let gone = {
                        let mut chats = this.chats();
                        match chats
                            .get_mut(&to)
                            .and_then(|q| q.iter_mut().find(|s| s.id == id))
                        {
                            Some(shown) => {
                                shown.reaction = Some(reaction.clone());
                                false
                            }
                            None => true,
                        }
                    };
                    // Replied to while it was being put on.
                    if gone {
                        this.take_off(&message_id, &reaction).await;
                    }
                }
                Err(e) => {
                    debug!(error = %format!("{e:#}"), "lark: could not show that a reply is due");
                }
            }
            tokio::time::sleep(WORKING_FOR).await;
            this.end(&to, |s| s.id == id);
        });
    }

    /// A reply went to `to`, or one that ends a turn. The message it
    /// answers (`in_reply_to`) loses its reaction, and one that answers a
    /// message without one takes no other's. A reply that names none, as the
    /// agent's in a private chat, takes it off the oldest there: the gateway
    /// does not say which it answers, nor tell them from a scheduled report.
    pub fn replied(&self, to: &str, in_reply_to: Option<&str>) {
        match in_reply_to {
            Some(answers) => self.end(to, |s| s.answered_by == answers),
            None => self.end(to, |_| true),
        }
    }

    fn end(&self, to: &str, which: impl Fn(&Shown) -> bool) {
        let shown = {
            let mut chats = self.chats();
            let Some(queue) = chats.get_mut(to) else {
                return;
            };
            let Some(at) = queue.iter().position(which) else {
                return;
            };
            let shown = queue.remove(at);
            if queue.is_empty() {
                chats.remove(to);
            }
            shown
        };
        if let Some(Shown {
            message_id,
            reaction: Some(reaction),
            ..
        }) = shown
        {
            let this = self.clone();
            tokio::spawn(async move { this.take_off(&message_id, &reaction).await });
        }
    }

    async fn take_off(&self, message_id: &str, reaction: &str) {
        if let Err(e) = self.lark.delete_reaction(message_id, reaction).await {
            debug!(error = %format!("{e:#}"), "lark: could not take a reaction off");
        }
    }

    /// The messages waiting for a reply to `to`.
    #[cfg(test)]
    fn waiting(&self, to: &str) -> Vec<String> {
        self.chats()
            .get(to)
            .map(|q| q.iter().map(|s| s.message_id.clone()).collect())
            .unwrap_or_default()
    }
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
        let delivered = Arc::new(Delivered::load(None, "bot"));
        delivered.took_aliases();
        (
            Incoming {
                lark: lark.clone(),
                gateway: Gateway::new("ws://127.0.0.1:9", "t", "lark", "bot").unwrap(),
                delivered,
                working: Working::new(lark),
                cards: Arc::default(),
                people: Arc::default(),
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

    fn press_event(event_id: &str, open_id: &str, value: Value, form: Value) -> Value {
        json!({
            "schema": "2.0",
            "header": {"event_id": event_id, "event_type": "card.action.trigger", "token": "",
                       "create_time": "1700000000000000", "app_id": "a", "tenant_key": "t"},
            "event": {
                "operator": {"open_id": open_id, "union_id": format!("on_{open_id}"), "tenant_key": "t"},
                "token": "c-1",
                "action": {"tag": "button", "value": value, "form_value": form},
                "host": "im_message",
                "context": {"open_message_id": "om_card", "open_chat_id": "oc_1"}
            }
        })
    }

    async fn said(rx: &mut mpsc::Receiver<ChannelFrame>) -> ChannelInbound {
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ChannelFrame::Inbound(inb))) => inb,
            other => panic!("expected an inbound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_press_goes_on_once_as_its_person_typed_it() {
        let (incoming, mut rx) = incoming(None, None);
        incoming
            .cards
            .lock()
            .unwrap()
            .note("om_card", "Approve the step?");
        let value = json!({ "said": "/approve 7", "to": "lark:on_ou_a" });
        // Someone it was not sent to: refused, nothing goes on.
        let Taken::Answer(answer) =
            incoming.take(press_event("p0", "ou_x", value.clone(), json!({})))
        else {
            panic!("a press is answered");
        };
        assert_eq!(answer["toast"]["type"], "error");
        let Taken::Answer(answer) =
            incoming.take(press_event("p1", "ou_a", value.clone(), json!({})))
        else {
            panic!("a press is answered");
        };
        // The card without its buttons.
        assert_eq!(answer["card"]["type"], "raw");
        let elements = answer["card"]["data"]["body"]["elements"]
            .as_array()
            .unwrap();
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0]["content"], "Approve the step?");
        let inb = said(&mut rx).await;
        assert_eq!(inb.sender_id, "lark:on_ou_a");
        assert_eq!(inb.sender_aliases, ["lark:ou_a"]);
        // Its reply goes to the person, not as a reply to the card.
        assert_eq!(inb.channel_message_id, "press:om_card");
        let raw = inb.raw.as_ref().unwrap();
        assert!(
            raw["event"].get("token").is_none()
                && raw["event"]["operator"].get("tenant_key").is_none()
        );
        assert_eq!(
            inb.content,
            ChannelMessageContent::Text {
                text: "/approve 7".into()
            }
        );
        // Pressed again: once only.
        let Taken::Answer(answer) = incoming.take(press_event("p2", "ou_a", value, json!({})))
        else {
            panic!("a press is answered");
        };
        assert_eq!(answer["toast"]["type"], "info");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_card_pressed_outside_the_person_s_private_chat_is_not_taken() {
        let (incoming, mut rx) = incoming(None, None);
        // A message shows their private chat, oc_1.
        assert_eq!(incoming.take(message_event("m1", "om_m1", "")), Taken::Done);
        let _ = said(&mut rx).await;
        let value = json!({ "said": "/approve 7", "to": "lark:on_b" });
        // A copy of the card, which the daemon does not know, forwarded into
        // a group and pressed there.
        let mut forwarded = press_event("p1", "ou_a", value, json!({}));
        forwarded["event"]["operator"]["union_id"] = json!("on_b");
        forwarded["event"]["context"]["open_message_id"] = json!("om_copy");
        forwarded["event"]["context"]["open_chat_id"] = json!("oc_group");
        let Taken::Answer(answer) = incoming.take(forwarded) else {
            panic!("answered");
        };
        assert_eq!(answer["toast"]["type"], "error");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "nothing goes on");
        assert!(!incoming.cards.lock().unwrap().was_pressed("om_copy"));
    }

    #[tokio::test]
    async fn a_question_sends_back_its_answer_and_the_numbers_chosen() {
        let (incoming, mut rx) = incoming(None, None);
        let value = json!({ "kind": "question", "answer": "/answer q1", "to": "lark:on_ou_a" });
        let Taken::Answer(answer) = incoming.take(press_event(
            "q0",
            "ou_a",
            value.clone(),
            json!({ "choices": [] }),
        )) else {
            panic!("answered");
        };
        assert_eq!(answer["toast"]["type"], "warning");
        let Taken::Answer(answer) = incoming.take(press_event(
            "q1",
            "ou_a",
            value.clone(),
            json!({ "choices": ["1", "3"] }),
        )) else {
            panic!("answered");
        };
        assert_eq!(
            answer["toast"]["type"], "error",
            "a card it does not know, pressed where it does not know the person's chat"
        );
        // A message of theirs shows their private chat with the bot, oc_1:
        // a card it does not know (sent before a restart) is taken there.
        assert_eq!(incoming.take(message_event("m1", "om_m1", "")), Taken::Done);
        let _ = said(&mut rx).await;
        let mut theirs = press_event(
            "q2",
            "ou_a",
            value.clone(),
            json!({ "choices": ["1", "3"] }),
        );
        theirs["event"]["operator"]["union_id"] = json!("on_b");
        theirs["event"]["operator"]["open_id"] = json!("ou_a");
        let value = json!({ "kind": "question", "answer": "/answer q1", "to": "lark:on_b" });
        theirs["event"]["action"]["value"] = value.clone();
        let Taken::Answer(answer) = incoming.take(theirs) else {
            panic!("answered");
        };
        assert_eq!(
            answer,
            json!({}),
            "a card it does not know is left as it is"
        );
        let inb = said(&mut rx).await;
        assert_eq!(
            inb.content,
            ChannelMessageContent::Text {
                text: "/answer q1 1,3".into()
            }
        );
    }

    #[tokio::test]
    async fn an_item_of_the_bot_s_menu_goes_on_as_its_command() {
        let (incoming, mut rx) = incoming(None, None);
        let event = json!({
            "schema": "2.0",
            "header": {"event_id": "m1", "event_type": "application.bot.menu_v6", "token": "",
                       "create_time": "1700000000000", "app_id": "a", "tenant_key": "t"},
            "event": {"operator": {"operator_name": "A", "operator_id": {"open_id": "ou_a", "union_id": "on_a"}},
                      "event_key": "new", "timestamp": 1_700_000_000}
        });
        assert_eq!(incoming.take(event), Taken::Done);
        let inb = said(&mut rx).await;
        assert_eq!(
            inb.content,
            ChannelMessageContent::Text {
                text: "/new".into()
            }
        );
        assert_eq!(inb.channel_message_id, "menu:m1");
        assert_eq!(inb.sender_id, "lark:on_a");
        assert!(inb.raw.unwrap()["header"].get("tenant_key").is_none());
    }

    #[tokio::test]
    async fn a_reply_takes_the_reaction_off_its_chat_s_oldest_message() {
        let (incoming, _rx) = incoming(None, None);
        let working = &incoming.working;
        working.start("lark:on_a", "om_1", "om_1");
        working.start("lark:on_a", "om_2", "om_2");
        working.start("lark:on_a", "om_card", "press:om_card");
        working.start("lark:on_b", "om_3", "om_3");
        assert_eq!(working.waiting("lark:on_a"), ["om_1", "om_2", "om_card"]);
        // The reply to a press takes the card's off, and only that.
        working.replied("lark:on_a", Some("press:om_card"));
        assert_eq!(working.waiting("lark:on_a"), ["om_1", "om_2"]);
        // A reply to a message with none, as an item of the menu, takes none.
        working.replied("lark:on_a", Some("menu:ev_1"));
        assert_eq!(working.waiting("lark:on_a"), ["om_1", "om_2"]);
        working.replied("lark:on_a", Some("om_2"));
        assert_eq!(working.waiting("lark:on_a"), ["om_1"]);
        // One that names none: the oldest.
        working.replied("lark:on_a", None);
        working.replied("lark:on_a", None);
        assert_eq!(working.waiting("lark:on_a"), Vec::<String>::new());
        assert_eq!(working.waiting("lark:on_b"), ["om_3"]);
    }

    #[test]
    fn images_are_told_by_their_bytes() {
        assert_eq!(
            image_type(b"\x89PNG\r\n\x1a\nrest"),
            Some(("png", "image/png"))
        );
        assert_eq!(
            image_type(&[0xff, 0xd8, 0xff, 0xe0]),
            Some(("jpg", "image/jpeg"))
        );
        assert_eq!(image_type(b"GIF89a.."), Some(("gif", "image/gif")));
        assert_eq!(
            image_type(b"RIFF\0\0\0\0WEBPVP8 "),
            Some(("webp", "image/webp"))
        );
        assert_eq!(image_type(b"%PDF-1.7"), None);
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
