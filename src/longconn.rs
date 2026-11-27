//! Lark's long connection: the app's events over a WebSocket the daemon
//! opens to Lark (Feishu's 长连接), so no public URL is needed. It is the
//! protocol of Lark's own SDKs:
//!
//! - `POST <lark_base_url>/callback/ws/endpoint` with the app's id and
//!   secret gives a `wss://` URL to connect to, good for one connection,
//!   and the client settings (how often to ping, how long to wait before
//!   connecting again);
//! - every message is a binary protobuf [`Frame`]: a control frame (`ping`
//!   from the daemon, `pong` from Lark, which may carry new settings), or a
//!   data frame carrying an event's JSON, cut in parts when it is large;
//! - each event is answered with its own frame, payload `{"code":200}`,
//!   within 3 seconds, or Lark sends it again (which [`Incoming`] drops); a
//!   press on a card (`card.action.trigger`) with what Lark is to do, the
//!   JSON in the payload's `data`, base64, as Lark's SDKs send it.
//!
//! The connection is opened again, with a fresh URL, whenever it ends: the
//! first time after a random wait of up to the settings' `ReconnectNonce`,
//! then every `ReconnectInterval`, without end. While Lark refuses the app
//! (wrong id or secret, the long connection not chosen, too many
//! connections), the wait doubles each time, up to [`REFUSED_WAIT_MAX`].

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use futures_util::{SinkExt as _, StreamExt as _};
use prost::Message as _;
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};

use crate::events::{Incoming, Taken};

const ENDPOINT_PATH: &str = "/callback/ws/endpoint";

/// A frame's `method`.
const CONTROL: i32 = 0;
const DATA: i32 = 1;

/// The settings until Lark gives its own, as its SDKs have them.
const PING_EVERY: Duration = Duration::from_secs(120);
const RECONNECT_EVERY: Duration = Duration::from_secs(120);
const RECONNECT_NONCE: Duration = Duration::from_secs(30);
/// A connection that heard nothing for two pings and this is taken for
/// lost (a peer gone without closing).
const PONG_GRACE: Duration = Duration::from_secs(5);
/// A write that takes longer ends the connection.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// The parts of an event cut in parts wait this long for the rest.
const PARTS_WAIT: Duration = Duration::from_secs(5);
/// Events waiting for their parts at most; past that, the oldest go.
const PARTS_KEPT: usize = 64;
/// The parts an event may be cut in, and the bytes it may have put
/// together, at most: past them it is dropped.
const PARTS_MAX: usize = 64;
const EVENT_BYTES_MAX: usize = 4 * 1024 * 1024;
/// The longest wait between connections while Lark refuses the app.
const REFUSED_WAIT_MAX: Duration = Duration::from_mins(30);
/// Lark's codes for a refusal to retry as any failure: busy, or its own
/// error (`ws/const.go` of its Go SDK).
const SYSTEM_BUSY: i64 = 1;
const INTERNAL_ERROR: i64 = 1_000_040_343;
/// The handshake statuses that refuse the app: forbidden, or the app's
/// credentials or connections refused.
const FORBIDDEN: &str = "403";
const AUTH_FAILED: &str = "514";

/// Lark refused the app itself, not the moment: waiting longer helps.
#[derive(Debug)]
struct Refused(String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// One message of the long connection (Lark's `pbbp2.Frame`).
#[derive(Clone, PartialEq, prost::Message)]
pub struct Frame {
    #[prost(uint64, required, tag = "1")]
    pub seq_id: u64,
    #[prost(uint64, required, tag = "2")]
    pub log_id: u64,
    #[prost(int32, required, tag = "3")]
    pub service: i32,
    #[prost(int32, required, tag = "4")]
    pub method: i32,
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<Header>,
    #[prost(string, optional, tag = "6")]
    pub payload_encoding: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub payload_type: Option<String>,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub payload: Option<Vec<u8>>,
    #[prost(string, optional, tag = "9")]
    pub log_id_new: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Header {
    #[prost(string, required, tag = "1")]
    pub key: String,
    #[prost(string, required, tag = "2")]
    pub value: String,
}

impl Frame {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.value.as_str())
    }

    fn number(&self, key: &str) -> usize {
        self.header(key).and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    fn ping(service: i32) -> Self {
        Self {
            service,
            method: CONTROL,
            headers: vec![Header {
                key: "type".into(),
                value: "ping".into(),
            }],
            ..Self::default()
        }
    }

    /// The answer to this data frame: itself, with how long it took and
    /// `code` in place of its payload, and `data` when there is something
    /// to say back.
    fn answer(mut self, code: u16, data: Option<&serde_json::Value>, took: Duration) -> Self {
        use base64::Engine as _;
        self.headers.push(Header {
            key: "biz_rt".into(),
            value: took.as_millis().to_string(),
        });
        let data = data.map(|d| base64::engine::general_purpose::STANDARD.encode(d.to_string()));
        self.payload = Some(
            json!({ "code": code, "headers": null, "data": data })
                .to_string()
                .into_bytes(),
        );
        self
    }
}

/// Where the long connection is opened from.
#[derive(Clone)]
pub struct LongConnConfig {
    pub base_url: String,
    pub app_id: String,
    pub app_secret: String,
}

/// The settings Lark gives a client, in seconds.
#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ClientConfig {
    #[serde(default)]
    reconnect_interval: i64,
    #[serde(default)]
    reconnect_nonce: i64,
    #[serde(default)]
    ping_interval: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Settings {
    ping: Duration,
    reconnect: Duration,
    nonce: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            ping: PING_EVERY,
            reconnect: RECONNECT_EVERY,
            nonce: RECONNECT_NONCE,
        }
    }
}

impl Settings {
    /// What Lark gave of them, in their place; a value it left out or
    /// gave as 0 stays.
    fn apply(&mut self, given: ClientConfig) {
        let secs = |s: i64| {
            u64::try_from(s)
                .ok()
                .filter(|s| *s > 0)
                .map(Duration::from_secs)
        };
        if let Some(ping) = secs(given.ping_interval) {
            self.ping = ping;
        }
        if let Some(reconnect) = secs(given.reconnect_interval) {
            self.reconnect = reconnect;
        }
        if let Some(nonce) = secs(given.reconnect_nonce) {
            self.nonce = nonce;
        }
    }
}

/// Keep the long connection open, for the life of the daemon.
pub async fn run(cfg: LongConnConfig, incoming: Incoming) {
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "lark long connection: no HTTP client; events will not come");
            return;
        }
    };
    let mut settings = Settings::default();
    let mut failed = 0u32;
    let mut refused = 0u32;
    loop {
        match connect(&http, &cfg, &mut settings).await {
            Ok((ws, service)) => {
                info!(lark = %cfg.base_url, "lark long connection open");
                failed = 0;
                refused = 0;
                let ended = session(ws, service, &mut settings, &incoming).await;
                warn!(error = %ended, "lark long connection ended; opening it again");
            }
            Err(e) => {
                if e.downcast_ref::<Refused>().is_some() {
                    refused = refused.saturating_add(1);
                }
                warn!(error = %format!("{e:#}"), "lark long connection could not open");
            }
        }
        let wait = next_wait(&settings, failed, refused);
        failed = failed.saturating_add(1);
        tokio::time::sleep(wait).await;
    }
}

/// How long to wait before connecting again, after `failed` failures in a
/// row, `refused` of them Lark's refusals of the app.
fn next_wait(settings: &Settings, failed: u32, refused: u32) -> Duration {
    if refused > 0 {
        let times = 1u32 << (refused - 1).min(16);
        return settings
            .reconnect
            .saturating_mul(times)
            .min(REFUSED_WAIT_MAX);
    }
    if failed == 0 {
        random_up_to(settings.nonce)
    } else {
        settings.reconnect
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Ask Lark where to connect, then connect: the socket and the service id
/// pings name.
async fn connect(
    http: &reqwest::Client,
    cfg: &LongConnConfig,
    settings: &mut Settings,
) -> Result<(Socket, i32)> {
    #[derive(Deserialize)]
    struct EndpointResp {
        code: i64,
        #[serde(default)]
        msg: String,
        #[serde(default)]
        data: Option<Endpoint>,
    }
    #[derive(Deserialize)]
    struct Endpoint {
        #[serde(rename = "URL", default)]
        url: String,
        #[serde(rename = "ClientConfig", default)]
        client_config: Option<ClientConfig>,
    }
    let resp = http
        .post(format!(
            "{}{ENDPOINT_PATH}",
            cfg.base_url.trim_end_matches('/')
        ))
        .header("locale", "zh")
        .json(&json!({ "AppID": cfg.app_id, "AppSecret": cfg.app_secret }))
        .send()
        .await
        .context("asking Lark for the long connection's address")?;
    let status = resp.status();
    let body: EndpointResp = resp
        .json()
        .await
        .with_context(|| format!("Lark's answer for the long connection (HTTP {status})"))?;
    if matches!(body.code, SYSTEM_BUSY | INTERNAL_ERROR) {
        bail!("Lark is busy: code={} msg={}", body.code, body.msg);
    }
    if body.code != 0 {
        return Err(Refused(format!(
            "Lark refused the long connection: code={} msg={} (check the app id and secret, and \
             that the app receives events through the long connection on the Developer Console)",
            body.code, body.msg
        ))
        .into());
    }
    let endpoint = body
        .data
        .filter(|d| !d.url.is_empty())
        .context("Lark gave no address for the long connection")?;
    if let Some(given) = endpoint.client_config {
        settings.apply(given);
    }
    let url = url::Url::parse(&endpoint.url).context("the long connection's address")?;
    // Its query holds the connection's ticket: only the host is shown.
    let host = url.host_str().unwrap_or_default().to_string();
    let service = url
        .query_pairs()
        .find(|(k, _)| k == "service_id")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    match tokio_tungstenite::connect_async(endpoint.url.as_str()).await {
        Ok((ws, _)) => Ok((ws, service)),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            let header = |name: &str| {
                resp.headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string()
            };
            let status = header("Handshake-Status");
            let said = format!(
                "{host} refused the long connection: HTTP {}, status {status} {} (auth code {})",
                resp.status(),
                header("Handshake-Msg"),
                header("Handshake-Autherrcode"),
            );
            if status == FORBIDDEN
                || status == AUTH_FAILED
                || resp.status() == reqwest::StatusCode::FORBIDDEN.as_u16()
            {
                return Err(Refused(said).into());
            }
            bail!(said)
        }
        Err(e) => Err(e).with_context(|| format!("connecting to {host}")),
    }
}

/// Run one connection until it ends: why it did.
async fn session(
    ws: Socket,
    service: i32,
    settings: &mut Settings,
    incoming: &Incoming,
) -> anyhow::Error {
    let (mut sink, mut stream) = ws.split();
    let mut every = settings.ping;
    // The first ping goes at once, as Lark's SDKs send it.
    let mut ping = pings(every, tokio::time::Instant::now());
    let mut parts = Parts::default();
    let mut heard = Instant::now();
    loop {
        let outgoing = tokio::select! {
            _ = ping.tick() => {
                if heard.elapsed() > every * 2 + PONG_GRACE {
                    return anyhow::anyhow!("nothing from Lark for {}s", heard.elapsed().as_secs());
                }
                Frame::ping(service)
            }
            msg = stream.next() => {
                let bytes = match msg {
                    Some(Ok(Message::Binary(b))) => b,
                    Some(Ok(Message::Close(c))) => {
                        return anyhow::anyhow!("Lark closed it: {c:?}");
                    }
                    Some(Ok(_)) => {
                        heard = Instant::now();
                        continue;
                    }
                    Some(Err(e)) => return e.into(),
                    None => return anyhow::anyhow!("Lark closed it"),
                };
                heard = Instant::now();
                let frame = match Frame::decode(bytes.as_ref()) {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(error = %e, "lark long connection: a frame could not be read");
                        continue;
                    }
                };
                let handled = handle(frame, settings, &mut parts, incoming);
                // A pong may bring another ping interval.
                if settings.ping != every {
                    every = settings.ping;
                    ping = pings(every, tokio::time::Instant::now() + every);
                }
                match handled {
                    Handled::Answer(answer) => answer,
                    Handled::Nothing => continue,
                }
            }
        };
        let bytes = outgoing.encode_to_vec();
        match tokio::time::timeout(WRITE_TIMEOUT, sink.send(Message::Binary(bytes.into()))).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return e.into(),
            Err(_) => {
                return anyhow::anyhow!("a write to Lark took over {}s", WRITE_TIMEOUT.as_secs());
            }
        }
    }
}

fn pings(every: Duration, first: tokio::time::Instant) -> tokio::time::Interval {
    let mut ping = tokio::time::interval_at(first, every);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping
}

/// What a frame from Lark calls for.
#[derive(Debug)]
enum Handled {
    Answer(Frame),
    Nothing,
}

fn handle(
    frame: Frame,
    settings: &mut Settings,
    parts: &mut Parts,
    incoming: &Incoming,
) -> Handled {
    let started = Instant::now();
    match frame.method {
        CONTROL => {
            if frame.header("type") == Some("pong")
                && let Some(payload) = frame.payload.as_deref().filter(|p| !p.is_empty())
            {
                match serde_json::from_slice::<ClientConfig>(payload) {
                    Ok(given) => settings.apply(given),
                    Err(e) => debug!(error = %e, "lark long connection: settings not read"),
                }
            }
            Handled::Nothing
        }
        DATA => {
            let Some(payload) = parts.whole(&frame) else {
                return Handled::Nothing;
            };
            // A card's press may come as a frame of its own kind.
            let (code, data) = match frame.header("type") {
                Some("event" | "card") => match serde_json::from_slice(&payload) {
                    Ok(event) => match incoming.take(event) {
                        Taken::Done => (200, None),
                        Taken::Answer(answer) => (200, Some(answer)),
                        Taken::Refused => (403, None),
                        Taken::Unreadable => (500, None),
                    },
                    Err(e) => {
                        warn!(error = %e, "lark long connection: an event is not JSON");
                        (500, None)
                    }
                },
                kind => {
                    debug!(kind, "lark long connection: not an event; answered");
                    (200, None)
                }
            };
            Handled::Answer(frame.answer(code, data.as_ref(), started.elapsed()))
        }
        method => {
            debug!(method, "lark long connection: a frame of an unknown method");
            Handled::Nothing
        }
    }
}

/// Events cut in parts, waiting for the rest, by message id: since when,
/// and the parts that came.
#[derive(Default)]
struct Parts {
    waiting: HashMap<String, Waiting>,
}

type Waiting = (Instant, Vec<Option<Vec<u8>>>);

impl Parts {
    /// The whole payload of `frame`'s event, once every part of it came.
    fn whole(&mut self, frame: &Frame) -> Option<Vec<u8>> {
        let payload = frame.payload.clone().unwrap_or_default();
        let sum = frame.number("sum");
        if sum <= 1 {
            return Some(payload);
        }
        if sum > PARTS_MAX {
            warn!(
                sum,
                "lark long connection: an event in too many parts; dropping it"
            );
            return None;
        }
        let seq = frame.number("seq");
        if seq >= sum {
            warn!(
                seq,
                sum, "lark long connection: a part past the last; dropping it"
            );
            return None;
        }
        let id = frame.header("message_id").unwrap_or_default().to_string();
        let now = Instant::now();
        self.waiting
            .retain(|_, (since, _)| now.duration_since(*since) < PARTS_WAIT);
        while self.waiting.len() >= PARTS_KEPT && !self.waiting.contains_key(&id) {
            let oldest = self
                .waiting
                .iter()
                .min_by_key(|(_, (since, _))| *since)
                .map(|(k, _)| k.clone())?;
            self.waiting.remove(&oldest);
        }
        let entry = self
            .waiting
            .entry(id.clone())
            .or_insert_with(|| (now, vec![None; sum]));
        if entry.1.len() != sum {
            entry.1 = vec![None; sum];
        }
        entry.1[seq] = Some(payload);
        let bytes: usize = entry.1.iter().flatten().map(Vec::len).sum();
        if bytes > EVENT_BYTES_MAX {
            warn!(
                bytes,
                "lark long connection: an event too large; dropping it"
            );
            self.waiting.remove(&id);
            return None;
        }
        if entry.1.iter().any(Option::is_none) {
            return None;
        }
        let (_, all) = self.waiting.remove(&id)?;
        Some(all.into_iter().flatten().flatten().collect())
    }
}

/// A wait of up to `max`, different each time.
fn random_up_to(max: Duration) -> Duration {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    let millis = u64::try_from(max.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(hasher.finish() % millis.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(headers: &[(&str, &str)], payload: &[u8]) -> Frame {
        Frame {
            seq_id: 5,
            log_id: 9,
            service: 3,
            method: DATA,
            headers: headers
                .iter()
                .map(|(k, v)| Header {
                    key: (*k).into(),
                    value: (*v).into(),
                })
                .collect(),
            payload: Some(payload.to_vec()),
            ..Frame::default()
        }
    }

    #[test]
    fn a_ping_is_encoded_as_lark_s_sdks_do() {
        // Every required field is written, even when 0.
        let bytes = Frame::ping(7).encode_to_vec();
        assert_eq!(
            bytes,
            [
                0x08, 0x00, 0x10, 0x00, 0x18, 0x07, 0x20, 0x00, 0x2a, 0x0c, 0x0a, 0x04, b't', b'y',
                b'p', b'e', 0x12, 0x04, b'p', b'i', b'n', b'g'
            ]
        );
        let back = Frame::decode(bytes.as_slice()).unwrap();
        assert_eq!(back.header("type"), Some("ping"));
        assert_eq!(back.service, 7);
    }

    #[test]
    fn an_answer_is_the_frame_with_its_code() {
        use base64::Engine as _;
        let frame = data(&[("type", "event"), ("message_id", "m1")], b"{}");
        let answer = frame.clone().answer(200, None, Duration::from_millis(12));
        assert_eq!((answer.seq_id, answer.log_id, answer.service), (5, 9, 3));
        assert_eq!(answer.header("message_id"), Some("m1"));
        assert_eq!(answer.header("biz_rt"), Some("12"));
        let payload: serde_json::Value =
            serde_json::from_slice(answer.payload.as_deref().unwrap()).unwrap();
        assert_eq!(payload["code"], 200);
        // What a press is answered with, as Lark's SDKs send it.
        let said = json!({ "toast": { "type": "info", "content": "ok" } });
        let answer = frame.answer(200, Some(&said), Duration::ZERO);
        let payload: serde_json::Value =
            serde_json::from_slice(answer.payload.as_deref().unwrap()).unwrap();
        let data = base64::engine::general_purpose::STANDARD
            .decode(payload["data"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&data).unwrap(),
            said
        );
    }

    #[test]
    fn an_event_in_parts_is_whole_once_every_part_came() {
        let mut parts = Parts::default();
        let part = |seq: &str, bytes: &[u8]| {
            data(
                &[
                    ("type", "event"),
                    ("message_id", "m1"),
                    ("sum", "3"),
                    ("seq", seq),
                ],
                bytes,
            )
        };
        assert_eq!(parts.whole(&part("2", b"c")), None);
        assert_eq!(parts.whole(&part("0", b"a")), None);
        assert_eq!(parts.whole(&part("1", b"b")), Some(b"abc".to_vec()));
        assert!(parts.waiting.is_empty());
        // One not cut goes as it is.
        assert_eq!(
            parts.whole(&data(&[("type", "event")], b"x")),
            Some(b"x".to_vec())
        );
        // A part past the last is dropped.
        assert_eq!(parts.whole(&part("3", b"d")), None);
    }

    #[test]
    fn an_event_in_too_many_parts_or_too_large_is_dropped() {
        let mut parts = Parts::default();
        let many = data(&[("message_id", "m"), ("sum", "65"), ("seq", "0")], b"a");
        assert_eq!(parts.whole(&many), None);
        assert!(parts.waiting.is_empty(), "nothing kept of it");
        let big = vec![b'x'; EVENT_BYTES_MAX / 2 + 1];
        let part = |seq: &str| data(&[("message_id", "m"), ("sum", "3"), ("seq", seq)], &big);
        assert_eq!(parts.whole(&part("0")), None);
        assert_eq!(parts.whole(&part("1")), None);
        assert!(parts.waiting.is_empty(), "dropped once past the bound");
        assert_eq!(parts.whole(&part("2")), None);
    }

    #[test]
    fn the_wait_doubles_while_lark_refuses_the_app() {
        let settings = Settings {
            ping: PING_EVERY,
            reconnect: Duration::from_secs(10),
            nonce: Duration::from_secs(2),
        };
        assert!(next_wait(&settings, 0, 0) <= Duration::from_secs(2));
        assert_eq!(next_wait(&settings, 3, 0), Duration::from_secs(10));
        assert_eq!(next_wait(&settings, 1, 1), Duration::from_secs(10));
        assert_eq!(next_wait(&settings, 2, 2), Duration::from_secs(20));
        assert_eq!(next_wait(&settings, 4, 4), Duration::from_secs(80));
        assert_eq!(next_wait(&settings, 99, 99), REFUSED_WAIT_MAX);
    }

    #[test]
    fn lark_s_settings_take_the_place_of_the_defaults() {
        let mut settings = Settings::default();
        let given: ClientConfig = serde_json::from_str(
            r#"{"ReconnectCount":-1,"ReconnectInterval":10,"ReconnectNonce":0,"PingInterval":30}"#,
        )
        .unwrap();
        settings.apply(given);
        assert_eq!(settings.ping, Duration::from_secs(30));
        assert_eq!(settings.reconnect, Duration::from_secs(10));
        assert_eq!(settings.nonce, RECONNECT_NONCE, "0 leaves it as it was");
    }

    #[test]
    fn a_random_wait_stays_within_its_bound() {
        for _ in 0..100 {
            assert!(random_up_to(Duration::from_secs(2)) <= Duration::from_secs(2));
        }
        assert_eq!(random_up_to(Duration::ZERO), Duration::ZERO);
    }
}
