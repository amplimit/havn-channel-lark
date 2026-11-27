//! `havn-channel-lark` — bridges the Lark / Feishu IM API ↔ havn's
//! `/api/v1/channel` WebSocket protocol (spec §3).
//!
//! Inbound (Lark → havn) comes over Lark's long connection
//! ([`longconn`]), a WebSocket the daemon opens, or over a webhook HTTP
//! server ([`events`]); either way each event is taken once ([`events`]),
//! however often Lark sends it. Outbound (havn → Lark) drains the gateway
//! WS ([`havn_ws`]) and hands each reply to the worker of its chat, which
//! delivers that chat's replies in order through the IM send / reply API
//! ([`lark`]); the last reply delivered is kept, so a reconnect or a
//! restart resumes from it. A background task keeps the
//! `tenant_access_token` fresh.
//!
//! 1:1 (`p2p`) chats only. Inbound: text, voice (downloaded +
//! Groq/Whisper STT), location are native; image/file are surfaced as a
//! text marker (havn content is URL-based with no file host, and Lark
//! resources need the adapter's token). Outbound: text, image, file are
//! native (the agent's URL is fetched and re-uploaded to Lark).

#![forbid(unsafe_code)]

mod config;
mod events;
mod havn_ws;
mod lark;
mod longconn;
mod types;
mod whisper;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use clap::Parser;
use serde_json::json;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{Config, Events};
use crate::events::Incoming;
use crate::havn_ws::{WsClient, WsConfig};
use crate::lark::LarkClient;
use crate::types::{ChannelFrame, ChannelMessageContent, ChannelOutbound};
use crate::whisper::Whisper;

#[derive(Debug, Parser)]
#[command(
    name = "havn-channel-lark",
    about = "havn channel adapter for Lark/Feishu (spec §3)",
    version
)]
struct Cli {
    /// Path to the daemon's TOML config file (see README).
    #[arg(
        long,
        env = "HAVN_LARK_CONFIG",
        default_value = "/etc/havn-channel-lark/config.toml"
    )]
    config: PathBuf,
    /// Where the daemon keeps the last reply it delivered, to resume from
    /// after a restart. Defaults to `<account_id>.state.json` in systemd's
    /// `StateDirectory`, else next to the config file.
    #[arg(long, env = "HAVN_LARK_STATE")]
    state: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;
    info!(
        config = %cli.config.display(),
        gateway = %cfg.havn_gateway,
        lark = %cfg.lark_base_url,
        channel = %cfg.channel_id,
        account = %cfg.account_id,
        events = ?cfg.events,
        "havn-channel-lark starting"
    );

    let (lark, expire) = LarkClient::new(
        cfg.lark_base_url.clone(),
        cfg.app_id.clone(),
        cfg.app_secret.clone(),
    )
    .await
    .context("Lark client init")?;
    lark::spawn_token_refresher(lark.clone(), expire);

    let whisper = match cfg.openai_api_key.as_ref() {
        Some(k) if !k.is_empty() => Some(
            Whisper::new(
                k.clone(),
                cfg.whisper_base_url.clone(),
                cfg.whisper_model.clone(),
            )
            .context("Whisper init")?,
        ),
        _ => {
            info!("no openai_api_key configured — inbound voice messages will be dropped");
            None
        }
    };

    // The last reply delivered, to resume from.
    let state = cli.state.clone().or_else(|| {
        // systemd sets it to the unit's StateDirectory (the first, when
        // there are several).
        let dir = std::env::var("STATE_DIRECTORY")
            .ok()
            .and_then(|dirs| dirs.split(':').next().map(PathBuf::from))
            .filter(|dir| !dir.as_os_str().is_empty())
            .or_else(|| cli.config.parent().map(std::path::Path::to_path_buf))?;
        Some(havn_ws::Delivered::file_in(&dir, &cfg.account_id))
    });
    let delivered = Arc::new(havn_ws::Delivered::load(state, &cfg.account_id));
    let ws = WsClient::spawn(WsConfig {
        gateway_url: cfg.havn_gateway.clone(),
        adapter_token: cfg.adapter_token.clone(),
        channel_id: cfg.channel_id.clone(),
        account_id: cfg.account_id.clone(),
        delivered: Arc::clone(&delivered),
    });
    let WsClient {
        outbound_tx,
        inbound_rx,
    } = ws;

    // Inbound: Lark's events, by the transport configured, become
    // ChannelFrame::Inbound.
    let incoming = Incoming {
        lark: lark.clone(),
        whisper: whisper.clone(),
        outbound_tx: outbound_tx.clone(),
        account_id: cfg.account_id.clone(),
        seq: Arc::new(AtomicU64::new(1)),
        seen: Arc::default(),
        verification_token: cfg.verification_token.clone(),
        encrypt_key: cfg.encrypt_key.clone(),
    };
    let inbound_task = match cfg.events {
        Events::LongConnection => tokio::spawn(async move {
            longconn::run(
                longconn::LongConnConfig {
                    base_url: cfg.lark_base_url.clone(),
                    app_id: cfg.app_id.clone(),
                    app_secret: cfg.app_secret.clone(),
                },
                incoming,
            )
            .await;
            Ok(())
        }),
        Events::Webhook => {
            let listener = tokio::net::TcpListener::bind(&cfg.webhook_listen)
                .await
                .with_context(|| format!("binding webhook listener on {}", cfg.webhook_listen))?;
            info!(addr = %cfg.webhook_listen, "webhook server listening on POST /webhook");
            tokio::spawn(async move { axum::serve(listener, events::router(incoming)).await })
        }
    };

    // Outbound: havn → Lark.
    let outbound_task = tokio::spawn(havn_outbound_loop(lark.clone(), inbound_rx, delivered));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received Ctrl-C, shutting down"),
        r = inbound_task => warn!(result = ?r, "Lark events stopped coming unexpectedly"),
        r = outbound_task => warn!(result = ?r, "havn outbound loop exited unexpectedly"),
    }
    drop(outbound_tx);
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,havn_channel_lark=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

/// havn → Lark. Drain WS-received frames: each `Outbound` goes to the
/// worker of its chat ([`Chats`]), which delivers them in order. Other
/// frame kinds are logged and ignored.
async fn havn_outbound_loop(
    lark: Arc<LarkClient>,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
    delivered: Arc<havn_ws::Delivered>,
) -> Result<()> {
    let chats = Chats::new(lark, delivered);
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Outbound(o) => chats.outbound(o),
            ChannelFrame::Commands(c) => {
                info!(
                    commands = c.commands.len(),
                    sender_aliases = c.sender_aliases,
                    "the gateway's commands"
                );
            }
            ChannelFrame::Pong => {}
            ChannelFrame::Inbound(_) => {
                warn!("dropping Inbound frame from gateway (wrong direction)");
            }
            ChannelFrame::Resume(_) => {
                warn!("dropping Resume frame from gateway (wrong direction)");
            }
            ChannelFrame::Ping => warn!("Ping from gateway not yet handled; ignoring"),
        }
    }
    info!("havn outbound channel closed; outbound loop exiting");
    Ok(())
}

/// A worker whose chat had nothing for this long ends; the next reply for
/// its chat starts another.
const CHAT_IDLE: Duration = Duration::from_secs(240);
/// How often an idle worker looks whether it is to end.
const CHAT_IDLE_EVERY: Duration = Duration::from_secs(30);

/// Replies a chat's inbox holds at most, by number and by bytes. Past
/// them, as when Lark keeps a chat behind what an agent sends it, the
/// newest are left out (and marked handled), so one chat cannot take the
/// memory of a daemon that serves every one.
const CHAT_OUTBOUNDS: usize = 256;
const CHAT_OUTBOUND_BYTES: usize = 8 * 1024 * 1024;

/// What waits for a chat's worker, in the order it came.
#[derive(Default)]
struct Pending {
    outbounds: VecDeque<Box<ChannelOutbound>>,
    /// The bytes of `outbounds`, about.
    bytes: usize,
    /// Replies were left out since the backlog began: said once.
    full: bool,
}

/// A chat's replies on their way to its worker. The router only adds to
/// it, and never waits.
#[derive(Default)]
struct Inbox {
    pending: Mutex<Pending>,
    added: tokio::sync::Notify,
}

impl Inbox {
    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Add `o`; whether it was, which it is not past [`CHAT_OUTBOUNDS`] or
    /// [`CHAT_OUTBOUND_BYTES`].
    fn outbound(&self, o: ChannelOutbound) -> bool {
        {
            let mut pending = self.pending();
            let size = serde_json::to_vec(&o).map_or(0, |b| b.len());
            if pending.outbounds.len() >= CHAT_OUTBOUNDS
                || pending.bytes + size > CHAT_OUTBOUND_BYTES
            {
                if !std::mem::replace(&mut pending.full, true) {
                    warn!(
                        held = pending.outbounds.len(),
                        bytes = pending.bytes,
                        "lark: a chat holds as many replies as it may; leaving out the newest until it catches up"
                    );
                }
                return false;
            }
            pending.bytes += size;
            pending.outbounds.push_back(Box::new(o));
        }
        self.added.notify_one();
        true
    }

    fn take(&self) -> Option<Box<ChannelOutbound>> {
        let mut pending = self.pending();
        let o = pending.outbounds.pop_front()?;
        let size = serde_json::to_vec(&o).map_or(0, |b| b.len());
        pending.bytes = pending.bytes.saturating_sub(size);
        if pending.outbounds.is_empty() {
            pending.bytes = 0;
            pending.full = false;
        }
        Some(o)
    }

    fn is_empty(&self) -> bool {
        self.pending().outbounds.is_empty()
    }
}

/// The inboxes of the chats replies go to, by `to`, each with a worker.
type Inboxes = Arc<Mutex<HashMap<String, Arc<Inbox>>>>;

fn inboxes(all: &Inboxes) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Inbox>>> {
    all.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Deliver the replies for chat `to`, in the order they come. A wait in
/// one chat holds up no other, and a failure in one, a panic included,
/// only that one. Each reply is marked delivered once it is. It ends, its
/// inbox let go with it, when its chat had nothing for [`CHAT_IDLE`].
async fn chat_worker(
    lark: Arc<LarkClient>,
    to: String,
    inbox: Arc<Inbox>,
    all: Inboxes,
    delivered: Arc<havn_ws::Delivered>,
) {
    use futures_util::FutureExt as _;
    let mut idle = tokio::time::interval(CHAT_IDLE_EVERY);
    let mut last = tokio::time::Instant::now();
    loop {
        let Some(o) = inbox.take() else {
            tokio::select! {
                biased;
                () = inbox.added.notified() => {}
                _ = idle.tick() => {
                    if last.elapsed() >= CHAT_IDLE {
                        // With the router's lock: it adds to this inbox, or
                        // starts another worker, not both.
                        let mut map = inboxes(&all);
                        if inbox.is_empty() {
                            if map.get(&to).is_some_and(|i| Arc::ptr_eq(i, &inbox)) {
                                map.remove(&to);
                            }
                            return;
                        }
                    }
                }
            }
            continue;
        };
        let sent = std::panic::AssertUnwindSafe(deliver_outbound(&lark, &o))
            .catch_unwind()
            .await;
        match sent {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(error = %format!("{e:#}"), seq = o.seq, "Lark delivery failed; dropping");
            }
            Err(_) => tracing::error!(
                seq = o.seq,
                "lark: delivering a reply panicked; dropping it"
            ),
        }
        delivered.mark(o.seq);
        last = tokio::time::Instant::now();
    }
}

/// Where replies go: each chat's inbox, and its worker.
struct Chats {
    lark: Arc<LarkClient>,
    delivered: Arc<havn_ws::Delivered>,
    inboxes: Inboxes,
}

impl Chats {
    fn new(lark: Arc<LarkClient>, delivered: Arc<havn_ws::Delivered>) -> Self {
        Self {
            lark,
            delivered,
            inboxes: Inboxes::default(),
        }
    }

    /// Hand `o` to its chat's worker, without waiting; one sent before a
    /// restart (the gateway replays from an earlier reply of a slower chat)
    /// is not delivered again.
    fn outbound(&self, o: ChannelOutbound) {
        if self.delivered.delivered_already(o.seq) {
            self.delivered.mark(o.seq);
            return;
        }
        self.delivered.handed(o.seq);
        let seq = o.seq;
        let mut map = inboxes(&self.inboxes);
        let inbox = map.entry(o.to.clone()).or_insert_with(|| {
            let inbox = Arc::new(Inbox::default());
            tokio::spawn(chat_worker(
                Arc::clone(&self.lark),
                o.to.clone(),
                Arc::clone(&inbox),
                Arc::clone(&self.inboxes),
                Arc::clone(&self.delivered),
            ));
            inbox
        });
        let kept = inbox.outbound(o);
        drop(map);
        // Left out: it is not to be delivered after a restart either.
        if !kept {
            self.delivered.mark(seq);
        }
    }
}

/// Who an outbound `to` names, as Lark's `receive_id_type` and id: a
/// sender's `union_id` (`on_`) or, from an older gateway or a reply kept
/// from before, its `open_id` (`ou_`); a chat (`oc_`).
fn recipient(to: &str) -> Option<(&'static str, &str)> {
    let id = to.strip_prefix(events::PREFIX)?;
    let kind = if id.starts_with("on_") {
        "union_id"
    } else if id.starts_with("ou_") {
        "open_id"
    } else if id.starts_with("oc_") {
        "chat_id"
    } else {
        return None;
    };
    Some((kind, id))
}

async fn deliver_outbound(lark: &LarkClient, o: &ChannelOutbound) -> Result<()> {
    // The end of a reply that says nothing more sends nothing.
    if matches!(&o.content, ChannelMessageContent::Text { text } if text.trim().is_empty()) {
        return Ok(());
    }
    let reply_id = o.in_reply_to.as_deref();
    let to = recipient(&o.to);
    let (msg_type, content, extra_text) = build_outbound(lark, &o.content).await?;
    send_one(lark, reply_id, to, msg_type, &content).await?;
    if let Some(t) = extra_text {
        let c = serde_json::to_string(&json!({ "text": t }))?;
        send_one(lark, reply_id, to, "text", &c).await?;
    }
    Ok(())
}

/// Send one message, threading under `reply_id` if present, else to `to`.
async fn send_one(
    lark: &LarkClient,
    reply_id: Option<&str>,
    to: Option<(&str, &str)>,
    msg_type: &str,
    content: &str,
) -> Result<()> {
    if let Some(rid) = reply_id {
        lark.reply_message(rid, msg_type, content).await?;
    } else if let Some((kind, id)) = to {
        lark.send_message(kind, id, msg_type, content).await?;
    } else {
        bail!("outbound has no in_reply_to and a `to` naming no Lark user or chat; cannot route");
    }
    Ok(())
}

/// Build `(msg_type, content_json, optional_extra_text)` for an outbound
/// havn content. Image/File are native (fetch the URL, re-upload to Lark);
/// Audio/Location/Contact flatten to a text marker (matches the gateway's
/// own flattening, and what the Telegram adapter does).
async fn build_outbound(
    lark: &LarkClient,
    content: &ChannelMessageContent,
) -> Result<(&'static str, String, Option<String>)> {
    let s = |v: &serde_json::Value| serde_json::to_string(v).map_err(|e| anyhow!(e));
    match content {
        ChannelMessageContent::Text { text } => Ok(("text", s(&json!({ "text": text }))?, None)),
        ChannelMessageContent::Image { url, caption } => {
            let bytes = lark
                .fetch_url(url)
                .await
                .context("fetch outbound image url")?;
            let image_key = lark
                .upload_image(bytes)
                .await
                .context("upload image to Lark")?;
            // Lark image messages carry no caption; deliver it as a follow-up text.
            Ok((
                "image",
                s(&json!({ "image_key": image_key }))?,
                caption.clone(),
            ))
        }
        ChannelMessageContent::File { url, filename } => {
            let bytes = lark
                .fetch_url(url)
                .await
                .context("fetch outbound file url")?;
            let file_key = lark
                .upload_file(bytes, filename)
                .await
                .context("upload file to Lark")?;
            Ok(("file", s(&json!({ "file_key": file_key }))?, None))
        }
        ChannelMessageContent::Audio {
            transcript,
            duration_seconds,
            ..
        } => {
            let t = match duration_seconds {
                Some(d) => format!("[voice, {d}s] {transcript}"),
                None => format!("[voice] {transcript}"),
            };
            Ok(("text", s(&json!({ "text": t }))?, None))
        }
        ChannelMessageContent::Location {
            latitude,
            longitude,
            name,
            ..
        } => {
            let label = name.clone().unwrap_or_default();
            let coords = format!("{latitude:.6},{longitude:.6}");
            let t = if label.is_empty() {
                format!("[location: {coords}]")
            } else {
                format!("[location: {coords} ({label})]")
            };
            Ok(("text", s(&json!({ "text": t }))?, None))
        }
        ChannelMessageContent::Contact {
            display_name,
            phone_number,
            ..
        } => {
            let t = match phone_number {
                Some(ph) => format!("[contact: {display_name} {ph}]"),
                None => format!("[contact: {display_name}]"),
            };
            Ok(("text", s(&json!({ "text": t }))?, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outbound(seq: u64, to: &str, text: &str) -> ChannelOutbound {
        ChannelOutbound {
            seq,
            account_id: "bot".into(),
            to: to.into(),
            in_reply_to: None,
            content: ChannelMessageContent::Text { text: text.into() },
        }
    }

    #[test]
    fn a_reply_goes_to_the_id_its_to_names() {
        assert_eq!(recipient("lark:on_1"), Some(("union_id", "on_1")));
        assert_eq!(recipient("lark:ou_1"), Some(("open_id", "ou_1")));
        assert_eq!(recipient("lark:oc_1"), Some(("chat_id", "oc_1")));
        assert_eq!(recipient("lark:x"), None);
        assert_eq!(recipient("tg:1"), None);
    }

    #[test]
    fn a_chat_inbox_keeps_the_order_and_leaves_out_the_newest_past_its_cap() {
        let inbox = Inbox::default();
        for seq in 0..CHAT_OUTBOUNDS as u64 {
            assert!(inbox.outbound(outbound(seq, "lark:on_1", "x")));
        }
        assert!(!inbox.outbound(outbound(9999, "lark:on_1", "x")));
        assert_eq!(inbox.take().map(|o| o.seq), Some(0));
        assert_eq!(inbox.take().map(|o| o.seq), Some(1));
        while inbox.take().is_some() {}
        assert!(inbox.is_empty());
        assert!(!inbox.pending().full, "the backlog is over");
    }

    #[tokio::test]
    async fn replies_are_marked_delivered_in_their_chat_s_order() {
        let d = std::env::temp_dir().join(format!("havn-lark-chats-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        let path = havn_ws::Delivered::file_in(&d, "bot");
        let delivered = Arc::new(havn_ws::Delivered::load(Some(path.clone()), "bot"));
        // Lark is not there: each delivery fails, and is marked all the same.
        let lark = LarkClient::unauthenticated("http://127.0.0.1:9".into(), "a".into(), "s".into())
            .unwrap();
        let chats = Chats::new(lark, Arc::clone(&delivered));
        for (seq, to) in [(1, "lark:on_1"), (2, "lark:on_2"), (3, "lark:on_1")] {
            delivered.got(seq);
            chats.outbound(outbound(seq, to, "hi"));
        }
        assert_eq!(inboxes(&chats.inboxes).len(), 2, "a worker per chat");
        for _ in 0..200 {
            if havn_ws::Delivered::load(Some(path.clone()), "bot").since() == Some(3) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            havn_ws::Delivered::load(Some(path.clone()), "bot").since(),
            Some(3)
        );
        // A reply replayed after a restart that was delivered is not again.
        let again = havn_ws::Delivered::load(Some(path), "bot");
        assert!(!again.delivered_already(4));
        let _ = std::fs::remove_dir_all(&d);
    }
}
