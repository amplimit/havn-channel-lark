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
//! 1:1 (`p2p`) chats only. Inbound: text, rich text and locations as
//! such; images, files, voice messages and videos downloaded and given to
//! the gateway ([`gateway`]) as the message's attachments; a press on a
//! card's button and an item of the bot's menu as what the person typed.
//! Outbound: replies as cards ([`card`]) of the agent's Markdown, with the
//! gateway's buttons and questions; files fetched from the gateway and sent
//! as images or files, their captions after them; a `Typing` reaction on
//! each message while its reply is due.

#![forbid(unsafe_code)]

mod card;
mod config;
mod events;
mod gateway;
mod havn_ws;
mod lark;
mod longconn;
mod types;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use clap::Parser;
use serde_json::{Value, json};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{Config, Events};
use crate::events::Incoming;
use crate::gateway::Gateway;
use crate::havn_ws::{WsClient, WsConfig};
use crate::lark::{LarkClient, Sent};
use crate::types::{ChannelFrame, ChannelMessageContent, ChannelOutbound};

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
        why = cfg.events_why,
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

    say_unused(&cfg);
    let gateway = Gateway::new(
        &cfg.havn_gateway,
        &cfg.adapter_token,
        &cfg.channel_id,
        &cfg.account_id,
    )
    .context("havn gateway client init")?;
    let out = Out {
        lark: lark.clone(),
        gateway: gateway.clone(),
        working: events::Working::new(lark.clone()),
        cards: Arc::default(),
    };

    let delivered = Arc::new(havn_ws::Delivered::load(
        state_path(&cli, &cfg.account_id),
        &cfg.account_id,
    ));
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
        gateway,
        delivered: Arc::clone(&delivered),
        working: out.working.clone(),
        cards: Arc::clone(&out.cards),
        outbound_tx: outbound_tx.clone(),
        account_id: cfg.account_id.clone(),
        seq: Arc::new(AtomicU64::new(1)),
        seen: Arc::default(),
        chats: Arc::default(),
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
    let outbound_task = tokio::spawn(havn_outbound_loop(out, inbound_rx, delivered));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received Ctrl-C, shutting down"),
        r = inbound_task => warn!(result = ?r, "Lark events stopped coming unexpectedly"),
        r = outbound_task => warn!(result = ?r, "havn outbound loop exited unexpectedly"),
    }
    drop(outbound_tx);
    Ok(())
}

/// Say the settings a config may still have that are no longer used: voice
/// messages go to the gateway, which transcribes them (havn issue #164).
fn say_unused(cfg: &Config) {
    if cfg.openai_api_key.is_some() || cfg.whisper_base_url.is_some() || cfg.whisper_model.is_some()
    {
        warn!(
            "openai_api_key, whisper_base_url and whisper_model are no longer used: voice \
             messages go to the gateway, which transcribes them with [transcription] in its \
             config.toml"
        );
    }
}

/// Where the last reply delivered is kept, to resume from: `--state`, else
/// in systemd's `StateDirectory` (the first, when there are several), else
/// next to the config file.
fn state_path(cli: &Cli, account_id: &str) -> Option<PathBuf> {
    cli.state.clone().or_else(|| {
        let dir = std::env::var("STATE_DIRECTORY")
            .ok()
            .and_then(|dirs| dirs.split(':').next().map(PathBuf::from))
            .filter(|dir| !dir.as_os_str().is_empty())
            .or_else(|| cli.config.parent().map(std::path::Path::to_path_buf))?;
        Some(havn_ws::Delivered::file_in(&dir, account_id))
    })
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,havn_channel_lark=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

/// What delivering a reply takes.
#[derive(Clone)]
struct Out {
    lark: Arc<LarkClient>,
    gateway: Gateway,
    working: events::Working,
    cards: Arc<Mutex<card::Cards>>,
}

/// havn → Lark. Drain WS-received frames: each `Outbound` goes to the
/// worker of its chat ([`Chats`]), which delivers them in order. The
/// gateway's commands are said in the log, for the bot's menu, which Lark
/// lets be set up on the Developer Console only. Other frame kinds are
/// logged and ignored.
async fn havn_outbound_loop(
    out: Out,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
    delivered: Arc<havn_ws::Delivered>,
) -> Result<()> {
    let chats = Chats::new(out, delivered);
    // The commands last said, so a reconnect does not say them again.
    let mut menu: Option<Vec<types::ChannelCommand>> = None;
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Outbound(o) => chats.outbound(o),
            ChannelFrame::Commands(c) => {
                if menu.as_ref() != Some(&c.commands) {
                    let keys: Vec<&str> = c.commands.iter().map(|c| c.name.as_str()).collect();
                    info!(
                        commands = %keys.join(", "),
                        "the gateway's commands: on the Developer Console, give the bot's menu an \
                         item for each to offer, its event key the command's name"
                    );
                    menu = Some(c.commands);
                }
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
    out: Out,
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
        let sent = std::panic::AssertUnwindSafe(deliver_outbound(&out, &o))
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
        // A reply came, or the end of one: a message waits on one fewer. A
        // part of one has more to come.
        if !o.partial {
            out.working.replied(&o.to);
        }
        last = tokio::time::Instant::now();
    }
}

/// Where replies go: each chat's inbox, and its worker.
struct Chats {
    out: Out,
    delivered: Arc<havn_ws::Delivered>,
    inboxes: Inboxes,
}

impl Chats {
    fn new(out: Out, delivered: Arc<havn_ws::Delivered>) -> Self {
        Self {
            out,
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
                self.out.clone(),
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

/// Where a reply's messages go: the first as a reply to the message it
/// answers, when that is a message (an item of the bot's menu is none), the
/// rest to its recipient.
struct Target<'a> {
    reply_to: Option<&'a str>,
    to: Option<(&'static str, &'a str)>,
    /// The recipient as the gateway names it, which presses must come from.
    named: &'a str,
}

impl<'a> Target<'a> {
    fn of(o: &'a ChannelOutbound) -> Self {
        Self {
            reply_to: o.in_reply_to.as_deref().filter(|id| id.starts_with("om_")),
            to: recipient(&o.to),
            named: &o.to,
        }
    }

    /// Send one message, of `msg_type` and its `content` JSON.
    async fn send(&mut self, lark: &LarkClient, msg_type: &str, content: &Value) -> Result<Sent> {
        let content = content.to_string();
        let sent = match (self.reply_to, self.to) {
            (Some(message_id), _) => lark.reply_message(message_id, msg_type, &content).await?,
            (None, Some((kind, id))) => lark.send_message(kind, id, msg_type, &content).await?,
            (None, None) => {
                bail!(
                    "outbound has no in_reply_to and a `to` naming no Lark user or chat; cannot route"
                )
            }
        };
        // The rest go to the recipient, when it can be named.
        if self.to.is_some() {
            self.reply_to = None;
        }
        Ok(sent)
    }
}

/// Whether Lark answered and refused a message, as for a card it does not
/// take: one sent again some other way does not come twice.
fn refused(e: &anyhow::Error) -> bool {
    e.downcast_ref::<lark::Refused>().is_some()
}

/// Deliver one reply: text as cards (with the gateway's buttons under the
/// last), a question of several answers as a card with a menu, and a file
/// fetched from the gateway and sent as an image or a file, its caption in
/// a card after it. Audio, locations and contacts go as text markers.
async fn deliver_outbound(out: &Out, o: &ChannelOutbound) -> Result<()> {
    let mut target = Target::of(o);
    let text = match &o.content {
        ChannelMessageContent::Text { text } => text.clone(),
        ChannelMessageContent::Image { url, caption } => {
            match out.gateway.fetch(url).await {
                Ok(bytes) => match send_file(out, &mut target, bytes, "image").await {
                    Ok(()) => caption.clone().unwrap_or_default(),
                    Err(e) => {
                        warn!(error = %format!("{e:#}"), "lark could not take an image");
                        "(An image could not be delivered.)".to_string()
                    }
                },
                // An address of the agent's own: the adapter fetches only the
                // gateway's files.
                Err(_) => match caption {
                    Some(c) => format!("[image: {url}]\n{c}"),
                    None => format!("[image: {url}]"),
                },
            }
        }
        ChannelMessageContent::File {
            url,
            filename,
            caption,
        } => match out.gateway.fetch(url).await {
            Ok(bytes) => match send_file(out, &mut target, bytes, filename).await {
                Ok(()) => caption.clone().unwrap_or_default(),
                Err(e) => {
                    warn!(filename, error = %format!("{e:#}"), "lark could not take a file");
                    format!("(A file, {filename}, could not be delivered.)")
                }
            },
            Err(e) => {
                warn!(filename, error = %format!("{e:#}"), "could not fetch a file the agent sent");
                format!("(A file, {filename}, could not be delivered.)")
            }
        },
        ChannelMessageContent::Audio {
            transcript,
            duration_seconds,
            ..
        } => match duration_seconds {
            Some(d) => format!("[voice, {d}s] {transcript}"),
            None => format!("[voice] {transcript}"),
        },
        ChannelMessageContent::Location {
            latitude,
            longitude,
            name,
            ..
        } => {
            let label = name.clone().unwrap_or_default();
            let coords = format!("{latitude:.6},{longitude:.6}");
            if label.is_empty() {
                format!("[location: {coords}]")
            } else {
                format!("[location: {coords} ({label})]")
            }
        }
        ChannelMessageContent::Contact {
            display_name,
            phone_number,
            ..
        } => match phone_number {
            Some(ph) => format!("[contact: {display_name} {ph}]"),
            None => format!("[contact: {display_name}]"),
        },
    };
    // A question of several answers: its text with a menu; its text alone,
    // which says how to answer by typing, when Lark refuses the card.
    if let Some(poll) = &o.poll {
        let markdown = if text.trim().is_empty() {
            poll.question.clone()
        } else {
            text.clone()
        };
        if markdown.chars().count() <= card::PART_CHARS
            && let Some(question) = card::question_card(&markdown, poll, target.named)
        {
            match target.send(&out.lark, "interactive", &question).await {
                Ok(sent) => {
                    note_card(out, &sent, &markdown);
                    return Ok(());
                }
                Err(e) if refused(&e) => {
                    warn!(error = %format!("{e:#}"), "lark refused a question's card; sending its text");
                }
                Err(e) => return Err(e),
            }
        }
    }
    // The end of a reply whose parts were all sent already says nothing
    // more (havn issue #212): it only ends the turn.
    if text.trim().is_empty() && o.buttons.is_empty() {
        return Ok(());
    }
    send_text(out, &mut target, &text, &o.buttons).await
}

/// Send `markdown` as cards, as many as it takes, the buttons under the
/// last. A card Lark refuses goes as plain text, without buttons: the text
/// says how to answer by typing.
async fn send_text(
    out: &Out,
    target: &mut Target<'_>,
    markdown: &str,
    buttons: &[Vec<types::Button>],
) -> Result<()> {
    let mut parts = card::split(markdown);
    if parts.is_empty() {
        parts.push(String::new());
    }
    let last = parts.len() - 1;
    for (i, part) in parts.iter().enumerate() {
        let under = if i == last { buttons } else { &[] };
        let shown = card::text_card(part, under, target.named);
        match target.send(&out.lark, "interactive", &shown).await {
            Ok(sent) => {
                if !under.is_empty() {
                    note_card(out, &sent, part);
                }
            }
            Err(e) if refused(&e) => {
                warn!(error = %format!("{e:#}"), "lark refused a card; sending its text as it is");
                let plain = json!({ "text": card::plain(part) });
                target.send(&out.lark, "text", &plain).await?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// A card with buttons or a question was sent: kept, to show it without
/// them once pressed.
fn note_card(out: &Out, sent: &Sent, markdown: &str) {
    out.cards
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .note(&sent.message_id, markdown);
}

/// The largest image Lark takes as one.
const IMAGE_MAX_BYTES: usize = 10 * 1024 * 1024;
/// The largest file Lark takes.
const FILE_MAX_BYTES: usize = 30 * 1024 * 1024;

/// Send `bytes`, a file the agent sent named `filename`: as an image Lark
/// shows when its bytes are one (PNG, JPEG, GIF, WebP, at most 10 MB), else,
/// or when Lark refuses it as one, as a file.
async fn send_file(
    out: &Out,
    target: &mut Target<'_>,
    bytes: Vec<u8>,
    filename: &str,
) -> Result<()> {
    if events::image_type(&bytes).is_some() && bytes.len() <= IMAGE_MAX_BYTES {
        let shown = match out.lark.upload_image(bytes.clone()).await {
            Ok(image_key) => {
                target
                    .send(&out.lark, "image", &json!({ "image_key": image_key }))
                    .await
            }
            Err(e) => Err(e),
        };
        match shown {
            Ok(_) => return Ok(()),
            Err(e) if refused(&e) || format!("{e:#}").contains("image upload failed") => {
                warn!(filename, error = %format!("{e:#}"), "lark refused an image; sending it as a file");
            }
            Err(e) => return Err(e),
        }
    }
    if bytes.len() > FILE_MAX_BYTES {
        bail!("the file is larger than Lark's 30 MB");
    }
    let name = if filename.contains('.') || events::image_type(&bytes).is_none() {
        filename.to_string()
    } else {
        format!(
            "{filename}.{}",
            events::image_type(&bytes).map_or("bin", |(e, _)| e)
        )
    };
    let file_key = out
        .lark
        .upload_file(bytes, &name, file_type(&name))
        .await
        .context("upload file to Lark")?;
    target
        .send(&out.lark, "file", &json!({ "file_key": file_key }))
        .await?;
    Ok(())
}

/// Lark's `file_type` for a file named `name`.
fn file_type(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "opus" => "opus",
        "mp4" => "mp4",
        "pdf" => "pdf",
        "doc" | "docx" => "doc",
        "xls" | "xlsx" => "xls",
        "ppt" | "pptx" => "ppt",
        _ => "stream",
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
            buttons: Vec::new(),
            poll: None,
            partial: false,
        }
    }

    fn out() -> Out {
        // Lark is not there: each delivery fails.
        let lark = LarkClient::unauthenticated("http://127.0.0.1:9".into(), "a".into(), "s".into())
            .unwrap();
        Out {
            gateway: gateway::Gateway::new("ws://127.0.0.1:9", "t", "lark", "bot").unwrap(),
            working: events::Working::new(Arc::clone(&lark)),
            cards: Arc::default(),
            lark,
        }
    }

    #[test]
    fn a_reply_goes_to_the_id_its_to_names() {
        assert_eq!(recipient("lark:on_1"), Some(("union_id", "on_1")));
        assert_eq!(recipient("lark:ou_1"), Some(("open_id", "ou_1")));
        assert_eq!(recipient("lark:oc_1"), Some(("chat_id", "oc_1")));
        assert_eq!(recipient("lark:x"), None);
        assert_eq!(recipient("tg:1"), None);
        // A reply to an item of the bot's menu, which is no message, goes
        // to the person.
        let mut o = outbound(1, "lark:on_1", "x");
        o.in_reply_to = Some("menu:ev_1".into());
        assert_eq!(Target::of(&o).reply_to, None);
        o.in_reply_to = Some("om_1".into());
        assert_eq!(Target::of(&o).reply_to, Some("om_1"));
    }

    #[test]
    fn a_file_goes_as_lark_s_kind_of_it() {
        assert_eq!(file_type("report.PDF"), "pdf");
        assert_eq!(file_type("a.docx"), "doc");
        assert_eq!(file_type("sheet.xlsx"), "xls");
        assert_eq!(file_type("talk.opus"), "opus");
        assert_eq!(file_type("archive.tar.gz"), "stream");
        assert_eq!(file_type("noextension"), "stream");
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
        let chats = Chats::new(out(), Arc::clone(&delivered));
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
