//! `havn-channel-lark` — bridges the Lark / Feishu IM API ↔ havn's
//! `/api/v1/channel` WebSocket protocol (spec §3).
//!
//! Inbound (Lark → havn) arrives over a **webhook** HTTP server
//! ([`webhook`]) — Lark International has no WebSocket long-connection.
//! Outbound (havn → Lark) drains the gateway WS and calls the IM send /
//! reply API ([`lark`]). A background task keeps the `tenant_access_token`
//! fresh.
//!
//! v1 supports 1:1 (`p2p`) chats. Inbound: text, voice (downloaded +
//! Groq/Whisper STT), location are native; image/file are surfaced as a
//! text marker (havn content is URL-based with no file host, and Lark
//! resources need the adapter's token). Outbound: text, image, file are
//! native (the agent's URL is fetched and re-uploaded to Lark).

#![forbid(unsafe_code)]

mod config;
mod havn_ws;
mod lark;
mod types;
mod webhook;
mod whisper;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context as _, Result, anyhow, bail};
use clap::Parser;
use serde_json::json;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::havn_ws::{WsClient, WsConfig};
use crate::lark::LarkClient;
use crate::types::{ChannelFrame, ChannelMessageContent, ChannelOutbound};
use crate::webhook::WebhookState;
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
        webhook = %cfg.webhook_listen,
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

    let ws = WsClient::spawn(WsConfig {
        gateway_url: cfg.havn_gateway.clone(),
        adapter_token: cfg.adapter_token.clone(),
        channel_id: cfg.channel_id.clone(),
        account_id: cfg.account_id.clone(),
    });
    let WsClient {
        outbound_tx,
        inbound_rx,
    } = ws;

    // Inbound: webhook server forwards Lark events as ChannelFrame::Inbound.
    let webhook_state = WebhookState {
        lark: lark.clone(),
        whisper: whisper.clone(),
        outbound_tx: outbound_tx.clone(),
        account_id: cfg.account_id.clone(),
        seq: Arc::new(AtomicU64::new(1)),
        verification_token: cfg.verification_token.clone(),
        encrypt_key: cfg.encrypt_key.clone(),
    };
    let listener = tokio::net::TcpListener::bind(&cfg.webhook_listen)
        .await
        .with_context(|| format!("binding webhook listener on {}", cfg.webhook_listen))?;
    info!(addr = %cfg.webhook_listen, "webhook server listening on POST /webhook");
    let webhook_task =
        tokio::spawn(async move { axum::serve(listener, webhook::router(webhook_state)).await });

    // Outbound: havn → Lark.
    let outbound_task = tokio::spawn(havn_outbound_loop(lark.clone(), inbound_rx));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received Ctrl-C, shutting down"),
        r = webhook_task => warn!(result = ?r, "webhook server exited unexpectedly"),
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

/// havn → Lark. Drain WS-received frames; deliver each `Outbound` via the
/// IM API. Other frame kinds are logged and ignored.
async fn havn_outbound_loop(
    lark: Arc<LarkClient>,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
) -> Result<()> {
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Outbound(o) => {
                if let Err(e) = deliver_outbound(&lark, &o).await {
                    warn!(error = %e, seq = o.seq, "Lark delivery failed; dropping");
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

async fn deliver_outbound(lark: &LarkClient, o: &ChannelOutbound) -> Result<()> {
    let reply_id = o.in_reply_to.as_deref();
    let open_id = o.to.strip_prefix("lark:");
    let (msg_type, content, extra_text) = build_outbound(lark, &o.content).await?;
    send_one(lark, reply_id, open_id, msg_type, &content).await?;
    if let Some(t) = extra_text {
        let c = serde_json::to_string(&json!({ "text": t }))?;
        send_one(lark, reply_id, open_id, "text", &c).await?;
    }
    Ok(())
}

/// Send one message, threading under `reply_id` if present, else to `open_id`.
async fn send_one(
    lark: &LarkClient,
    reply_id: Option<&str>,
    open_id: Option<&str>,
    msg_type: &str,
    content: &str,
) -> Result<()> {
    if let Some(rid) = reply_id {
        lark.reply_message(rid, msg_type, content).await?;
    } else if let Some(oid) = open_id {
        lark.send_message("open_id", oid, msg_type, content).await?;
    } else {
        bail!("outbound has no in_reply_to and a non-lark: `to`; cannot route");
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
