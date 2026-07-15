# havn-channel-lark

A [havn](https://github.com/amplimit/havn) channel adapter for **Lark / Feishu**. It bridges the Lark IM API and havn's `/api/v1/channel` WebSocket protocol (spec §3), so a havn agent can chat with users over a Lark bot. Runs as a small daemon outside havn's trust zone — havn never sees your Lark credentials.

Sibling of [havn-channel-telegram](https://github.com/amplimit/havn-channel-telegram); shares the same havn-side wire types and WS client.

## How it works

```
Lark user ──event webhook──▶ havn-channel-lark ──WS /api/v1/channel──▶ havn gateway ──▶ agent
Lark user ◀──IM send/reply── havn-channel-lark ◀──WS outbound──────── havn gateway ◀──┘
```

- **Inbound** arrives over an HTTP **webhook** (`im.message.receive_v1`). Lark International does not expose a WebSocket long-connection, so webhook is the only inbound path — the daemon runs an HTTP server you point Lark's "Request URL" at.
- **Outbound** drains the gateway WS and calls Lark's IM send/reply API. A background task keeps the `tenant_access_token` fresh (Lark tokens expire every 2h).

## Setup

### 1. Lark Developer Console

1. Create a **custom app**; note the **App ID** and **App Secret**.
2. **Permissions**: grant `im:message`, `im:message:send_as_bot` (send messages), and `im:resource` (download voice/files). Add a bot to the app.
3. **Events & Callbacks**:
   - Subscribe to **`im.message.receive_v1`**.
   - Set **Request URL** to `https://<your-public-host>/webhook` (reverse-proxy it to this daemon's `webhook_listen`).
   - Copy the **Verification Token** and (recommended) set + copy an **Encrypt Key**.
4. Publish/enable the app for your tenant.

> The webhook URL must be reachable from Lark's servers. Put it behind your reverse proxy / TLS, the same way you front the havn gateway.

### 2. havn side

In havn's `gateway.toml`:

```toml
[[channels.lark.accounts]]
id = "acme-lark-bot"
adapter_token_ref = "secret:channel:lark:acme-lark-bot"

[[bindings]]
channel = "lark"
account_id = "acme-lark-bot"
agent_id = "<your-agent-uuid>"
```

Store the adapter token in havn's credential store (it's compared at WS upgrade):

```bash
havn credential add channel:lark <a-long-random-string> --name acme-lark-bot
```

Editing `[[channels]]` / `[[bindings]]` is **restart-class** — run `havn gateway restart` to apply (spec §8.4).

### 3. This daemon

```bash
cp config.example.toml config.toml   # then fill it in
cargo build --release
./target/release/havn-channel-lark --config ./config.toml
```

The `adapter_token` in `config.toml` must equal the random string you gave `havn credential add`.

## Content support (v1)

| | Inbound (user → agent) | Outbound (agent → user) |
|---|---|---|
| Text | ✅ native | ✅ native |
| Voice | ✅ downloaded + transcribed (OpenAI-compatible STT, e.g. Groq) | flattened to text |
| Location | ✅ native | flattened to text |
| Image / File | text marker* | ✅ native (URL fetched, re-uploaded to Lark) |

\* Inbound images/files are surfaced as a text marker rather than delivered: havn's content model is URL-based with no file host, and Lark resources require this adapter's token to fetch — so the agent can't retrieve them directly. Voice is the exception because it's transcribed to text here. Outbound image/file **is** native.

v1 handles **1:1 (`p2p`) chats** only; group messages are dropped.

## License

Apache-2.0. Copyright 2025-2026 Amplimit.
