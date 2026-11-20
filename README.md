# havn-channel-lark

A [havn](https://github.com/amplimit/havn) channel adapter for **Lark / Feishu**. It bridges the Lark IM API and havn's `/api/v1/channel` WebSocket protocol (spec §3), so a havn agent can chat with users over a Lark bot. Runs as a small daemon outside havn's trust zone — havn never sees your Lark credentials.

Sibling of [havn-channel-telegram](https://github.com/amplimit/havn-channel-telegram); shares the same havn-side wire types and WS client.

## How it works

```
Lark user ──event webhook──▶ havn-channel-lark ──WS /api/v1/channel──▶ havn gateway ──▶ agent
Lark user ◀──IM send/reply── havn-channel-lark ◀──WS outbound──────── havn gateway ◀──┘
```

- **Inbound** arrives over an HTTP **webhook** (`im.message.receive_v1`), the only inbound path this adapter implements: the daemon serves `POST /webhook` on `webhook_listen` (default `0.0.0.0:8090`), which you point Lark's "Request URL" at. It answers Lark's `url_verification` challenge, decrypts events when `encrypt_key` is set, and refuses events whose Verification Token does not match. The adapter opens no WebSocket long connection to Lark or Feishu: it gets only the events sent to its Request URL.
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

In havn's `config.toml` (`~/.config/havn/config.toml` of the user the gateway runs as, or `$HAVN_CONFIG`):

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

Who the bot answers is set on the account as for any channel (`dm_policy`, `allow_from`, invite codes; see [Channel Adapters](https://github.com/amplimit/Havn#channel-adapters) in the Havn README). Letting a Lark account talk to the bot does not tell Havn whose it is: the gateway answers it as someone other than the agent's owner. For the account you use yourself, open the agent's settings page in the dashboard, **chat apps** > **connect**, and send the bot the code it shows; the bot replies "Connected to Havn account <name>." Until then the bot treats you as someone else: the agent's shell, file and memory tools wait for your approval in the dashboard's web chat, nothing you say there changes its memory, and the admin commands (`/model`, `/approve`, ...) need your id in `admins`.

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
