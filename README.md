# havn-channel-lark

A [havn](https://github.com/amplimit/havn) channel adapter for **Lark / Feishu**. It bridges the Lark IM API and havn's `/api/v1/channel` WebSocket protocol (spec §3), so a havn agent can chat with users over a Lark bot. Runs as a small daemon outside havn's trust zone — havn never sees your Lark credentials.

Sibling of [havn-channel-telegram](https://github.com/amplimit/havn-channel-telegram); shares the same havn-side wire types and WS client.

## How it works

```
Lark user ──long connection (or webhook)──▶ havn-channel-lark ──WS /api/v1/channel──▶ havn gateway ──▶ agent
Lark user ◀──IM send/reply──────────────── havn-channel-lark ◀──WS outbound──────── havn gateway ◀──┘
```

- **Inbound** comes over Lark's **long connection** (`events = "long_connection"`, the default for a config that does not set up a webhook): the daemon opens a WebSocket to Lark (Feishu's 长连接, the event subscription Lark's own SDKs use) and gets the app's events on it, so it needs no public address, domain or certificate. It asks `<lark_base_url>/callback/ws/endpoint` for a fresh address each time it connects, pings as often as Lark asks (every 2 minutes unless Lark says otherwise), answers each event at once, and connects again whenever the connection drops or goes quiet for two pings: the first time after a random wait of up to 30 seconds (or what Lark says), then every 2 minutes (or what Lark says), without end; while Lark refuses the app (a wrong id or secret, the long connection not chosen on the console), the wait doubles each time, up to 30 minutes. An event Lark cuts in parts is put together, up to 64 parts and 4 MB; a larger one is dropped. With `events = "webhook"`, Lark sends the events as HTTP requests instead: the daemon serves `POST /webhook` on `webhook_listen` (default `0.0.0.0:8090`), which you point Lark's "Request URL" at. A config without `events` that sets `verification_token` or `webhook_listen`, as every config of this adapter did before the long connection, stays on the webhook; the log says which way events come, and why. The webhook answers Lark's `url_verification` challenge and refuses events whose Verification Token does not match. With an Encrypt Key, a request that carries Lark's signature (`X-Lark-Signature`) is checked before anything else is read of it and refused when the signature does not match or its timestamp is more than 5 minutes from the daemon's clock, so a request seen on the way cannot be sent again later; an event without a signature is refused. Either way an encrypted event is decrypted with `encrypt_key`.
- **Each event once.** Lark sends an event again when it got no answer in time; the daemon remembers the last 4096 events (`event_id`) and messages (`message_id`) it took and drops one it took before, so a retry makes no second turn. They are remembered while the daemon runs: an event Lark sends again across a restart of the daemon is taken again. A chat's messages go to the gateway in the order they came, one at a time (64 may wait; past that, a new one is dropped), so a voice message being transcribed is not overtaken by the text after it. What goes to the gateway with each message is Lark's event without its Verification Token and tenant key.
- **Outbound** drains the gateway WS and calls Lark's IM send/reply API. A background task keeps the `tenant_access_token` fresh (Lark tokens expire every 2h). An empty end of a reply sends nothing. When Lark says to slow down (HTTP 429, or its codes 99991400 and 230020: Lark takes 5 messages a second to one user or group), the message is sent again after the seconds Lark asks for (`x-ogw-ratelimit-reset`, else 1, 2, 4 and 8 seconds), at most 30 seconds a time and 4 times; the wait holds up that chat only.
- **Replies missed while disconnected.** When the daemon reconnects to the gateway, after the gateway or the daemon restarted or the connection dropped, it gets the replies sent meanwhile that the gateway kept (the last 5 minutes), in order, before any new one. It keeps the last reply it delivered in `<account_id>.state.json` in the unit's `StateDirectory` (or next to the config file, or at `--state`), so a restart resumes too. Each chat's replies are delivered in order, by a worker of the chat's own, so a chat Lark is slow to take holds up no other; a restart resumes from the earliest reply not delivered yet, and does not deliver again those after it that were. A reply is delivered twice only if the daemon stops between sending it and keeping it.

Lark (`https://open.larksuite.com`) and Feishu (`https://open.feishu.cn`) speak the same API, the long connection included; `lark_base_url` picks one.

## Sender ids

A sender is `lark:<union_id>`: the same person in every app of the same developer, which for apps made in your tenant (custom apps) is every custom app of the tenant. So a Lark account connected to a Havn account through one of your bots is that Havn user on all of them (see [Connecting a chat account](https://github.com/amplimit/Havn#connecting-a-chat-account)), and `allow_from` and `admins` take `lark:on_...` ids. A message from someone Lark gives no `union_id` for goes by their `open_id` (`lark:ou_...`), which is per app.

Each message also names the sender's `open_id`, the id this adapter gave before. A gateway of 0.2.17 or later notes it, for the account, as an id the sender went by, and keeps what it had under it: the `open_id`'s admission counts for the `union_id` both ways, so their chats from before, scheduled tasks and replies on their way still reach them, and an `open_id` in `allow_from` or `admins` still matches (the gateway's log says to replace it). Only the connection to a Havn account moves to the `union_id`, once the bot the message came through knows the `open_id`; then their chats from before are listed under **in chat apps** with the new ones. Their next message starts a new chat. See [Connecting a chat account](https://github.com/amplimit/Havn#connecting-a-chat-account).

The daemon waits for the gateway's first frame on each connection, which says whether it takes these ids. With an older gateway, senders go by their `open_id` as before, and links are per app. Once a gateway took them, the state file says so (`"aliases": true`), and the daemon gives `union_id`s from then on, even to an older gateway, as a connection that moved is under the `union_id`. Going back:

- **To an adapter that gives `open_id`s** (this one with `"aliases": true` taken out of the state file while it is stopped, or one before it): the gateway finds their admissions and chats under the `open_id`s, where they were, but a connection that moved is under the `union_id`: connect again (**chat apps** > **connect**).
- **To a gateway before 0.2.17**: it cannot open the database 0.2.17 migrated, so it means the backup taken before the update (`havn restore`), which has no `union_id`s. Give `open_id`s again as above first, or the senders come to it as people it does not know.

## Setup

### 1. Lark Developer Console

1. Create a **custom app**; note the **App ID** and **App Secret**.
2. **Permissions**: grant `im:message`, `im:message:send_as_bot` (send messages), `im:resource` (download and upload images and files) and `im:message.reactions:write_only` (the `Typing` reaction; `im:message` covers it too). `im:message` also lets the bot update its cards and take them down, for replies shown as they are written. Add a bot to the app.
3. **Events & Callbacks**:
   - Subscribe to the events **`im.message.receive_v1`** and **`application.bot.menu_v6`** (the bot's menu), and to the callback **`card.action.trigger`** (presses on the bot's cards), the same way as the events: by the long connection, or at the same Request URL.
   - **Long connection** (the default): choose **Receive events through persistent connection** (使用长连接接收事件). Lark checks that a client is connected when you save it, so start the daemon first (step 3).
   - **Webhook** (`events = "webhook"`): set **Request URL** to `https://<your-public-host>/webhook` (reverse-proxy it to this daemon's `webhook_listen`), and copy the **Verification Token** and (recommended) set and copy an **Encrypt Key**. The URL must be reachable from Lark's servers: put it behind your reverse proxy / TLS, the same way you front the havn gateway.
4. **The bot's menu** (optional): under the bot's features, add menu items that send an event, each with a command's name as its event key (`new`, `stop`, `retry`, `model`, ...; the daemon's log lists the gateway's when it connects). Choosing one sends that command, as `/new` typed. Lark has no API for a bot's menu, so it is set up here.
5. Publish/enable the app for your tenant.

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

Who the bot answers is set on the account as for any channel (`dm_policy`, `allow_from`, invite codes; see [Channel Adapters](https://github.com/amplimit/Havn#channel-adapters) in the Havn README). Letting a Lark account talk to the bot does not tell Havn whose it is: the gateway answers it as someone other than the agent's owner. For the account you use yourself, open the agent's settings page in the dashboard, **chat apps** > **connect**, and send the bot `/start` and the code it shows (`/start <code>`; a bare code is passed on to the agent when the bot already answers you); the bot replies "Connected to Havn account <name>." Until then the bot treats you as someone else: the agent's shell, file and memory tools wait for your approval in the dashboard's web chat, nothing you say there changes its memory, and the admin commands (`/model`, ...) need your id in `admins`, and even then the calls that wait for the owner (shell, files, memory, MCP) are approved only from the web chat.

### 3. This daemon

```bash
cp config.example.toml config.toml   # then fill it in
cargo build --release
./target/release/havn-channel-lark --config ./config.toml
```

The `adapter_token` in `config.toml` must equal the random string you gave `havn credential add`.

Under systemd, give it a `StateDirectory`, where it keeps the last reply delivered:

```ini
# /etc/systemd/system/havn-channel-lark.service
[Unit]
Description=havn channel adapter for Lark / Feishu
After=network.target havn.service
Wants=havn.service

[Service]
ExecStart=/usr/local/bin/havn-channel-lark --config /etc/havn-channel-lark/config.toml
Restart=always
RestartSec=5
User=havn-lark
# Where it keeps the last reply delivered, to resume after a restart.
StateDirectory=havn-channel-lark
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

## What it does

- **Private chats** (1:1, `p2p`) with the bot; group messages are dropped.
- **Text and rich text** (`post`, flattened to its text), and **locations**, passed to the agent as such.
- **Files both ways.** An image, a file, a voice message or a video someone sends is downloaded from Lark (at most 25 MB) and saved in the agent's inbox by the gateway, which also reads the text of a PDF, Word document or spreadsheet, and transcribes a voice message (`voice.ogg`) when `[transcription]` is set in its config.toml (see [Voice messages](https://github.com/amplimit/havn#voice-messages) in the Havn README); the agent gets the message with its file, a voice message with a line saying how long it is. A file that cannot be passed on is named in the message, with what went wrong, so the agent can say so. A sticker, which Lark lets no bot download, is a line saying so. A file the agent sends is fetched from the gateway and posted as an image when its bytes are one Lark shows (PNG, JPEG, GIF or WebP, at most 10 MB; one Lark refuses goes as a file), else as a file (Lark's `pdf`, `doc`, `xls`, `ppt`, `mp4` or `opus` by its extension, else `stream`; at most 30 MB); its caption follows it as a card. An image at an address of the agent's own goes as `[image: <url>]`: the daemon fetches only the gateway's files.
- **Replies as cards.** The agent's Markdown is sent as a Lark card (JSON 2.0, `markdown`), which shows headings, bold, italic, strikethrough, links, inline code, code blocks with their language, lists, quotes and tables. Outside code, a `<` is written as `&#60;`, so tags such as `<at id=all>` show as written and mention no one, and an image becomes a link (a card shows only images uploaded to Lark). A long reply goes in several cards of at most 4000 characters, cut at paragraph, line or word breaks, a code block cut in two closed and opened again. A card Lark refuses goes as plain text, with a zero-width space after each `<`.
- **Replies as they are written**, from a gateway of 0.2.15 or later. When the agent starts a reply, the daemon sends one card, "Replying…", with a **Stop** button under it, and updates it in place as the model writes, at most once a second (Lark takes 5 updates a second of one message); a reply longer than one card (4000 characters) goes on in the next, and the button moves to the last. The text the model writes before it uses a tool stays as a card of its own, keeping the button while the tool runs, and the reply goes on in a card below it. The finished reply takes the place of what was shown, and the button goes; what cannot be put in its cards (one recalled, Lark kept refusing) is sent in new cards (and then what was shown is made to show nothing). An update Lark refuses for another reason than slowing down is tried again only once the text changes, or at the end. When Lark asks the bot to slow down, an update waits for the next one, and the finished reply waits as Lark asks (in its chat only). **Stop** does what `/stop` does, for the same people, and the text written so far stays, followed by `(stopped)`; the button stays until the reply ends, as the gateway decides who may stop it, and a person's presses within 5 seconds count once. One pressed after its reply ended stops nothing and says so. While the agent thinks, runs a tool or waits for an approval, the gateway says every minute that the reply is still being written; a reply that hears nothing for 3 minutes (the agent or the gateway went away) keeps what it showed and loses its button, and a card that only said "Replying…" is made to show nothing. The daemon does not recall its cards, as a bot's recall can show in the chat; a card no longer needed is updated to show nothing, and recalled only when that fails. A reply that ends with buttons or a question, or with a file, goes as such: what was shown of its call keeps its text and loses its button, and a card that only said "Replying…" becomes its card (a file goes in a message of its own, and that card shows nothing); after a part that is one, the reply goes on below it. A card being written when the daemon stopped stays as it was. With an older gateway, replies come once written, as before.
- **Buttons.** Buttons the gateway puts under a message (such as **Approve** and **Deny** on a step waiting for approval, or the options of the agent's `ask_choice`) are buttons under its card, on the last of a long reply. A press goes to the gateway as if the person who pressed it had typed the button's command, with the same checks; it is answered at once, with the card shown again without its buttons, so it is pressed once. Only the person the card was sent to may press it: someone else, as in a forwarded copy, is told it is not theirs. A press is taken on a card the daemon sent and remembers (the last 256), or in the person's private chat with the bot: a copy forwarded into a group and pressed there is not, so the agent's reply, which goes to the person in their private chat, never lands in a group. A card sent before a restart keeps its buttons, and its press is taken once the person's messages showed their chat; a second press is told it was answered. When the person's chat has as many messages waiting as it may (64), a press is told to try again, and the card keeps its buttons.
- **Questions of several answers** (the agent's `ask_choice` with several to choose) are a card with the question, a menu that takes several (at most 50 options; more go as text) and a **Send** button: sent, the choice goes to the gateway as the person's answer, as a press does; sent with none chosen, the person is asked to choose one.
- **The bot's menu.** An item of the menu set up on the Developer Console (see Setup) goes to the gateway as its command, typed by the person who chose it, in turn with their messages (once one showed their private chat; Lark names no chat for a menu item); the answer goes to them.
- **A `Typing` reaction** on each message from when it arrives until its reply is sent (a part of a reply with more to come leaves it on), and on a card pressed until the answer to the press. A reply that names the message it answers (the gateway's own answers, such as to a code) takes it off that message, and one that answers a message without a reaction (an item of the menu) takes none off. The agent's replies in a private chat name no message, so the end of each turn (a reply the gateway gave an id) takes it off the oldest message waiting there, and a message that ends no turn (a scheduled report, a notice) takes none; with an older gateway, which gives no ids, any reply takes the oldest. One that gets no reply loses it after 5 minutes. A typed `/stop` gets none, as it gets no reply of its own.
- **Long connection or webhook**, each event taken once, replies resumed after a reconnect or restart: see "How it works".

**Voice messages are transcribed by the gateway now**, with `[transcription]` in its config.toml (see [Voice messages](https://github.com/amplimit/havn#voice-messages)): the adapter's `openai_api_key`, `whisper_base_url` and `whisper_model` are ignored. A config that still has them loads, and the log says so; move the key to the gateway's `[transcription]`.

## License

Apache-2.0. Copyright 2025-2026 Amplimit.
