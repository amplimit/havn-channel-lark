//! Group chats (havn issue #263): what the daemon knows of the groups the bot
//! is in, and what a group's message says to the gateway.
//!
//! A group message goes on to a gateway that said it takes them, with the
//! group (`lark:<chat_id>`), its name, the sender's name and whether it is to
//! the bot: it mentions the bot, replies to one of the bot's messages, or is
//! one of the gateway's commands. A message in a thread (Lark's topic) is in
//! a conversation of its own (havn issue #285), the thread (`lark:<thread_id>`),
//! and what goes to it replies to a message in it, as Lark sends nothing to a
//! thread itself: the thread's first message, kept beside the state file.
//!
//! Lark gives a bot without the permission to read every group message only
//! the messages that mention it: the permissions the tenant granted are
//! asked when the daemon starts and every half hour, and a message that does
//! not mention the bot shows it has one.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::events::PREFIX;
use crate::lark::{LarkClient, Mention};
use crate::types::{ChannelGroup, ChannelPlace, Reach};

/// How often Lark is asked who the bot is until it says.
const BOT_AGAIN: Duration = Duration::from_secs(30);
/// How often the permissions the tenant granted are asked.
const SCOPES_EVERY: Duration = Duration::from_mins(30);
/// The permissions that give a bot every message of its groups.
const SEE_ALL_SCOPES: [&str; 4] = [
    "im:message.group_msg",
    "im:message.group_msg:readonly",
    "im:message.group_msg.include_bot:read",
    "im:message.group_bot_msg:readonly",
];
/// A group's name is asked again after this long.
const TITLE_FOR: Duration = Duration::from_hours(1);
/// A group's members are asked again, for a sender not among them, at most
/// this often.
const MEMBERS_AGAIN: Duration = Duration::from_mins(1);
/// Pages of 100 members asked at most, per group.
const MEMBER_PAGES: usize = 10;
/// After Lark refused to say a group's name or members (the app lacks
/// `im:chat:readonly`), they are not asked for this long.
const UNREADABLE_FOR: Duration = Duration::from_mins(10);
/// Groups whose names and members are kept at most.
const GROUPS_KEPT: usize = 1024;
/// Threads kept at most; past that, the oldest is let go.
const THREADS_KEPT: usize = 4096;
/// Messages known to be the bot's or not, at most.
const OURS_KEPT: usize = 4096;

/// The bot's `open_id`, once Lark said it.
static BOT: OnceLock<String> = OnceLock::new();

/// The bot's `open_id`, which its mentions name, once known.
pub fn bot() -> Option<&'static str> {
    BOT.get().map(String::as_str)
}

#[cfg(test)]
pub fn know_bot(open_id: &str) {
    let _ = BOT.set(open_id.to_string());
}

/// Ask Lark who the bot is, at once and then every [`BOT_AGAIN`] until it
/// says: until then a mention does not address it.
pub async fn learn_bot(lark: Arc<LarkClient>) {
    loop {
        match lark.bot_open_id().await {
            Ok(open_id) => {
                info!(bot = %open_id, "lark: the bot is known; a mention of it in a group is to it");
                let _ = BOT.set(open_id);
                return;
            }
            Err(e) => {
                warn!(error = %format!("{e:#}"), "lark: could not learn who the bot is; group mentions are not to it until then");
            }
        }
        tokio::time::sleep(BOT_AGAIN).await;
    }
}

/// The gateway said it takes group messages (havn issue #263).
static GATEWAY_GROUPS: AtomicBool = AtomicBool::new(false);
/// The commands the gateway answers, as its last menu named them.
static COMMANDS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

/// The gateway's menu: whether it takes group messages, and its commands.
pub fn gateway_said(groups: bool, commands: impl IntoIterator<Item = String>) {
    GATEWAY_GROUPS.store(groups, Ordering::Relaxed);
    let mut known = COMMANDS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *known = commands
        .into_iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();
}

/// Whether the gateway takes group messages: an older one would take them
/// for private ones.
pub fn gateway_takes_groups() -> bool {
    GATEWAY_GROUPS.load(Ordering::Relaxed)
}

/// Whether `text` is a command: `/` and one of the gateway's commands (any
/// name, before the gateway named them), alone or before a space.
pub fn is_command(text: &str) -> bool {
    let Some(rest) = text.trim_start().strip_prefix('/') else {
        return false;
    };
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty()
        || !rest[name.len()..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
    {
        return false;
    }
    let known = COMMANDS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    known.is_empty() || known.contains(&name.to_ascii_lowercase())
}

/// What the tenant's permissions say of the bot's groups: 0 not known, 1 it
/// gets only mentions, 2 every message.
static SCOPES_SEE_ALL: AtomicU8 = AtomicU8::new(0);
/// A group message that does not mention the bot came: it gets every one.
static SAW_ALL: AtomicBool = AtomicBool::new(false);

/// Whether the bot sees every message of its groups, where known. Lark
/// grants it to the app, for all of them.
pub fn sees_all() -> Option<bool> {
    if SAW_ALL.load(Ordering::Relaxed) {
        return Some(true);
    }
    match SCOPES_SEE_ALL.load(Ordering::Relaxed) {
        1 => Some(false),
        2 => Some(true),
        _ => None,
    }
}

/// What reaches a bot that sees every message of its groups (`all`), or
/// only those that mention it.
pub fn reaches(all: bool) -> Vec<Reach> {
    if all {
        vec![Reach::Mention, Reach::Reply, Reach::Command]
    } else {
        vec![Reach::Mention]
    }
}

/// A group message that does not mention the bot came: only a bot that gets
/// every message gets one.
pub fn saw_all() {
    if !SAW_ALL.swap(true, Ordering::Relaxed) && SCOPES_SEE_ALL.load(Ordering::Relaxed) != 2 {
        info!("lark: a group message that does not mention the bot came: it sees every message");
    }
}

/// Ask the permissions the tenant granted, for [`sees_all`].
pub async fn learn_scopes(lark: &LarkClient) {
    match lark.granted_scopes().await {
        Ok(scopes) => {
            let all = scopes.iter().any(|s| SEE_ALL_SCOPES.contains(&s.as_str()));
            let was = SCOPES_SEE_ALL.swap(if all { 2 } else { 1 }, Ordering::Relaxed);
            if was != SCOPES_SEE_ALL.load(Ordering::Relaxed) {
                if all {
                    info!("lark: the bot may read every group message");
                } else {
                    info!(
                        "lark: in a group the bot gets only the messages that mention it; grant \
                         im:message.group_msg for replies to it and commands without a mention"
                    );
                }
            }
        }
        Err(e) => debug!(error = %format!("{e:#}"), "lark: could not ask the permissions granted"),
    }
}

/// [`learn_scopes`] now and every [`SCOPES_EVERY`].
pub async fn keep_scopes(lark: Arc<LarkClient>) {
    loop {
        learn_scopes(&lark).await;
        tokio::time::sleep(SCOPES_EVERY).await;
    }
}

/// `text`, sent with `mentions`, as the agent reads it, and whether it
/// mentions the bot (`bot`): its mention taken out, each other one as `@`
/// and the name, a mention of everyone (`@_all`) as `@all`.
pub fn written(text: &str, mentions: &[Mention], bot: Option<&str>) -> (String, bool) {
    let mut keys: Vec<&Mention> = mentions.iter().filter(|m| !m.key.is_empty()).collect();
    keys.sort_by_key(|m| std::cmp::Reverse(m.key.len()));
    let mut out = String::with_capacity(text.len());
    let mut mentioned = false;
    let mut rest = text;
    while let Some(at) = rest.find("@_") {
        out.push_str(&rest[..at]);
        let here = &rest[at..];
        if let Some(m) = keys.iter().find(|m| here.starts_with(&m.key)) {
            rest = &here[m.key.len()..];
            if is_bot(m, bot) {
                mentioned = true;
                rest = rest.strip_prefix(' ').unwrap_or(rest);
            } else {
                out.push('@');
                out.push_str(if m.name.is_empty() {
                    "someone"
                } else {
                    &m.name
                });
            }
        } else if let Some(after) = here.strip_prefix("@_all") {
            out.push_str("@all");
            rest = after;
        } else {
            out.push_str("@_");
            rest = &here[2..];
        }
    }
    out.push_str(rest);
    (out.trim().to_string(), mentioned)
}

/// Whether mention `m` is of the bot.
pub fn is_bot(m: &Mention, bot: Option<&str>) -> bool {
    bot.is_some() && m.id.open_id.as_deref() == bot
}

/// Messages known to be the bot's (`true`) or not, the oldest let go first.
#[derive(Default)]
struct Ours {
    known: HashMap<String, bool>,
    order: VecDeque<String>,
}

impl Ours {
    fn note(&mut self, id: &str, ours: bool) {
        if self.known.insert(id.to_string(), ours).is_none() {
            self.order.push_back(id.to_string());
        }
        while self.order.len() > OURS_KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.known.remove(&oldest);
            }
        }
    }
}

static OURS: LazyLock<Mutex<Ours>> = LazyLock::new(Mutex::default);

fn ours() -> std::sync::MutexGuard<'static, Ours> {
    OURS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The bot sent message `id`.
pub fn note_ours(id: &str) {
    ours().note(id, true);
}

/// Whether message `id`, which a group message replies to, is the bot's:
/// one it sent while running, else as Lark says.
pub async fn is_ours(lark: &LarkClient, id: &str) -> bool {
    if let Some(known) = ours().known.get(id).copied() {
        return known;
    }
    match lark.sent_by_bot(id).await {
        Ok(theirs) => {
            ours().note(id, theirs);
            theirs
        }
        Err(e) => {
            debug!(error = %format!("{e:#}"), "lark: could not tell whose message a reply answers");
            false
        }
    }
}

/// A thread: the group it is in, and the message what goes to it replies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Thread {
    pub chat: String,
    pub anchor: String,
}

/// What is known of the groups.
#[derive(Default)]
struct Known {
    titles: HashMap<String, (Instant, Option<String>)>,
    members: HashMap<String, (Instant, HashMap<String, String>)>,
    threads: HashMap<String, Thread>,
    thread_order: VecDeque<String>,
    /// When Lark last refused a group's name or members.
    unreadable: Option<Instant>,
}

static KNOWN: LazyLock<Mutex<Known>> = LazyLock::new(Mutex::default);

fn known() -> std::sync::MutexGuard<'static, Known> {
    KNOWN
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Where the threads are kept across restarts, beside the state file.
static THREADS_FILE: OnceLock<PathBuf> = OnceLock::new();

/// Keep the threads beside the state file `state` (`<state>.threads`),
/// loading those kept there.
pub fn keep_beside(state: Option<&Path>) {
    let Some(state) = state else {
        return;
    };
    let mut path = state.as_os_str().to_owned();
    path.push(".threads");
    let path = PathBuf::from(path);
    match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice::<Vec<(String, Thread)>>(&bytes) {
            Ok(kept) => {
                let mut known = known();
                for (id, thread) in kept {
                    if known.threads.insert(id.clone(), thread).is_none() {
                        known.thread_order.push_back(id);
                    }
                }
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "lark: could not read the threads kept; starting without them");
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(path = %path.display(), error = %e, "lark: could not read the threads kept; starting without them");
        }
    }
    let _ = THREADS_FILE.set(path);
}

/// Write the threads to [`THREADS_FILE`], the file replaced whole.
fn save_threads(known: &Known) {
    let Some(path) = THREADS_FILE.get() else {
        return;
    };
    let kept: Vec<(&String, &Thread)> = known
        .thread_order
        .iter()
        .filter_map(|id| known.threads.get(id).map(|t| (id, t)))
        .collect();
    let written = serde_json::to_vec(&kept)
        .map_err(std::io::Error::other)
        .and_then(|bytes| {
            let mut part = path.as_os_str().to_owned();
            part.push(".part");
            let part = PathBuf::from(part);
            std::fs::write(&part, bytes)?;
            std::fs::rename(&part, path)
        });
    if let Err(e) = written {
        warn!(path = %path.display(), error = %e, "lark: could not keep the threads");
    }
}

/// A message came in thread `thread` of group `chat`, the thread's first
/// message `anchor`: what goes to the thread replies to it.
pub fn note_thread(thread: &str, chat: &str, anchor: &str) {
    let mut known = known();
    if known.threads.contains_key(thread) {
        return;
    }
    known.threads.insert(
        thread.to_string(),
        Thread {
            chat: chat.to_string(),
            anchor: anchor.to_string(),
        },
    );
    known.thread_order.push_back(thread.to_string());
    while known.thread_order.len() > THREADS_KEPT {
        if let Some(oldest) = known.thread_order.pop_front() {
            known.threads.remove(&oldest);
        }
    }
    save_threads(&known);
}

/// Thread `thread`, when one of its messages came.
pub fn thread(thread: &str) -> Option<Thread> {
    known().threads.get(thread).cloned()
}

/// The bot was added to group `chat`, named `name`.
pub fn added(chat: &str, name: Option<String>) {
    info!(
        group = %format!("{PREFIX}{chat}"),
        name = name.as_deref().unwrap_or_default(),
        "lark: the bot was added to a group; it is used there once the gateway lets the group in"
    );
    let mut known = known();
    remember_title(&mut known, chat, name);
}

/// The bot was removed from group `chat`, or it was disbanded: what is known
/// of it is forgotten.
pub fn removed(chat: &str) {
    info!(group = %format!("{PREFIX}{chat}"), "lark: the bot is no longer in a group");
    let mut known = known();
    known.titles.remove(chat);
    known.members.remove(chat);
    let before = known.threads.len();
    known.threads.retain(|_, t| t.chat != chat);
    if known.threads.len() != before {
        let Known {
            threads,
            thread_order,
            ..
        } = &mut *known;
        thread_order.retain(|id| threads.contains_key(id));
        save_threads(&known);
    }
}

fn remember_title(known: &mut Known, chat: &str, title: Option<String>) {
    if known.titles.len() >= GROUPS_KEPT && !known.titles.contains_key(chat) {
        known.titles.clear();
    }
    known
        .titles
        .insert(chat.to_string(), (Instant::now(), title));
}

/// Whether Lark refused a group's name or members a moment ago.
fn unreadable(known: &Known) -> bool {
    known
        .unreadable
        .is_some_and(|at| at.elapsed() < UNREADABLE_FOR)
}

/// Lark refused a group's name or members: said once, and not asked again
/// for a while.
fn refused(e: &anyhow::Error) {
    static TOLD: AtomicBool = AtomicBool::new(false);
    if !TOLD.swap(true, Ordering::Relaxed) {
        warn!(
            error = %format!("{e:#}"),
            "lark: could not read a group's name or members; grant im:chat:readonly to name them to the agent"
        );
    }
    known().unreadable = Some(Instant::now());
}

/// The name of group `chat`, asked of Lark at most every [`TITLE_FOR`].
async fn title(lark: &LarkClient, chat: &str) -> Option<String> {
    {
        let known = known();
        if let Some((at, title)) = known.titles.get(chat)
            && at.elapsed() < TITLE_FOR
        {
            return title.clone();
        }
        if unreadable(&known) {
            return known.titles.get(chat).and_then(|(_, t)| t.clone());
        }
    }
    match lark.chat_name(chat).await {
        Ok(title) => {
            remember_title(&mut known(), chat, title.clone());
            title
        }
        Err(e) => {
            refused(&e);
            known().titles.get(chat).and_then(|(_, t)| t.clone())
        }
    }
}

/// The name of the person `open_id` in group `chat`, from its members, asked
/// again for one not among them at most every [`MEMBERS_AGAIN`].
async fn name(lark: &LarkClient, chat: &str, open_id: &str) -> Option<String> {
    {
        let known = known();
        if let Some((at, names)) = known.members.get(chat) {
            if let Some(name) = names.get(open_id) {
                return Some(name.clone());
            }
            if at.elapsed() < MEMBERS_AGAIN {
                return None;
            }
        }
        if unreadable(&known) {
            return None;
        }
    }
    match lark.chat_members(chat, MEMBER_PAGES).await {
        Ok(members) => {
            let names: HashMap<String, String> = members.into_iter().collect();
            let name = names.get(open_id).cloned();
            let mut known = known();
            if known.members.len() >= GROUPS_KEPT && !known.members.contains_key(chat) {
                known.members.clear();
            }
            known
                .members
                .insert(chat.to_string(), (Instant::now(), names));
            name
        }
        Err(e) => {
            refused(&e);
            None
        }
    }
}

/// Where in a group a message was written: the group, and the thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    pub chat: String,
    pub thread: Option<String>,
}

impl Place {
    /// The conversation it is in, as the gateway names it: the thread, or
    /// the group.
    pub fn conversation(&self) -> String {
        format!("{PREFIX}{}", self.thread.as_deref().unwrap_or(&self.chat))
    }

    /// The place an outbound's `to` names, when that is a group or one of
    /// its threads.
    pub fn of_to(to: &str) -> Option<Self> {
        let id = to.strip_prefix(PREFIX)?;
        if id.starts_with("oc_") {
            Some(Self {
                chat: id.to_string(),
                thread: None,
            })
        } else if id.starts_with("omt_") {
            thread(id).map(|t| Self {
                chat: t.chat,
                thread: Some(id.to_string()),
            })
        } else {
            None
        }
    }

    /// The group of a message `open_id` wrote here, as the gateway takes it.
    pub async fn group(&self, lark: &LarkClient, open_id: &str, addressed: bool) -> ChannelGroup {
        ChannelGroup {
            id: format!("{PREFIX}{}", self.chat),
            title: title(lark, &self.chat).await,
            sender_name: name(lark, &self.chat, open_id).await,
            addressed,
            sees_all: sees_all(),
            reaches: sees_all().map(reaches),
            space: None,
            thread: self.thread.as_ref().map(|t| ChannelPlace {
                id: format!("{PREFIX}{t}"),
                title: None,
            }),
        }
    }
}

/// Whether `to` names a group or a thread: replies there name the message
/// they answer.
pub fn in_group(to: &str) -> bool {
    to.strip_prefix(PREFIX)
        .is_some_and(|id| id.starts_with("oc_") || id.starts_with("omt_"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lark::MentionId;

    fn mention(key: &str, open_id: &str, name: &str) -> Mention {
        Mention {
            key: key.into(),
            id: MentionId {
                open_id: Some(open_id.into()),
            },
            name: name.into(),
        }
    }

    #[test]
    fn the_bot_s_mention_is_taken_out_and_others_are_names() {
        let mentions = [
            mention("@_user_1", "ou_bot", "Acme"),
            mention("@_user_2", "ou_ann", "Ann"),
            mention("@_user_10", "ou_tom", "Tom"),
        ];
        assert_eq!(
            written(
                "@_user_1 ask @_user_2 and @_user_10, @_all too",
                &mentions,
                Some("ou_bot")
            ),
            ("ask @Ann and @Tom, @all too".to_string(), true)
        );
        // Not the bot's: no mention of it, and an unknown key stays.
        assert_eq!(
            written("@_user_2 hi @_user_3", &mentions, Some("ou_bot")),
            ("@Ann hi @_user_3".to_string(), false)
        );
        // Until Lark says who the bot is, nothing mentions it.
        assert_eq!(
            written("@_user_1 hi", &mentions, None),
            ("@Acme hi".to_string(), false)
        );
    }

    #[test]
    fn a_command_is_one_of_the_gateway_s() {
        gateway_said(true, ["new".to_string(), "stop".to_string()]);
        assert!(is_command("/new"));
        assert!(is_command("  /STOP now"));
        assert!(!is_command("/usr/bin/env"));
        assert!(!is_command("/model"));
        assert!(!is_command("new"));
        assert!(gateway_takes_groups());
    }

    #[test]
    fn a_place_s_conversation_is_its_thread_or_its_group() {
        note_thread("omt_t1", "oc_g1", "om_root");
        assert_eq!(
            Place::of_to("lark:omt_t1"),
            Some(Place {
                chat: "oc_g1".into(),
                thread: Some("omt_t1".into()),
            })
        );
        assert_eq!(
            Place::of_to("lark:omt_t1").map(|p| p.conversation()),
            Some("lark:omt_t1".into())
        );
        assert_eq!(
            Place::of_to("lark:oc_g2").map(|p| p.conversation()),
            Some("lark:oc_g2".into())
        );
        assert_eq!(Place::of_to("lark:omt_unknown"), None);
        assert_eq!(Place::of_to("lark:on_ann"), None);
        assert!(in_group("lark:omt_t1") && in_group("lark:oc_g2") && !in_group("lark:on_ann"));
        removed("oc_g1");
        assert_eq!(thread("omt_t1"), None);
    }
}
