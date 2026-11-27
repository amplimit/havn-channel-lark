//! Replies shown as they are written (havn issue #325), as
//! havn-channel-discord shows them: the gateway's `Writing` frames of a
//! reply go in one card, opened with its first text (the frames that only
//! say it is being written open none: the Typing reaction shows meanwhile)
//! and updated in place (Lark's `PATCH` of a message) as the text comes,
//! with a **Stop** button under it; a reply longer than one card goes on in
//! the next. The text the model writes before it uses a
//! tool stays as a card of its own, the reply going on below it, and the
//! finished reply takes the place of what was shown, without the button.
//!
//! Lark takes 5 updates a second of one message: a reply is updated at most
//! every [`LIVE_EVERY`]. An update Lark asks to slow down for waits for the
//! next one, and one it refuses otherwise waits for the text to change; the
//! finished reply waits as Lark asks, in its chat's worker only, and what
//! cannot be put in its cards goes in new ones.
//!
//! A card is not taken down (a bot's recall of a message can show in the
//! chat that it recalled one): one no longer needed (a long reply's text
//! that came out shorter, or what was sent again below) is updated to show
//! nothing, or a line saying where the reply is when Lark refuses a card
//! that shows nothing ([`retire`]), and only taken down when that fails.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::card;
use crate::lark::{LarkClient, Refused, SlowDown};
use crate::types::{self, ChannelMessageContent, ChannelOutbound};

/// How often a reply being written is updated, at most: Lark takes 5
/// updates a second of one message.
const LIVE_EVERY: Duration = Duration::from_secs(1);
/// Replies being written that are kept at most; past that, the oldest is
/// finished as it is (one whose end never came).
const LIVE_KEPT: usize = 256;
/// A reply being written that gets nothing for this long is finished as it
/// is, its Stop button taken off: the agent or the gateway went away before
/// its end. The gateway says every minute that a reply is still being
/// written, so this does not come while its turn runs.
pub const LIVE_IDLE: Duration = Duration::from_secs(180);
/// How often replies being written are looked at for [`LIVE_IDLE`].
pub const LIVE_IDLE_EVERY: Duration = Duration::from_secs(30);
/// Replies that ended, remembered so a frame of one that comes late shows
/// nothing.
const ENDED_KEPT: usize = 256;
/// How a Stop button names its reply: this and the reply's id.
pub const STOP_PREFIX: &str = "stop#";
/// A person's presses of a Stop button within this send `/stop` once.
const PRESS_AGAIN_AFTER: Duration = Duration::from_secs(5);

/// The Stop button of a reply being written: the conversation it stops,
/// and when each person last pressed it.
struct Stop {
    to: String,
    pressed: HashMap<String, std::time::Instant>,
}

/// The Stop buttons of the replies being written, by reply. A press of one
/// whose reply is no longer written stops nothing.
static STOPS: std::sync::LazyLock<Mutex<HashMap<String, Stop>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn stops() -> std::sync::MutexGuard<'static, HashMap<String, Stop>> {
    STOPS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What a press of a button does.
#[derive(Debug, PartialEq, Eq)]
pub enum Pressed {
    /// It is not a Stop button.
    Other,
    /// A Stop button: `/stop` from the person who pressed it, for the
    /// conversation of its reply. The button stays until the reply ends,
    /// as the gateway may not let the person stop it.
    Stop(String),
    /// Nothing: the same person pressed it a moment ago.
    Again,
    /// Nothing: its reply is no longer written.
    Spent,
}

/// What person `person`'s press of button `data` does.
pub fn stop_of(data: &str, person: &str) -> Pressed {
    let Some(reply) = data.strip_prefix(STOP_PREFIX) else {
        return Pressed::Other;
    };
    let mut stops = stops();
    let Some(stop) = stops.get_mut(reply) else {
        return Pressed::Spent;
    };
    let now = std::time::Instant::now();
    if stop
        .pressed
        .get(person)
        .is_some_and(|at| now.duration_since(*at) < PRESS_AGAIN_AFTER)
    {
        return Pressed::Again;
    }
    stop.pressed.insert(person.to_string(), now);
    Pressed::Stop(stop.to.clone())
}

/// Whether `text` is a typed `/stop`, which the gateway answers with nothing
/// of its own.
pub fn is_stop(text: &str) -> bool {
    text.trim()
        .strip_prefix('/')
        .is_some_and(|name| name.eq_ignore_ascii_case("stop"))
}

/// The buttons reply `reply` shows of `given`: the gateway's Stop button
/// (`/stop`) naming the reply; none when the reply is not being written.
fn live_buttons(reply: &str, given: &[Vec<types::Button>]) -> Vec<Vec<types::Button>> {
    if !stops().contains_key(reply) {
        return Vec::new();
    }
    given
        .iter()
        .map(|row| {
            row.iter()
                .map(|b| types::Button {
                    label: b.label.clone(),
                    data: if b.data == "/stop" {
                        format!("{STOP_PREFIX}{reply}")
                    } else {
                        b.data.clone()
                    },
                })
                .collect()
        })
        .collect()
}

/// One card of a reply being written: its message, its text, and whether
/// it has the buttons.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shown {
    id: String,
    text: String,
    buttons: bool,
}

/// What changes a reply's cards from what is shown to its parts.
#[derive(Debug, PartialEq, Eq)]
enum Change {
    Update(usize),
    Send(usize),
    Delete(usize),
}

/// The changes, in order: cards whose text or buttons differ updated,
/// parts past them sent, cards past the parts taken down, the last first.
fn changes(shown: &[Shown], parts: &[String], buttons: bool) -> Vec<Change> {
    let mut changes = Vec::new();
    for (n, part) in parts.iter().enumerate() {
        let under = buttons && n + 1 == parts.len();
        match shown.get(n) {
            Some(s) if s.text == *part && s.buttons == under => {}
            Some(_) => changes.push(Change::Update(n)),
            None => changes.push(Change::Send(n)),
        }
    }
    changes.extend((parts.len()..shown.len()).rev().map(Change::Delete));
    changes
}

/// The cards a reply being written is shown in.
#[derive(Debug)]
struct Written {
    /// The gateway's id of the reply.
    reply: String,
    /// The recipient, as the gateway names it, and as Lark does.
    to: String,
    route: (&'static str, String),
    /// What its first card answers, a message of the chat.
    answers: Option<String>,
    cards: Vec<Shown>,
    /// What the gateway last gave it to show under its last card.
    buttons: Vec<Vec<types::Button>>,
}

impl Written {
    fn new(reply: &str, to: &str, route: (&'static str, String), answers: Option<String>) -> Self {
        Self {
            reply: reply.to_string(),
            to: to.to_string(),
            route,
            answers,
            cards: Vec::new(),
            buttons: Vec::new(),
        }
    }

    /// The cards of what comes after this part of the reply: below it, with
    /// its buttons.
    fn below(&mut self) -> Self {
        Self {
            buttons: std::mem::take(&mut self.buttons),
            ..Self::new(&self.reply, &self.to, self.route.clone(), None)
        }
    }

    /// Show `text` in its cards, with its buttons under the last when
    /// `with_buttons`: those that change are updated, those past them sent,
    /// those no longer needed taken down. When `done` (the reply's part or
    /// end), Lark's waits are waited out, and what cannot be put in its
    /// cards goes in new ones; otherwise it waits for the next time.
    async fn show(&mut self, lark: &LarkClient, text: &str, with_buttons: bool, done: bool) {
        let buttons = if with_buttons {
            live_buttons(&self.reply, &self.buttons)
        } else {
            Vec::new()
        };
        let parts = card::split(text);
        if let Err((n, _)) = self.put_all(lark, &parts, &buttons, done).await
            && done
        {
            self.fall_back(lark, &parts, &buttons, n).await;
        }
    }

    /// [`Self::show`] without its fallback: the first part that could not
    /// be put, and whether Lark only asked to slow down, as the error.
    async fn put_all(
        &mut self,
        lark: &LarkClient,
        parts: &[String],
        buttons: &[Vec<types::Button>],
        done: bool,
    ) -> Result<(), (usize, bool)> {
        let under = !buttons.is_empty();
        for change in changes(&self.cards, parts, under) {
            let n = match change {
                Change::Delete(n) => {
                    retire(lark, &self.cards[n].id, ENDS_ABOVE).await;
                    self.cards.truncate(n);
                    continue;
                }
                Change::Update(n) | Change::Send(n) => n,
            };
            let with = under && n + 1 == parts.len();
            let rows: &[Vec<types::Button>] = if with { buttons } else { &[] };
            let id = self.cards.get(n).map(|s| s.id.clone());
            match self
                .put(lark, id.as_deref(), n, &parts[n], rows, done)
                .await
            {
                Ok(id) => {
                    let shown = Shown {
                        id,
                        text: parts[n].clone(),
                        buttons: with,
                    };
                    match self.cards.get_mut(n) {
                        Some(old) => *old = shown,
                        None => self.cards.push(shown),
                    }
                }
                Err(e) => {
                    debug!(error = %format!("{e:#}"), "lark: a reply being written could not be put");
                    return Err((n, e.downcast_ref::<SlowDown>().is_some()));
                }
            }
        }
        Ok(())
    }

    /// The `n`th of `parts` could not be put in its card: those parts go in
    /// new cards (or, when Lark refuses a card, as plain text), as a reply
    /// is sent, and then that card and those after it go. When the new ones
    /// cannot be sent, the old ones stay, without their buttons.
    async fn fall_back(
        &mut self,
        lark: &LarkClient,
        parts: &[String],
        buttons: &[Vec<types::Button>],
        n: usize,
    ) {
        warn!(
            "lark: a reply could not be shown in the cards it was being written in; sending the rest anew"
        );
        let n = n.min(self.cards.len());
        let under = !buttons.is_empty();
        let mut sent = Vec::new();
        for (k, part) in parts.iter().enumerate().skip(n) {
            let with = under && k + 1 == parts.len();
            let rows: &[Vec<types::Button>] = if with { buttons } else { &[] };
            let put = match self.put(lark, None, k, part, rows, true).await {
                Err(e) if e.downcast_ref::<Refused>().is_some() => {
                    let plain = serde_json::json!({ "text": card::plain(part) }).to_string();
                    lark.post_message(self.answers_of(k), self.route(), "text", &plain, true)
                        .await
                        .map(|s| s.message_id)
                }
                other => other,
            };
            match put {
                Ok(id) => sent.push(Shown {
                    id,
                    text: part.clone(),
                    buttons: with,
                }),
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "lark: a reply could not be sent");
                    // What was sent goes after what stays.
                    self.cards.extend(sent);
                    for k in 0..self.cards.len() {
                        if self.cards[k].buttons {
                            let card =
                                card::text_card(&self.cards[k].text, &[], &self.to).to_string();
                            if lark
                                .update_card(&self.cards[k].id, &card, true)
                                .await
                                .is_ok()
                            {
                                self.cards[k].buttons = false;
                            }
                        }
                    }
                    return;
                }
            }
        }
        let gone: Vec<Shown> = self.cards.drain(n..).collect();
        for old in gone.iter().rev() {
            retire(lark, &old.id, GOES_ON_BELOW).await;
        }
        self.cards.extend(sent);
    }

    fn route(&self) -> (&str, &str) {
        (self.route.0, &self.route.1)
    }

    fn answers_of(&self, n: usize) -> Option<&str> {
        if n == 0 {
            self.answers.as_deref()
        } else {
            None
        }
    }

    /// `part` in card `id`, or in a new card (the `n`th) when none: its
    /// message id. When `patient`, again after each wait Lark asks for.
    async fn put(
        &self,
        lark: &LarkClient,
        id: Option<&str>,
        n: usize,
        part: &str,
        buttons: &[Vec<types::Button>],
        patient: bool,
    ) -> anyhow::Result<String> {
        let shown = card::text_card(part, buttons, &self.to).to_string();
        match id {
            Some(id) => lark
                .update_card(id, &shown, patient)
                .await
                .map(|()| id.to_string()),
            None => lark
                .post_message(
                    self.answers_of(n),
                    self.route(),
                    "interactive",
                    &shown,
                    patient,
                )
                .await
                .map(|s| s.message_id),
        }
    }

    /// Take the buttons off its last card; its text stays.
    async fn drop_buttons(&mut self, lark: &LarkClient) {
        let Some(n) = self.cards.len().checked_sub(1) else {
            return;
        };
        let Shown { id, text, buttons } = self.cards[n].clone();
        if !buttons {
            return;
        }
        match self.put(lark, Some(&id), n, &text, &[], true).await {
            Ok(_) => self.cards[n].buttons = false,
            Err(e) => {
                debug!(error = %format!("{e:#}"), "lark: the buttons could not be taken off");
            }
        }
    }
}

/// What a card no longer needed shows: nothing, a zero-width space.
const RETIRED: &str = "\u{200b}";
/// What a card no longer needed after the reply's last says, when Lark
/// refuses one that shows nothing.
const ENDS_ABOVE: &str = "(The reply ends above.)";
/// What one whose text was sent again below says, then.
const GOES_ON_BELOW: &str = "(The reply goes on below.)";

/// Card `id`, no longer needed, made to show nothing, or `line` when Lark
/// refuses a card that shows nothing; taken down only when that fails too,
/// as a recall may show in the chat.
async fn retire(lark: &LarkClient, id: &str, line: &str) {
    let blank = card::bare(RETIRED).to_string();
    match lark.update_card(id, &blank, true).await {
        Ok(()) => return,
        Err(e) if e.downcast_ref::<Refused>().is_some() => {
            if lark
                .update_card(id, &card::bare(line).to_string(), true)
                .await
                .is_ok()
            {
                return;
            }
        }
        Err(_) => {}
    }
    if let Err(e) = lark.delete_message(id).await {
        debug!(error = %format!("{e:#}"), "lark: a card of a reply could not be taken down");
    }
}

/// What a reply being written is to show: its text and its buttons.
type Showing = (String, Vec<Vec<types::Button>>);

/// A reply being written in a chat.
#[derive(Debug)]
struct Live {
    written: Written,
    /// The latest text of its model call not shown yet.
    latest: Option<Showing>,
    /// When it may be updated next.
    next: tokio::time::Instant,
    /// A part of it is shown: what comes next goes below it.
    part: bool,
    /// When it last got a frame.
    seen: tokio::time::Instant,
}

/// The replies being written in one chat, oldest first; those that have
/// said only that they are being written, which show no card until their
/// first text (the Typing reaction shows meanwhile); and those that ended
/// lately.
#[derive(Debug, Default)]
pub struct Replies {
    live: HashMap<String, Live>,
    order: VecDeque<String>,
    unopened: VecDeque<String>,
    ended: VecDeque<String>,
}

impl Replies {
    /// Whether a reply is being written in the chat.
    pub fn any(&self) -> bool {
        !self.live.is_empty()
    }

    /// `reply` ended: its Stop button stops nothing, and a frame of it that
    /// comes late shows nothing. What it showed, to finish.
    fn end(&mut self, reply: String) -> Option<Live> {
        self.order.retain(|r| *r != reply);
        self.unopened.retain(|r| *r != reply);
        let live = self.live.remove(&reply);
        stops().remove(&reply);
        self.ended.retain(|r| *r != reply);
        self.ended.push_back(reply);
        while self.ended.len() > ENDED_KEPT {
            self.ended.pop_front();
        }
        live
    }

    /// Finish `live` as it is shown: its buttons come off, its text stays.
    async fn finish(lark: &LarkClient, live: Option<Live>) {
        if let Some(mut live) = live {
            live.written.drop_buttons(lark).await;
        }
    }

    /// Finish the replies that got nothing for [`LIVE_IDLE`].
    pub async fn idle(&mut self, lark: &LarkClient) {
        let now = tokio::time::Instant::now();
        let idle: Vec<String> = self
            .live
            .iter()
            .filter(|(_, live)| now.duration_since(live.seen) >= LIVE_IDLE)
            .map(|(reply, _)| reply.clone())
            .collect();
        for reply in idle {
            info!("lark: a reply being written got nothing for a while; finished as it is");
            let live = self.end(reply);
            Self::finish(lark, live).await;
        }
    }

    /// When the next reply is due to be updated.
    pub fn due(&self) -> Option<tokio::time::Instant> {
        self.live
            .values()
            .filter(|live| live.latest.is_some())
            .map(|live| live.next)
            .min()
    }

    /// Update the replies due, each with its latest text, at most every
    /// [`LIVE_EVERY`]. One Lark asks to slow down for keeps its latest and
    /// waits until the next time, as long as Lark asks.
    pub async fn update_due(&mut self, lark: &LarkClient) {
        let now = tokio::time::Instant::now();
        for live in self.live.values_mut() {
            if live.next > now {
                continue;
            }
            let Some((text, buttons)) = live.latest.take() else {
                continue;
            };
            live.written.buttons = buttons;
            let before = live.written.cards.clone();
            let shown = live_buttons(&live.written.reply, &live.written.buttons);
            let put = live
                .written
                .put_all(lark, &card::split(&text), &shown, false)
                .await;
            // Not shown as Lark asked to slow down: shown at the next time.
            // Refused otherwise: tried again once the text changes, or at
            // its end.
            if matches!(put, Err((_, true))) && live.written.cards == before {
                live.latest = Some((text, live.written.buttons.clone()));
            }
            live.next = tokio::time::Instant::now() + LIVE_EVERY;
        }
    }

    /// Take `w`: its text for its reply's cards, or for new ones (below a
    /// part of it shown, which loses its buttons); nothing for a reply that
    /// ended, or that has no one to go to. One that only says the reply is
    /// still being written shows nothing and keeps it; so does one that only
    /// says it is being written: a reply opens its card with its first
    /// text, and a part keeps its button until the next call has text.
    pub async fn writing(&mut self, lark: &LarkClient, w: types::ChannelWriting) {
        if self.ended.contains(&w.reply) {
            return;
        }
        let now = tokio::time::Instant::now();
        if let Some(live) = self.live.get_mut(&w.reply) {
            live.seen = now;
            if w.alive || w.waiting {
                return;
            }
            if std::mem::take(&mut live.part) {
                live.written.drop_buttons(lark).await;
                live.written = live.written.below();
            }
            live.latest = Some((w.text, w.buttons));
            return;
        }
        if w.alive {
            return;
        }
        if w.waiting {
            if !self.unopened.contains(&w.reply) {
                self.unopened.push_back(w.reply);
                while self.unopened.len() > LIVE_KEPT {
                    self.unopened.pop_front();
                }
            }
            return;
        }
        self.unopened.retain(|r| *r != w.reply);
        let Some((kind, id)) = crate::recipient(&w.to) else {
            warn!(to = %w.to, "lark: a reply being written for no one; dropping");
            self.end(w.reply);
            return;
        };
        stops().entry(w.reply.clone()).or_insert(Stop {
            to: w.to.clone(),
            pressed: HashMap::new(),
        });
        let answers = w.in_reply_to.clone().filter(|m| m.starts_with("om_"));
        self.order.push_back(w.reply.clone());
        self.live.insert(
            w.reply.clone(),
            Live {
                written: Written::new(&w.reply, &w.to, (kind, id), answers),
                latest: Some((w.text, w.buttons)),
                next: now,
                part: false,
                seen: now,
            },
        );
        while self.order.len() > LIVE_KEPT {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            let live = self.end(oldest);
            Self::finish(lark, live).await;
        }
    }

    /// Show `o`, a part of a reply being written or its end, in that
    /// reply's cards: whether it was; one that is not, or whose reply is not
    /// shown, goes as any other. The end takes the buttons off; one with
    /// nothing more to say leaves its cards as they are.
    ///
    /// One with buttons or a question, or a file, goes as any reply: what
    /// was shown of its call is finished as it is (its text stays, its Stop
    /// button goes), and the reply ends with it or, after a part, goes on
    /// below it.
    pub async fn reply(&mut self, lark: &LarkClient, o: &ChannelOutbound) -> bool {
        let Some(reply) = &o.reply else {
            return false;
        };
        let Some(live) = self.live.get_mut(reply) else {
            // One that showed no card yet goes as any reply.
            if !o.partial && self.unopened.contains(reply) {
                self.end(reply.clone());
            }
            return false;
        };
        live.seen = tokio::time::Instant::now();
        live.latest = None;
        let text = match &o.content {
            ChannelMessageContent::Text { text } if o.buttons.is_empty() && o.poll.is_none() => {
                Some(text)
            }
            _ => None,
        };
        let shown = match text {
            Some(text) if o.partial || !text.trim().is_empty() => {
                if std::mem::take(&mut live.part) {
                    live.written.drop_buttons(lark).await;
                    live.written = live.written.below();
                }
                live.written.show(lark, text, o.partial, true).await;
                true
            }
            // An end that says nothing more, buttons, a question, a file:
            // what was shown stays, without its button.
            empty => {
                live.written.drop_buttons(lark).await;
                if std::mem::take(&mut live.part) {
                    live.written = live.written.below();
                }
                empty.is_some()
            }
        };
        if o.partial {
            live.part = true;
        } else {
            self.end(reply.clone());
        }
        shown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shown(text: &str, buttons: bool) -> Shown {
        Shown {
            id: format!("om_{text}"),
            text: text.into(),
            buttons,
        }
    }

    #[test]
    fn a_reply_being_written_changes_only_what_differs() {
        let parts = |p: &[&str]| p.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        assert_eq!(changes(&[], &parts(&["a"]), true), [Change::Send(0)]);
        let one = [shown("a", true)];
        assert_eq!(changes(&one, &parts(&["a"]), true), Vec::new());
        assert_eq!(changes(&one, &parts(&["ab"]), true), [Change::Update(0)]);
        // The buttons move to the last card.
        assert_eq!(
            changes(&one, &parts(&["a", "b"]), true),
            [Change::Update(0), Change::Send(1)]
        );
        // The end takes the buttons off; a shorter text takes cards down.
        let two = [shown("a", false), shown("b", true)];
        assert_eq!(changes(&two, &parts(&["a"]), false), [Change::Delete(1)]);
        assert_eq!(
            changes(&two, &parts(&[]), false),
            [Change::Delete(1), Change::Delete(0)]
        );
    }

    #[test]
    fn a_stop_button_stops_its_reply_only_while_it_is_written() {
        stops().insert(
            "r1".into(),
            Stop {
                to: "lark:on_a".into(),
                pressed: HashMap::new(),
            },
        );
        let given = vec![vec![types::Button {
            label: "Stop".into(),
            data: "/stop".into(),
        }]];
        assert_eq!(live_buttons("r1", &given)[0][0].data, "stop#r1");
        assert!(live_buttons("r2", &given).is_empty(), "not being written");
        assert_eq!(
            stop_of("stop#r1", "lark:on_a"),
            Pressed::Stop("lark:on_a".into())
        );
        // Pressed again at once: once.
        assert_eq!(stop_of("stop#r1", "lark:on_a"), Pressed::Again);
        // Someone else's press counts for them.
        assert_eq!(
            stop_of("stop#r1", "lark:on_b"),
            Pressed::Stop("lark:on_a".into())
        );
        stops().remove("r1");
        assert_eq!(stop_of("stop#r1", "lark:on_a"), Pressed::Spent);
        assert_eq!(stop_of("/approve 1", "lark:on_a"), Pressed::Other);
    }

    #[test]
    fn a_typed_stop_is_told_apart() {
        assert!(is_stop("/stop") && is_stop(" /STOP ") && !is_stop("stop") && !is_stop("/stopper"));
    }

    fn lark() -> std::sync::Arc<LarkClient> {
        // Lark is not there: every call fails at once.
        LarkClient::unauthenticated("http://127.0.0.1:9".into(), "a".into(), "s".into()).unwrap()
    }

    fn live_of(cards: Vec<Shown>) -> Live {
        let now = tokio::time::Instant::now();
        let mut written = Written::new("rid", "lark:on_a", ("union_id", "on_a".into()), None);
        written.cards = cards;
        Live {
            written,
            latest: Some(("stale".into(), Vec::new())),
            next: now,
            part: false,
            seen: now,
        }
    }

    fn end_of(content: ChannelMessageContent, partial: bool) -> ChannelOutbound {
        ChannelOutbound {
            seq: 1,
            account_id: "bot".into(),
            to: "lark:on_a".into(),
            in_reply_to: None,
            content,
            buttons: Vec::new(),
            poll: None,
            partial,
            reply: Some("rid".into()),
        }
    }

    fn file() -> ChannelMessageContent {
        ChannelMessageContent::File {
            url: "/api/v1/channel/files/a/b".into(),
            filename: "a.pdf".into(),
            caption: None,
        }
    }

    fn frame(text: &str, waiting: bool) -> types::ChannelWriting {
        types::ChannelWriting {
            account_id: "bot".into(),
            to: "lark:on_a".into(),
            in_reply_to: None,
            reply: "rid".into(),
            text: text.into(),
            buttons: Vec::new(),
            waiting,
            alive: false,
        }
    }

    #[tokio::test]
    async fn a_reply_opens_its_card_only_with_its_first_text() {
        let lark = lark();
        let mut replies = Replies::default();
        replies.writing(&lark, frame("Replying…", true)).await;
        assert!(
            !replies.any() && replies.due().is_none(),
            "no card for the line alone"
        );
        // An end that comes before any text goes as any reply, and ends it.
        assert!(
            !replies
                .reply(
                    &lark,
                    &end_of(ChannelMessageContent::Text { text: "Hi".into() }, false)
                )
                .await
        );
        assert!(replies.ended.contains(&"rid".to_string()) && replies.unopened.is_empty());
        // Another, which gets text: its card opens then.
        replies.ended.clear();
        replies.writing(&lark, frame("Replying…", true)).await;
        replies.writing(&lark, frame("Hel", false)).await;
        assert!(replies.any() && replies.unopened.is_empty());
        assert_eq!(
            replies.live["rid"].latest.as_ref().map(|l| l.0.as_str()),
            Some("Hel")
        );
    }

    #[tokio::test]
    async fn a_part_keeps_its_button_until_the_next_call_has_text() {
        let lark = lark();
        let mut replies = Replies::default();
        let mut live = live_of(vec![shown("the part", true)]);
        live.part = true;
        live.latest = None;
        replies.live.insert("rid".into(), live);
        replies.writing(&lark, frame("Replying…", true)).await;
        let live = &replies.live["rid"];
        assert!(live.part && live.written.cards.len() == 1 && live.latest.is_none());
    }

    #[tokio::test]
    async fn a_reply_ending_on_a_file_is_ended_and_a_part_that_is_one_goes_below() {
        let lark = lark();
        let mut replies = Replies::default();
        replies
            .live
            .insert("rid".into(), live_of(vec![shown("so far", false)]));
        replies.order.push_back("rid".into());
        // A file as a part: delivered as any reply; the next call goes below it.
        assert!(!replies.reply(&lark, &end_of(file(), true)).await);
        assert!(replies.live["rid"].part && replies.live["rid"].latest.is_none());
        // A file as its end: delivered, and the reply ends.
        assert!(!replies.reply(&lark, &end_of(file(), false)).await);
        assert!(!replies.any() && replies.ended.contains(&"rid".to_string()));
    }

    #[tokio::test]
    async fn buttons_after_a_part_leave_no_stop_button_nor_old_text() {
        let lark = lark();
        let mut replies = Replies::default();
        let mut live = live_of(vec![shown("the part", false)]);
        live.part = true;
        replies.live.insert("rid".into(), live);
        let mut asked = end_of(
            ChannelMessageContent::Text {
                text: "Which?".into(),
            },
            true,
        );
        asked.buttons = vec![vec![types::Button {
            label: "Red".into(),
            data: "/choose 1".into(),
        }]];
        assert!(!replies.reply(&lark, &asked).await);
        let live = &replies.live["rid"];
        assert!(
            live.latest.is_none(),
            "the call's old text is not sent below the buttons"
        );
        assert!(live.written.cards.is_empty(), "what comes next goes below");
        assert!(live.part);
    }

    #[tokio::test]
    async fn an_end_with_nothing_more_to_say_leaves_what_was_shown() {
        let lark = lark();
        let mut replies = Replies::default();
        replies
            .live
            .insert("rid".into(), live_of(vec![shown("all of it", false)]));
        let end = end_of(
            ChannelMessageContent::Text {
                text: String::new(),
            },
            false,
        );
        assert!(replies.reply(&lark, &end).await, "nothing to deliver");
        assert!(!replies.any());
    }

    #[tokio::test]
    async fn a_reply_being_written_is_kept_by_any_frame_and_finished_when_idle() {
        // Lark is not there: every card fails, which leaves the reply waiting.
        let lark = LarkClient::unauthenticated("http://127.0.0.1:9".into(), "a".into(), "s".into())
            .unwrap();
        let mut replies = Replies::default();
        let w = |text: &str, alive: bool| types::ChannelWriting {
            account_id: "bot".into(),
            to: "lark:on_a".into(),
            in_reply_to: None,
            reply: "rid".into(),
            text: text.into(),
            buttons: Vec::new(),
            waiting: false,
            alive,
        };
        replies.writing(&lark, w("Hel", false)).await;
        assert!(replies.any() && replies.due().is_some());
        replies.update_due(&lark).await;
        assert!(
            replies.live["rid"].latest.is_none(),
            "refused, not for slowing down: not tried again until the text changes"
        );
        replies.writing(&lark, w("Hel", false)).await;
        // A frame that only says it is alive keeps the text to show.
        replies.writing(&lark, w("", true)).await;
        assert_eq!(
            replies.live["rid"].latest.as_ref().map(|l| l.0.as_str()),
            Some("Hel")
        );
        replies.idle(&lark).await;
        assert!(replies.any(), "not idle yet");
        replies.live.get_mut("rid").unwrap().seen -= LIVE_IDLE;
        replies.idle(&lark).await;
        assert!(!replies.any());
        // A frame of a reply that ended shows nothing.
        replies.writing(&lark, w("late", false)).await;
        assert!(!replies.any());
    }
}
