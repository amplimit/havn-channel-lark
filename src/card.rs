//! Replies as Lark cards (JSON 2.0, `msg_type: "interactive"`): the
//! agent's Markdown in a `markdown` element, which Lark renders (headings,
//! emphasis, links, code with its language, lists, quotes, tables), with
//! the gateway's buttons under it and its questions of several answers as
//! a form. A long reply is cut into several cards, its code blocks closed
//! and opened again across them, as havn-channel-discord cuts its
//! messages.
//!
//! The card's Markdown also takes a few HTML tags, among them `<at>`, which
//! mentions people (`<at id=all>`, everyone), and `<font>`: outside code, a
//! `<` of the agent's is written as `&#60;`, so it shows as written and
//! mentions no one. An image (`![..](..)`) takes only an image key Lark
//! gave, so the agent's are links.

use serde_json::{Value, json};

use crate::types::{Button, Poll};

/// Characters of Markdown in one card: Lark asks a message's JSON to stay
/// under 30 KB, and a character of Chinese is three bytes of it.
pub const PART_CHARS: usize = 4000;

/// Buttons in one card at most (Lark takes 200 elements a card).
const BUTTONS_MAX: usize = 25;
/// Characters of a button's text at most (Lark's 100).
const LABEL_CHARS: usize = 100;
/// Options of a question shown as a menu at most; past that, its text alone.
pub const OPTIONS_MAX: usize = 50;

/// The key of a press's `value` that holds what it sends back, the
/// recipient it was sent to, and what it is.
pub const SAID: &str = "said";
pub const TO: &str = "to";
pub const KIND: &str = "kind";
/// The kind of a question's submit button, and its answer's key.
pub const QUESTION: &str = "question";
pub const ANSWER: &str = "answer";
/// The name of a question's menu in the form it is in.
pub const CHOICES: &str = "choices";

/// `text` as card parts of at most [`PART_CHARS`]: cut at a paragraph,
/// else a line, else a word, else anywhere; a code block cut in two is
/// closed in one part and opened again, with its language, in the next.
pub fn split(text: &str) -> Vec<String> {
    let text = text.trim_end();
    let mut parts = Vec::new();
    let mut rest = text;
    // The fence line that opened a code block still open, as ```rust.
    let mut fence: Option<String> = None;
    while !rest.is_empty() {
        let head = fence.as_ref().map(|f| format!("{f}\n")).unwrap_or_default();
        // Room for the head, and for closing a block left open.
        let budget = PART_CHARS
            .saturating_sub(head.chars().count() + "\n```".len())
            .max(1);
        if rest.chars().count() <= budget {
            parts.push(format!("{head}{rest}"));
            break;
        }
        let cut = cut_at(rest, budget);
        let (part, remaining) = rest.split_at(cut);
        let open = fence_after(fence.clone(), part);
        let mut piece = format!("{head}{}", part.trim_end());
        if open.is_some() {
            piece.push_str("\n```");
        }
        parts.push(piece);
        fence = open;
        rest = remaining.trim_start_matches(['\n', '\r']);
        // Inside a code block the spaces lead a line of code; outside
        // they would start the next part with a space.
        if fence.is_none() {
            rest = rest.trim_start_matches(' ');
        }
    }
    parts
}

/// The byte index to cut `text` at so that at most `budget` characters
/// come before it: after the last blank line, else line, else space, in
/// the second half of the part; else at the budget.
fn cut_at(text: &str, budget: usize) -> usize {
    let limit = text
        .char_indices()
        .nth(budget)
        .map_or(text.len(), |(i, _)| i);
    let window = &text[..limit];
    let half = window.len() / 2;
    for sep in ["\n\n", "\n", " "] {
        if let Some(at) = window.rfind(sep)
            && at >= half
        {
            return at + sep.len();
        }
    }
    limit
}

/// Whether a code block is open after `part`, which began with `open`:
/// the fence line that opened it, if so.
fn fence_after(mut open: Option<String>, part: &str) -> Option<String> {
    for line in part.lines() {
        let line = line.trim();
        if let Some(info) = line.strip_prefix("```") {
            open = match open {
                Some(_) => None,
                // A fence that closes on its own line, as ```x```, opens none.
                None if info.contains("```") => None,
                // Opened again with its language only, short.
                None => {
                    let language: String = info
                        .split_whitespace()
                        .next()
                        .unwrap_or_default()
                        .chars()
                        .take(20)
                        .collect();
                    Some(format!("```{language}"))
                }
            };
        }
    }
    open
}

/// `markdown` as the card's Markdown shows it as written: outside code
/// blocks and code spans, `<` as `&#60;` (no tag, no mention) and an image
/// as a link.
pub fn escaped(markdown: &str) -> String {
    let mut out = String::with_capacity(markdown.len());
    let mut fenced = false;
    for line in markdown.split_inclusive('\n') {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            out.push_str(line);
            continue;
        }
        if fenced {
            out.push_str(line);
        } else {
            escape_line(line, &mut out);
        }
    }
    out
}

/// One line outside a code block, its code spans as they are.
fn escape_line(line: &str, out: &mut String) {
    let mut rest = line;
    while !rest.is_empty() {
        let Some(tick) = rest.find('`') else {
            escape_text(rest, out);
            return;
        };
        escape_text(&rest[..tick], out);
        let run = rest[tick..].chars().take_while(|c| *c == '`').count();
        let after = &rest[tick + run..];
        // The span ends at the next run of as many backticks.
        let fence = "`".repeat(run);
        if let Some(end) = after.find(&fence) {
            out.push_str(&rest[tick..tick + run + end + run]);
            rest = &after[end + run..];
        } else {
            out.push_str(&rest[tick..tick + run]);
            rest = after;
        }
    }
}

fn escape_text(text: &str, out: &mut String) {
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '<' => out.push_str("&#60;"),
            '!' if chars.peek() == Some(&'[') => {}
            c => out.push(c),
        }
    }
}

/// `text` for a plain text message, its tags taken apart so it mentions no
/// one: a zero-width space after each `<`.
pub fn plain(text: &str) -> String {
    text.replace('<', "<\u{200b}")
}

/// A card of `markdown`, with `buttons` under it, whose presses name
/// `to`, the recipient. Buttons with no label or no data, or with the data
/// of another, are left out: the text says how to answer by typing.
pub fn text_card(markdown: &str, buttons: &[Vec<Button>], to: &str) -> Value {
    let mut elements = vec![markdown_element(markdown)];
    let mut seen = std::collections::HashSet::new();
    let mut left = BUTTONS_MAX;
    for row in buttons {
        let columns: Vec<Value> = row
            .iter()
            .filter(|b| !b.label.trim().is_empty() && !b.data.is_empty() && seen.insert(&b.data))
            .take(left)
            .map(|b| {
                json!({
                    "tag": "column",
                    "width": "auto",
                    "elements": [button(&b.label, json!({ SAID: b.data, TO: to }))],
                })
            })
            .collect();
        left -= columns.len();
        if !columns.is_empty() {
            elements.push(json!({ "tag": "column_set", "flex_mode": "flow", "columns": columns }));
        }
    }
    card(elements)
}

/// A question of several answers: its text, then a menu that takes
/// several and a button that sends the numbers chosen; `None` when it has
/// no options or more than [`OPTIONS_MAX`], to go as text.
pub fn question_card(markdown: &str, poll: &Poll, to: &str) -> Option<Value> {
    if poll.options.is_empty() || poll.options.len() > OPTIONS_MAX {
        return None;
    }
    let options: Vec<Value> = poll
        .options
        .iter()
        .enumerate()
        .map(|(i, label)| {
            json!({
                "text": { "tag": "plain_text", "content": label.chars().take(LABEL_CHARS).collect::<String>() },
                "value": (i + 1).to_string(),
            })
        })
        .collect();
    let mut submit = button(
        "Send",
        json!({ KIND: QUESTION, ANSWER: poll.answer, TO: to }),
    );
    submit["form_action_type"] = json!("submit");
    submit["name"] = json!("send");
    submit["type"] = json!("primary");
    Some(card(vec![
        markdown_element(markdown),
        json!({
            "tag": "form",
            "name": "question",
            "elements": [
                {
                    "tag": "multi_select_static",
                    "name": CHOICES,
                    "placeholder": { "tag": "plain_text", "content": "Choose" },
                    "options": options,
                    "required": true,
                },
                submit,
            ],
        }),
    ]))
}

/// A card of `markdown` alone: what a card becomes once its buttons were
/// pressed.
pub fn bare(markdown: &str) -> Value {
    card(vec![markdown_element(markdown)])
}

fn markdown_element(markdown: &str) -> Value {
    json!({ "tag": "markdown", "content": escaped(markdown) })
}

fn button(label: &str, value: Value) -> Value {
    json!({
        "tag": "button",
        "type": "default",
        "text": { "tag": "plain_text", "content": label.trim().chars().take(LABEL_CHARS).collect::<String>() },
        "behaviors": [{ "type": "callback", "value": value }],
    })
}

fn card(elements: Vec<Value>) -> Value {
    json!({
        "schema": "2.0",
        "config": { "update_multi": true },
        "body": { "elements": elements },
    })
}

/// Cards with buttons or a question the bot sent, remembered while it
/// runs, up to [`CARDS_KEPT`]: each one's Markdown, to show it without its
/// buttons once pressed, and those pressed, which take no second press.
#[derive(Debug, Default)]
pub struct Cards {
    shown: std::collections::HashMap<String, String>,
    order: std::collections::VecDeque<String>,
    pressed: std::collections::HashSet<String>,
    pressed_order: std::collections::VecDeque<String>,
}

/// What a press on a card finds.
#[derive(Debug, PartialEq, Eq)]
pub enum Pressed {
    /// It was pressed before.
    Before,
    /// It was not: its Markdown, when it is remembered.
    Now(Option<String>),
}

/// Cards remembered at most, and presses.
const CARDS_KEPT: usize = 256;
const PRESSES_KEPT: usize = 1024;

impl Cards {
    /// The card `message_id`, of `markdown`, was sent.
    pub fn note(&mut self, message_id: &str, markdown: &str) {
        if self
            .shown
            .insert(message_id.to_string(), markdown.to_string())
            .is_none()
        {
            self.order.push_back(message_id.to_string());
        }
        while self.order.len() > CARDS_KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.shown.remove(&oldest);
            }
        }
    }

    /// A press on card `message_id`.
    pub fn press(&mut self, message_id: &str) -> Pressed {
        if !self.pressed.insert(message_id.to_string()) {
            return Pressed::Before;
        }
        self.pressed_order.push_back(message_id.to_string());
        while self.pressed_order.len() > PRESSES_KEPT {
            if let Some(oldest) = self.pressed_order.pop_front() {
                self.pressed.remove(&oldest);
            }
        }
        self.order.retain(|m| m != message_id);
        Pressed::Now(self.shown.remove(message_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_reply_is_one_part_and_a_long_one_is_cut_at_paragraphs() {
        assert_eq!(split("hello"), ["hello"]);
        let para = "word ".repeat(300);
        let text = format!("{para}\n\n{para}\n\n{para}\n\n{para}");
        let parts = split(&text);
        assert!(parts.len() > 1);
        assert!(parts.iter().all(|p| p.chars().count() <= PART_CHARS));
    }

    #[test]
    fn a_code_block_cut_in_two_is_closed_and_opened_again() {
        let code = "let x = 1;\n".repeat(800);
        let text = format!("```rust\n{code}```");
        let parts = split(&text);
        assert!(parts.len() > 1);
        assert!(parts[0].starts_with("```rust\n") && parts[0].ends_with("\n```"));
        assert!(parts[1].starts_with("```rust\n"));
    }

    #[test]
    fn tags_and_images_outside_code_show_as_written() {
        assert_eq!(
            escaped("hi <at id=all></at> and <font color=red>x</font>"),
            "hi &#60;at id=all>&#60;/at> and &#60;font color=red>x&#60;/font>"
        );
        assert_eq!(
            escaped("see ![a cat](https://x/c.png)"),
            "see [a cat](https://x/c.png)"
        );
        // Code keeps them: a < b in code is code.
        assert_eq!(escaped("`a < b` and <x>"), "`a < b` and &#60;x>");
        assert_eq!(
            escaped("```\nif a < b {}\n```\n<b>"),
            "```\nif a < b {}\n```\n&#60;b>"
        );
        assert_eq!(escaped("``a ` < b`` <"), "``a ` < b`` &#60;");
        // A span never closed is not code.
        assert_eq!(escaped("`open < x"), "`open &#60; x");
        assert_eq!(
            escaped("**bold** _it_ ~~s~~ [l](https://x)"),
            "**bold** _it_ ~~s~~ [l](https://x)"
        );
        assert_eq!(
            plain("<at user_id=\"all\"></at>"),
            "<\u{200b}at user_id=\"all\"><\u{200b}/at>"
        );
    }

    #[test]
    fn buttons_go_in_rows_under_the_text_and_name_their_recipient() {
        let b = |label: &str, data: &str| Button {
            label: label.into(),
            data: data.into(),
        };
        let card = text_card(
            "Approve?",
            &[
                vec![b("Approve", "/approve 1"), b("Deny", "/deny 1")],
                vec![b("", "/x"), b("Again", "/approve 1")],
            ],
            "lark:on_a",
        );
        let elements = card["body"]["elements"].as_array().unwrap();
        assert_eq!(elements.len(), 2, "the empty and the repeated are left out");
        assert_eq!(elements[0]["content"], "Approve?");
        let columns = elements[1]["columns"].as_array().unwrap();
        assert_eq!(columns.len(), 2);
        let pressed = &columns[1]["elements"][0];
        assert_eq!(pressed["text"]["content"], "Deny");
        assert_eq!(
            pressed["behaviors"][0]["value"],
            json!({ "said": "/deny 1", "to": "lark:on_a" })
        );
        assert_eq!(card["schema"], "2.0");
    }

    #[test]
    fn a_card_is_pressed_once_and_remembered_up_to_a_bound() {
        let mut cards = Cards::default();
        cards.note("om_1", "Approve?");
        assert_eq!(cards.press("om_1"), Pressed::Now(Some("Approve?".into())));
        assert_eq!(cards.press("om_1"), Pressed::Before);
        assert_eq!(cards.press("om_unknown"), Pressed::Now(None));
        for n in 0..=CARDS_KEPT {
            cards.note(&format!("om_x{n}"), "x");
        }
        assert_eq!(
            cards.press("om_x0"),
            Pressed::Now(None),
            "the oldest was let go"
        );
        assert_eq!(
            cards.press(&format!("om_x{CARDS_KEPT}")),
            Pressed::Now(Some("x".into()))
        );
    }

    #[test]
    fn a_question_is_a_menu_in_a_form_with_a_send_button() {
        let poll = Poll {
            question: "Which?".into(),
            options: vec!["Red".into(), "Blue".into()],
            answer: "/answer q1".into(),
        };
        let card = question_card("Which?\n1. Red\n2. Blue", &poll, "lark:on_a").unwrap();
        let form = &card["body"]["elements"][1];
        assert_eq!(form["tag"], "form");
        let menu = &form["elements"][0];
        assert_eq!(menu["tag"], "multi_select_static");
        assert_eq!(menu["name"], CHOICES);
        assert_eq!(menu["options"][1]["value"], "2");
        let send = &form["elements"][1];
        assert_eq!(send["form_action_type"], "submit");
        assert_eq!(send["behaviors"][0]["value"][ANSWER], "/answer q1");
        let many = Poll {
            options: (0..=OPTIONS_MAX).map(|n| n.to_string()).collect(),
            ..poll
        };
        assert!(question_card("x", &many, "lark:on_a").is_none());
    }
}
