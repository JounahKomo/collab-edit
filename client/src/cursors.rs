//! UDP cursor sharing: sends the local cursor position to the server and
//! tracks the cursors of the other users that the server relays back.

use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{AppWindow, RemoteCursor, net::make_frame};

/// A remote cursor is dropped when the server has not refreshed it for this
/// long (the server re-sends all known cursors every second).
const CURSOR_TTL: Duration = Duration::from_secs(5);

/// Sends local cursor updates over UDP once the server knows our endpoint.
pub struct CursorSender {
    socket: Arc<UdpSocket>,
    registered: AtomicBool,
    last_sent: Mutex<Option<(usize, usize)>>,
    /// Most recent local cursor, so it can be sent as soon as we are registered.
    latest: Mutex<Option<(usize, usize)>>,
    msg_id: AtomicU32,
}

impl CursorSender {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        Self {
            socket,
            registered: AtomicBool::new(false),
            last_sent: Mutex::new(None),
            latest: Mutex::new(None),
            msg_id: AtomicU32::new(1),
        }
    }

    /// Called once the server has acknowledged our hello datagram.
    pub fn mark_registered(&self) {
        if !self.registered.swap(true, Ordering::SeqCst) {
            let latest = *self.latest.lock().unwrap();
            if let Some((line, col)) = latest {
                self.send_if_changed(line, col);
            }
        }
    }

    pub fn is_registered(&self) -> bool {
        self.registered.load(Ordering::SeqCst)
    }

    /// Sends the hello datagram that lets the server learn our UDP address.
    pub fn send_hello(&self, token: &str) {
        let id = self.msg_id.fetch_add(2, Ordering::SeqCst);
        let _ = self.socket.send(&make_frame(id, b"UDP ", token));
    }

    /// Sends the cursor if it moved since the last datagram (best effort).
    pub fn send_if_changed(&self, line: usize, col: usize) {
        *self.latest.lock().unwrap() = Some((line, col));
        if !self.registered.load(Ordering::SeqCst) {
            return;
        }
        let mut last = self.last_sent.lock().unwrap();
        if *last == Some((line, col)) {
            return;
        }
        *last = Some((line, col));
        let id = self.msg_id.fetch_add(2, Ordering::SeqCst);
        let frame = make_frame(id, b"CUR ", &format!("{line} {col}"));
        let _ = self.socket.send(&frame);
    }
}

pub type RemoteCursors = Arc<Mutex<HashMap<String, (usize, usize, Instant)>>>;

/// A datagram of type `UDP ` is the server's acknowledgement of our hello.
fn is_hello_ack(data: &[u8]) -> bool {
    data.len() >= 12 && &data[8..12] == b"UDP "
}

/// Parses a datagram `len|id|CUR |<username> <line> <col>`.
fn parse_cursor(data: &[u8]) -> Option<(String, usize, usize)> {
    if data.len() < 12
        || u32::from_be_bytes(data[..4].try_into().ok()?) as usize != data.len()
        || &data[8..12] != b"CUR "
    {
        return None;
    }
    let text = std::str::from_utf8(&data[12..]).ok()?;
    // Usernames may contain spaces, so split from the right.
    let (rest, col) = text.rsplit_once(' ')?;
    let (user, line) = rest.rsplit_once(' ')?;
    Some((user.to_string(), line.parse().ok()?, col.parse().ok()?))
}

/// Stable per-user colour derived from the username.
fn user_color(name: &str) -> slint::Color {
    let hash = name
        .bytes()
        .fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619));
    let hue = (hash % 360) as f32;
    let (s, l) = (0.75f32, 0.42f32);
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((hue / 60.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;
    let (r, g, b) = match (hue / 60.0) as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };
    let to8 = |v: f32| ((v + m) * 255.0).round() as u8;
    slint::Color::from_rgb_u8(to8(r), to8(g), to8(b))
}

fn sync_cursors_ui(ui: &AppWindow, cursors: &RemoteCursors) {
    let mut list: Vec<RemoteCursor> = cursors
        .lock()
        .unwrap()
        .iter()
        .map(|(name, (line, col, _))| RemoteCursor {
            username: name.as_str().into(),
            line: *line as i32,
            col: *col as i32,
            color: user_color(name),
        })
        .collect();
    list.sort_by(|a, b| a.username.cmp(&b.username));
    ui.set_remote_cursors(slint::ModelRc::new(slint::VecModel::from(list)));
}

/// Receives relayed cursors, expires stale ones and refreshes the UI.
pub fn start_cursor_receiver(
    socket: Arc<UdpSocket>,
    ui_weak: slint::Weak<AppWindow>,
    cursors: RemoteCursors,
    own_name: Arc<Mutex<String>>,
    sender: Arc<CursorSender>,
    token: Arc<Mutex<Option<String>>>,
) {
    thread::spawn(move || {
        let _ = socket.set_read_timeout(Some(Duration::from_millis(500)));
        let mut buf = [0u8; 2048];
        loop {
            let mut changed = false;
            // Until acknowledged, keep announcing ourselves (datagrams may be lost).
            if !sender.is_registered()
                && let Some(t) = token.lock().unwrap().as_deref()
            {
                sender.send_hello(t);
            }
            let received = socket.recv(&mut buf).ok();
            if let Some(n) = received
                && is_hello_ack(&buf[..n])
            {
                sender.mark_registered();
            }
            if let Some(n) = received
                && let Some((user, line, col)) = parse_cursor(&buf[..n])
                && user != *own_name.lock().unwrap()
            {
                let mut map = cursors.lock().unwrap();
                let prev = map.insert(user, (line, col, Instant::now()));
                changed = prev.is_none_or(|(l, c, _)| (l, c) != (line, col));
            }
            {
                let mut map = cursors.lock().unwrap();
                let before = map.len();
                map.retain(|_, (_, _, seen)| seen.elapsed() < CURSOR_TTL);
                changed |= map.len() != before;
            }
            if changed {
                let ui_clone = ui_weak.clone();
                let cursors = Arc::clone(&cursors);
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_clone.upgrade() {
                        sync_cursors_ui(&ui, &cursors);
                    }
                });
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cursor_datagram() {
        let f = make_frame(2, b"CUR ", "mary jane 3 14");
        assert_eq!(parse_cursor(&f), Some(("mary jane".into(), 3, 14)));
        assert_eq!(parse_cursor(&f[..f.len() - 1]), None);
        assert_eq!(parse_cursor(&make_frame(2, b"GET ", "a 1 2")), None);
    }
}
