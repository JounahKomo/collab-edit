//! One handler per TCP message type. Handlers return `Err(reason)` to have the
//! dispatcher answer with an `ERR ` frame and count the error.

use tracing::{error, info, warn};

use crate::{
    protocol::{create_error, frame},
    state::{ClientId, State},
};

/// A decoded request frame.
pub struct Request<'a> {
    pub id: u32,
    pub msg_type: &'a [u8],
    pub payload: &'a [u8],
    /// The complete frame, for handlers that echo it back unchanged.
    pub raw: &'a [u8],
}

/// Everything a handler may touch: the shared state, the identity of the
/// requesting client and that client's outgoing buffer.
pub struct Context<'a> {
    pub client_id: ClientId,
    pub state: &'a mut State,
    pub out: &'a mut Vec<u8>,
}

type HandlerResult = Result<(), String>;

const NOT_REGISTERED: &str = "Not registered. Send USR first.";
/// Longest accepted UDP registration token.
const MAX_TOKEN_LEN: usize = 64;

/// Runs the handler for the request's message type and turns a failure into an
/// `ERR ` reply that carries the request's ID.
pub fn dispatch(ctx: &mut Context, req: &Request) {
    let result = match req.msg_type {
        b"MET " => handle_met(ctx, req),
        b"TST " => handle_tst(ctx, req),
        b"USR " => handle_usr(ctx, req),
        b"UDP " => handle_udp(ctx, req),
        b"MSG " => handle_msg(ctx, req),
        b"GET " => handle_get(ctx, req),
        b"INS " => handle_ins(ctx, req),
        b"DEL " => handle_del(ctx, req),
        _ => Err("Unknown message type".to_string()),
    };

    if let Err(reason) = result {
        warn!("Request {} failed: {reason}", req.id);
        ctx.state.metrics.errors += 1;
        ctx.out.extend_from_slice(&create_error(req.id, &reason));
    }
}

impl Context<'_> {
    fn username(&self) -> Option<String> {
        self.state
            .clients
            .get(&self.client_id)
            .and_then(|c| c.username.clone())
    }

    /// The requesting client's username, or the "not registered" error.
    fn require_username(&self) -> Result<String, String> {
        self.username().ok_or_else(|| NOT_REGISTERED.to_string())
    }

    fn echo(&mut self, req: &Request) {
        self.out.extend_from_slice(req.raw);
    }

    /// Sends the current document to everyone (sender included).
    fn broadcast_document(&mut self) {
        let payload = document_payload(self.state);
        self.state
            .broadcast_to_all(self.client_id, self.out, b"GET ", payload.as_bytes());
    }
}

fn document_payload(state: &State) -> String {
    format!("{}\n{}", state.document.revision, state.document.text())
}

fn payload_str<'a>(req: &'a Request, name: &str) -> Result<&'a str, String> {
    std::str::from_utf8(req.payload).map_err(|e| {
        warn!("Failed parsing of {name} payload: {e}");
        format!("Invalid UTF-8 in {name} payload")
    })
}

fn parse_usize(text: &str, what: &str, command: &str) -> Result<usize, String> {
    text.parse()
        .map_err(|_| format!("Invalid {what} in {command}"))
}

fn handle_met(ctx: &mut Context, req: &Request) -> HandlerResult {
    let json = serde_json::to_string(&ctx.state.metrics).map_err(|e| {
        error!("Failed to serialize metrics: {e}");
        "Internal error serializing metrics".to_string()
    })?;
    ctx.out
        .extend_from_slice(&frame(req.id, b"MET ", json.as_bytes()));
    Ok(())
}

fn handle_tst(ctx: &mut Context, req: &Request) -> HandlerResult {
    ctx.echo(req);
    Ok(())
}

fn handle_usr(ctx: &mut Context, req: &Request) -> HandlerResult {
    let requested = std::str::from_utf8(req.payload)
        .map_err(|_| "Invalid UTF-8 in username".to_string())?
        .to_string();

    let is_taken = ctx
        .state
        .clients
        .values()
        .any(|c| c.username.as_deref() == Some(requested.as_str()));
    if is_taken {
        return Err("Username already taken!".to_string());
    }

    if let Some(me) = ctx.state.clients.get_mut(&ctx.client_id) {
        me.username = Some(requested);
    }
    ctx.echo(req);
    Ok(())
}

/// `UDP <token>`: announces the token that the UDP hello datagram must carry.
fn handle_udp(ctx: &mut Context, req: &Request) -> HandlerResult {
    ctx.require_username()?;
    let token = std::str::from_utf8(req.payload)
        .ok()
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.len() <= MAX_TOKEN_LEN && !t.contains(' '))
        .ok_or("Invalid UDP syntax. Expected: <token>")?;

    if let Some(me) = ctx.state.clients.get_mut(&ctx.client_id) {
        me.udp_token = Some(token.to_string());
        me.udp_addr = None;
    }
    ctx.echo(req);
    Ok(())
}

fn handle_msg(ctx: &mut Context, req: &Request) -> HandlerResult {
    let sender = ctx.require_username()?;
    let text = payload_str(req, "MSG")?;
    let (channel, message) = text
        .split_once(' ')
        .ok_or("Invalid MSG syntax. Expected: <channel> <message>")?;

    // Unknown channels are created dynamically.
    if ctx.state.channels.insert(channel.to_string()) {
        info!("Created new channel: {channel}");
    }

    let broadcast = format!("{sender} {channel} {message}");
    ctx.state
        .broadcast_to_all(ctx.client_id, ctx.out, b"MSG ", broadcast.as_bytes());
    Ok(())
}

fn handle_get(ctx: &mut Context, req: &Request) -> HandlerResult {
    ctx.require_username()?;
    let payload = document_payload(ctx.state);
    ctx.out
        .extend_from_slice(&frame(req.id, b"GET ", payload.as_bytes()));
    Ok(())
}

/// `INS <line> <col> <text>`
fn handle_ins(ctx: &mut Context, req: &Request) -> HandlerResult {
    ctx.require_username()?;
    let text = payload_str(req, "INS")?;
    let (line_str, rest) = text
        .split_once(' ')
        .ok_or("Invalid INS syntax. Expected: <line> <col> <text>")?;
    let (col_str, inserted) = rest.split_once(' ').unwrap_or((rest, ""));
    let line = parse_usize(line_str, "line number", "INS")?;
    let col = parse_usize(col_str, "column number", "INS")?;

    ctx.state.document.insert(line, col, inserted)?;
    ctx.echo(req);
    ctx.broadcast_document();
    Ok(())
}

/// `DEL <line> <col> <count>`
fn handle_del(ctx: &mut Context, req: &Request) -> HandlerResult {
    ctx.require_username()?;
    let text = payload_str(req, "DEL")?;
    let mut parts = text.split_whitespace();
    let (Some(line_str), Some(col_str), Some(count_str), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("Invalid DEL syntax. Expected: <line> <col> <count>".to_string());
    };
    let line = parse_usize(line_str, "line number", "DEL")?;
    let col = parse_usize(col_str, "column number", "DEL")?;
    let count = parse_usize(count_str, "count", "DEL")?;

    ctx.state.document.delete(line, col, count)?;
    ctx.echo(req);
    ctx.broadcast_document();
    Ok(())
}
