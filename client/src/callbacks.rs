//! Wires Slint UI callbacks to document edits, chat actions and network sends.

use crate::{AppWindow, context::ClientContext};

pub fn register_ui_callbacks(ui: &AppWindow, ctx: ClientContext) {
    let ctx_login = ctx.clone();
    ui.on_login_submitted(move |name| {
        let clean_name = name.trim().to_string();
        if !clean_name.is_empty() {
            ctx_login.send_frame(b"USR ", &clean_name);
            ctx_login.send_frame(b"GET ", "");
        }
    });

    let ctx_key = ctx.clone();
    ui.on_key_typed(move |text| {
        if text.is_empty() || text.chars().any(|c| c.is_control()) {
            return;
        }
        let (line, col) = ctx_key.with_doc_state_mut(|st| {
            let line = st.cursor_line;
            let col = st.cursor_col;
            st.insert_text(&text);
            (line, col)
        });
        ctx_key.send_frame(b"INS ", &format!("{} {} {}", line, col, text));
    });

    let ctx_enter = ctx.clone();
    ui.on_enter_pressed(move || {
        let (line, col) = ctx_enter.with_doc_state_mut(|st| {
            let line = st.cursor_line;
            let col = st.cursor_col;
            st.insert_newline();
            (line, col)
        });
        ctx_enter.send_frame(b"INS ", &format!("{} {} \n", line, col));
    });

    let ctx_backspace = ctx.clone();
    ui.on_backspace_pressed(move || {
        let del_op = ctx_backspace.with_doc_state_mut(|st| st.backspace());
        if let Some((line, col, count)) = del_op {
            ctx_backspace.send_frame(b"DEL ", &format!("{} {} {}", line, col, count));
        }
    });

    let ctx_arrow = ctx.clone();
    ui.on_arrow_pressed(move |dir| {
        ctx_arrow.with_doc_state_mut(|st| {
            st.move_cursor(&dir);
        });
    });

    let ctx_click = ctx.clone();
    ui.on_cursor_position_clicked(move |line, col| {
        if line >= 0 && col >= 0 {
            ctx_click.with_doc_state_mut(|st| {
                st.set_cursor_pos(line as usize, col as usize);
            });
        }
    });

    let ctx_select_ch = ctx.clone();
    ui.on_channel_selected(move |channel| {
        ctx_select_ch.with_chat_state_mut(|ch| {
            ch.active_channel = channel.to_string();
        });
    });

    let ctx_send_msg = ctx.clone();
    ui.on_send_chat_message(move |channel, text| {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            ctx_send_msg.send_frame(b"MSG ", &format!("{} {}", channel, trimmed));
        }
    });

    let ctx_add_ch = ctx;
    ui.on_add_channel_submitted(move |new_channel| {
        let trimmed = new_channel.trim().to_string();
        if !trimmed.is_empty() {
            ctx_add_ch.with_chat_state_mut(|ch| {
                ch.add_channel(&trimmed);
                ch.active_channel = trimmed.clone();
            });
            // Send initial ping to create the channel on the server
            ctx_add_ch.send_frame(b"MSG ", &format!("{} joined channel #{}", trimmed, trimmed));
        }
    });
}
