//! Pushes client state into the Slint UI.

use crate::{
    AppWindow, ChatMessage, PageData, PageLineData, chat_state::ClientChatState,
    doc_state::ClientDocState,
};

/// Number of document lines shown on one page.
const LINES_PER_PAGE: usize = 35;

pub fn sync_doc_ui(ui: &AppWindow, state: &ClientDocState) {
    ui.set_cursor_line(state.cursor_line as i32);
    ui.set_cursor_col(state.cursor_col as i32);

    let (prefix, suffix) = state.line_prefix_suffix();
    ui.set_line_prefix(prefix.into());
    ui.set_line_suffix(suffix.into());

    // Group lines into pages of LINES_PER_PAGE lines
    let total_lines = state.lines.len();
    let num_pages = (total_lines.div_ceil(LINES_PER_PAGE)).max(1);

    let mut pages_vec: Vec<PageData> = Vec::with_capacity(num_pages);
    for page_idx in 0..num_pages {
        let start_line = page_idx * LINES_PER_PAGE;
        let end_line = (start_line + LINES_PER_PAGE).min(total_lines);

        let mut page_lines: Vec<PageLineData> = Vec::with_capacity(LINES_PER_PAGE);
        for global_idx in start_line..end_line {
            page_lines.push(PageLineData {
                global_idx: global_idx as i32,
                text: state.lines[global_idx].as_str().into(),
            });
        }

        pages_vec.push(PageData {
            page_number: (page_idx + 1) as i32,
            lines: slint::ModelRc::new(slint::VecModel::from(page_lines)),
        });
    }

    ui.set_pages(slint::ModelRc::new(slint::VecModel::from(pages_vec)));
}

pub fn sync_chat_ui(ui: &AppWindow, chat: &ClientChatState) {
    let ch_model: Vec<slint::SharedString> = chat
        .channels
        .iter()
        .map(|c| slint::SharedString::from(c.as_str()))
        .collect();
    ui.set_channels(slint::ModelRc::new(slint::VecModel::from(ch_model)));
    ui.set_active_channel(chat.active_channel.as_str().into());

    let msg_model: Vec<ChatMessage> = chat
        .messages
        .iter()
        .filter(|m| m.channel == chat.active_channel)
        .cloned()
        .collect();
    ui.set_messages(slint::ModelRc::new(slint::VecModel::from(msg_model)));
}
