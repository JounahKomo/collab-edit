//! Shared client context used by the UI callbacks.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering},
    mpsc,
};

use crate::{
    AppWindow,
    chat_state::ClientChatState,
    cursors::CursorSender,
    doc_state::ClientDocState,
    net::make_frame,
    ui_sync::{sync_chat_ui, sync_doc_ui},
};

pub fn next_msg_id(counter: &AtomicU32) -> u32 {
    let id = counter.fetch_add(2, Ordering::SeqCst);
    if id.is_multiple_of(2) {
        counter.fetch_add(1, Ordering::SeqCst);
    }
    id
}

/// Helper context bundling state, UI handle, transmitter, and message ID generator.
#[derive(Clone)]
pub struct ClientContext {
    pub ui: slint::Weak<AppWindow>,
    pub doc_state: Arc<Mutex<ClientDocState>>,
    pub chat_state: Arc<Mutex<ClientChatState>>,
    pub tx_net: mpsc::Sender<Vec<u8>>,
    pub msg_counter: Arc<AtomicU32>,
    pub cursor_sender: Arc<CursorSender>,
}

impl ClientContext {
    /// Mutates the client document state, immediately refreshes the UI, and returns the callback's result.
    pub fn with_doc_state_mut<R>(&self, f: impl FnOnce(&mut ClientDocState) -> R) -> R {
        let mut st = self.doc_state.lock().unwrap();
        let res = f(&mut st);
        if let Some(ui) = self.ui.upgrade() {
            sync_doc_ui(&ui, &st);
        }
        self.cursor_sender
            .send_if_changed(st.cursor_line, st.cursor_col);
        res
    }

    /// Mutates the client chat state and refreshes the chat UI.
    pub fn with_chat_state_mut<R>(&self, f: impl FnOnce(&mut ClientChatState) -> R) -> R {
        let mut ch = self.chat_state.lock().unwrap();
        let res = f(&mut ch);
        if let Some(ui) = self.ui.upgrade() {
            sync_chat_ui(&ui, &ch);
        }
        res
    }

    /// Generates a unique client message ID and queues a frame to be sent to the server.
    pub fn send_frame(&self, msg_type: &[u8; 4], payload: &str) -> u32 {
        let id = next_msg_id(&self.msg_counter);
        let _ = self.tx_net.send(make_frame(id, msg_type, payload));
        id
    }

    /// Sends an INS/DEL request that was already applied locally, remembering it
    /// so stale server snapshots are not applied before the server answers.
    pub fn send_edit(&self, msg_type: &[u8; 4], payload: &str) {
        let mut st = self.doc_state.lock().unwrap();
        let id = self.send_frame(msg_type, payload);
        st.edit_sent(id);
    }
}
