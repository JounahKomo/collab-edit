//! Client-side chat state: channel list, active channel and message history.

use crate::ChatMessage;

#[derive(Debug, Clone)]
pub struct ClientChatState {
    pub channels: Vec<String>,
    pub active_channel: String,
    pub messages: Vec<ChatMessage>,
}

impl Default for ClientChatState {
    fn default() -> Self {
        Self {
            channels: vec!["general".to_string()],
            active_channel: "general".to_string(),
            messages: Vec::new(),
        }
    }
}

impl ClientChatState {
    pub fn add_channel(&mut self, channel: &str) {
        if !self.channels.iter().any(|c| c == channel) {
            self.channels.push(channel.to_string());
        }
    }

    pub fn add_message(&mut self, sender: &str, channel: &str, text: &str) {
        self.add_channel(channel);
        self.messages.push(ChatMessage {
            sender: sender.into(),
            channel: channel.into(),
            text: text.into(),
        });
    }
}
