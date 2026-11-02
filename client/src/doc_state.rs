//! Document state management for the CollabEdit client.

/// Maximum number of characters (columns) per line, matching the server's limit.
pub const MAX_COLUMNS: usize = 80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientDocState {
    pub lines: Vec<String>,
    pub cursor_line: usize,
    pub cursor_col: usize,
    pub revision: u32,
}

impl Default for ClientDocState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientDocState {
    /// Creates a new document state with a single empty line at revision 0.
    pub fn new() -> Self {
        Self {
            lines: vec![String::new()],
            cursor_line: 0,
            cursor_col: 0,
            revision: 0,
        }
    }

    /// Replaces the document content and revision with state received from the server,
    /// clamping the current user cursor within the new boundaries.
    pub fn update_from_server(&mut self, revision: u32, content: &str) {
        self.revision = revision;
        let mut new_lines: Vec<String> = content.split('\n').map(|s| s.to_string()).collect();
        if new_lines.is_empty() {
            new_lines.push(String::new());
        }
        self.lines = new_lines;
        self.clamp_cursor();
    }

    /// Ensures cursor coordinates do not point outside the current lines vector or column lengths.
    pub fn clamp_cursor(&mut self) {
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        if self.cursor_line >= self.lines.len() {
            self.cursor_line = self.lines.len() - 1;
        }
        let line_len = self.lines[self.cursor_line].chars().count();
        if self.cursor_col > line_len {
            self.cursor_col = line_len;
        }
    }

    /// Sets the cursor directly to a target line and column, clamping within valid boundaries.
    pub fn set_cursor_pos(&mut self, line: usize, col: usize) {
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        self.cursor_line = line.min(self.lines.len() - 1);
        let line_len = self.lines[self.cursor_line].chars().count();
        self.cursor_col = col.min(line_len);
    }

    /// Splits the line containing the cursor into prefix (before cursor) and suffix (after cursor).
    pub fn line_prefix_suffix(&self) -> (String, String) {
        if self.cursor_line >= self.lines.len() {
            return (String::new(), String::new());
        }
        let line = &self.lines[self.cursor_line];
        let col = self.cursor_col.min(line.chars().count());
        let prefix: String = line.chars().take(col).collect();
        let suffix: String = line.chars().skip(col).collect();
        (prefix, suffix)
    }

    /// Moves the cursor in the specified direction ("up", "down", "left", "right").
    pub fn move_cursor(&mut self, dir: &str) {
        match dir {
            "up" => {
                if self.cursor_line > 0 {
                    self.cursor_line -= 1;
                    let line_len = self.lines[self.cursor_line].chars().count();
                    if self.cursor_col > line_len {
                        self.cursor_col = line_len;
                    }
                }
            }
            "down" => {
                if self.cursor_line + 1 < self.lines.len() {
                    self.cursor_line += 1;
                    let line_len = self.lines[self.cursor_line].chars().count();
                    if self.cursor_col > line_len {
                        self.cursor_col = line_len;
                    }
                }
            }
            "left" => {
                if self.cursor_col > 0 {
                    self.cursor_col -= 1;
                } else if self.cursor_line > 0 {
                    self.cursor_line -= 1;
                    self.cursor_col = self.lines[self.cursor_line].chars().count();
                }
            }
            "right" => {
                let line_len = self.lines[self.cursor_line].chars().count();
                if self.cursor_col < line_len {
                    self.cursor_col += 1;
                } else if self.cursor_line + 1 < self.lines.len() {
                    self.cursor_line += 1;
                    self.cursor_col = 0;
                }
            }
            _ => {}
        }
    }

    /// Inserts single-line text at the current cursor position, wrapping overflowing
    /// characters across subsequent lines at MAX_COLUMNS (80), and advances the cursor.
    pub fn insert_text(&mut self, text: &str) {
        self.clamp_cursor();
        let target_line = self.cursor_line;
        let target_col = self.cursor_col;

        let line = &mut self.lines[target_line];
        let prefix: String = line.chars().take(target_col).collect();
        let suffix: String = line.chars().skip(target_col).collect();
        *line = format!("{}{}{}", prefix, text, suffix);

        // Advance cursor, wrapping to next line(s) if exceeding MAX_COLUMNS
        let mut c_line = target_line;
        let mut c_col = target_col + text.chars().count();
        while c_col > MAX_COLUMNS {
            c_line += 1;
            c_col -= MAX_COLUMNS;
        }

        self.wrap_lines_from(target_line);
        self.cursor_line = c_line;
        self.cursor_col = c_col;
        self.clamp_cursor();
    }

    /// Inserts a newline at the cursor, splitting the current line and moving to (line + 1, col 0).
    pub fn insert_newline(&mut self) {
        self.clamp_cursor();
        let line = &self.lines[self.cursor_line];
        let col = self.cursor_col;
        let prefix: String = line.chars().take(col).collect();
        let suffix: String = line.chars().skip(col).collect();

        self.lines[self.cursor_line] = prefix;
        self.lines.insert(self.cursor_line + 1, suffix);
        self.cursor_line += 1;
        self.cursor_col = 0;
    }

    /// Deletes the character before the cursor or merges with the previous line if at col 0.
    /// Returns Some((line, col, count)) specifying the delete operation performed, or None if at doc start.
    pub fn backspace(&mut self) -> Option<(usize, usize, usize)> {
        self.clamp_cursor();
        if self.cursor_col > 0 {
            let del_col = self.cursor_col - 1;
            let line = &mut self.lines[self.cursor_line];
            let prefix: String = line.chars().take(del_col).collect();
            let suffix: String = line.chars().skip(self.cursor_col).collect();
            *line = format!("{}{}", prefix, suffix);
            self.cursor_col = del_col;
            Some((self.cursor_line, del_col, 1))
        } else if self.cursor_line > 0 {
            let prev_line_idx = self.cursor_line - 1;
            let prev_len = self.lines[prev_line_idx].chars().count();
            let cur_line = self.lines.remove(self.cursor_line);
            self.lines[prev_line_idx].push_str(&cur_line);
            self.cursor_line = prev_line_idx;
            self.cursor_col = prev_len;
            self.wrap_lines_from(prev_line_idx);
            self.clamp_cursor();
            Some((prev_line_idx, prev_len, 1))
        } else {
            None
        }
    }

    /// Cascading wrap for any lines that exceed MAX_COLUMNS (80), prepending
    /// overflowing characters into the next line.
    pub fn wrap_lines_from(&mut self, start_line: usize) {
        let mut cur_line = start_line;
        while cur_line < self.lines.len() {
            let char_count = self.lines[cur_line].chars().count();
            if char_count > MAX_COLUMNS {
                let kept: String = self.lines[cur_line].chars().take(MAX_COLUMNS).collect();
                let overflow: String = self.lines[cur_line].chars().skip(MAX_COLUMNS).collect();
                self.lines[cur_line] = kept;
                if cur_line + 1 < self.lines.len() {
                    let next_line = format!("{}{}", overflow, self.lines[cur_line + 1]);
                    self.lines[cur_line + 1] = next_line;
                } else {
                    self.lines.push(overflow);
                }
            }
            cur_line += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let state = ClientDocState::new();
        assert_eq!(state.lines, vec![""]);
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 0);
        assert_eq!(state.revision, 0);
        assert_eq!(state.line_prefix_suffix(), (String::new(), String::new()));
    }

    #[test]
    fn test_set_cursor_pos() {
        let mut state = ClientDocState::new();
        state.lines = vec!["hello".to_string(), "world".to_string()];

        state.set_cursor_pos(0, 3);
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 3);

        // Clamp past line length
        state.set_cursor_pos(0, 20);
        assert_eq!(state.cursor_col, 5);

        // Clamp past line count
        state.set_cursor_pos(10, 2);
        assert_eq!(state.cursor_line, 1);
        assert_eq!(state.cursor_col, 2);
    }

    #[test]
    fn test_insert_and_split() {
        let mut state = ClientDocState::new();
        state.insert_text("hello world");
        assert_eq!(state.lines[0], "hello world");
        assert_eq!(state.cursor_col, 11);

        // Move left 5 characters to "hello "
        for _ in 0..5 {
            state.move_cursor("left");
        }
        assert_eq!(state.cursor_col, 6);
        assert_eq!(
            state.line_prefix_suffix(),
            ("hello ".to_string(), "world".to_string())
        );

        // Split line
        state.insert_newline();
        assert_eq!(state.lines, vec!["hello ", "world"]);
        assert_eq!(state.cursor_line, 1);
        assert_eq!(state.cursor_col, 0);
    }

    #[test]
    fn test_insert_text_wrapping() {
        let mut state = ClientDocState::new();
        let s = "a".repeat(MAX_COLUMNS);
        state.insert_text(&s);
        assert_eq!(state.lines.len(), 1);
        assert_eq!(state.lines[0].chars().count(), MAX_COLUMNS);
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, MAX_COLUMNS);

        // Typing another character at column 80 wraps to line 1, col 1
        state.insert_text("b");
        assert_eq!(state.lines.len(), 2);
        assert_eq!(state.lines[0], "a".repeat(MAX_COLUMNS));
        assert_eq!(state.lines[1], "b");
        assert_eq!(state.cursor_line, 1);
        assert_eq!(state.cursor_col, 1);
    }

    #[test]
    fn test_insert_text_cascade_wrapping() {
        let mut state = ClientDocState::new();
        state.lines = vec!["a".repeat(MAX_COLUMNS), "b".repeat(MAX_COLUMNS)];
        state.cursor_line = 0;
        state.cursor_col = 40;

        // Inserting "XYZ" at col 40 of line 0
        state.insert_text("XYZ");
        assert_eq!(state.lines.len(), 3);
        assert_eq!(state.lines[0].chars().count(), MAX_COLUMNS);
        assert_eq!(state.lines[1].chars().count(), MAX_COLUMNS);
        assert_eq!(state.lines[2], "bbb");
        // Cursor was at col 40, advanced by 3 to col 43 on line 0
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 43);
    }

    #[test]
    fn test_backspace_within_line_and_merge() {
        let mut state = ClientDocState::new();
        state.insert_text("ab");
        state.insert_newline();
        state.insert_text("cd");
        assert_eq!(state.lines, vec!["ab", "cd"]);

        // Backspace 'd'
        let op1 = state.backspace();
        assert_eq!(op1, Some((1, 1, 1)));
        assert_eq!(state.lines, vec!["ab", "c"]);
        assert_eq!(state.cursor_col, 1);

        // Backspace 'c'
        let op2 = state.backspace();
        assert_eq!(op2, Some((1, 0, 1)));
        assert_eq!(state.lines, vec!["ab", ""]);
        assert_eq!(state.cursor_col, 0);

        // Backspace at col 0 -> merge lines
        let op3 = state.backspace();
        assert_eq!(op3, Some((0, 2, 1)));
        assert_eq!(state.lines, vec!["ab"]);
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 2);

        // Backspace at (0, 0) -> None
        state.cursor_col = 0;
        assert_eq!(state.backspace(), None);
    }

    #[test]
    fn test_update_from_server_clamps_cursor() {
        let mut state = ClientDocState::new();
        state.lines = vec!["very long line here".to_string(), "second line".to_string()];
        state.cursor_line = 1;
        state.cursor_col = 10;

        // Server updates with single short line
        state.update_from_server(5, "hi");
        assert_eq!(state.revision, 5);
        assert_eq!(state.lines, vec!["hi"]);
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 2);
    }

    #[test]
    fn test_navigation_boundaries() {
        let mut state = ClientDocState::new();
        state.lines = vec!["one".to_string(), "two".to_string()];

        // Up at top line does nothing
        state.move_cursor("up");
        assert_eq!(state.cursor_line, 0);

        // Down to line 1
        state.move_cursor("down");
        assert_eq!(state.cursor_line, 1);

        // Down at bottom line does nothing
        state.move_cursor("down");
        assert_eq!(state.cursor_line, 1);

        // Left wraps to end of previous line when at col 0
        state.cursor_col = 0;
        state.move_cursor("left");
        assert_eq!(state.cursor_line, 0);
        assert_eq!(state.cursor_col, 3);

        // Right wraps to start of next line when at line end
        state.move_cursor("right");
        assert_eq!(state.cursor_line, 1);
        assert_eq!(state.cursor_col, 0);
    }
}
