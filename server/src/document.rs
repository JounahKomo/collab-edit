/// Maximum number of characters (columns) per line, equivalent to font size 12
/// on a standard document page with 1-inch margins (~80 characters).
pub const MAX_COLUMNS: usize = 80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub revision: u64,
    pub lines: Vec<String>,
}

impl Default for Document {
    fn default() -> Self {
        Document {
            revision: 0,
            lines: vec![String::new()],
        }
    }
}

impl Document {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the full document text joined by newline characters.
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    /// Inserts `text` at the specified 0-indexed line and column.
    ///
    /// Free-form grid behavior:
    /// - If `line` exceeds the current number of lines, the document is automatically
    ///   padded with empty lines up to `line`.
    /// - `col` must be <= `MAX_COLUMNS`.
    /// - If `col` exceeds the current line length, spaces are automatically padded up to `col`.
    /// - When any line exceeds `MAX_COLUMNS`, characters overflowing the column limit
    ///   cascade and wrap into subsequent lines.
    pub fn insert(&mut self, line: usize, col: usize, text: &str) -> Result<(), &'static str> {
        if col > MAX_COLUMNS {
            return Err("Column exceeds maximum line width");
        }

        // Auto-grow lines if target line is beyond current line count
        if line >= self.lines.len() {
            self.lines.resize(line + 1, String::new());
        }

        // Auto-pad with spaces up to col (free-form grid)
        let cur_char_count = self.lines[line].chars().count();
        if col > cur_char_count {
            let padding = " ".repeat(col - cur_char_count);
            self.lines[line].push_str(&padding);
        }

        let prefix: String = self.lines[line].chars().take(col).collect();
        let suffix: String = self.lines[line].chars().skip(col).collect();

        if !text.contains('\n') {
            self.lines[line] = format!("{}{}{}", prefix, text, suffix);
        } else {
            let parts: Vec<&str> = text.split('\n').collect();
            let first_line = format!("{}{}", prefix, parts[0]);
            let last_line = format!("{}{}", parts.last().unwrap(), suffix);

            self.lines[line] = first_line;
            for (idx, &part) in parts[1..parts.len() - 1].iter().enumerate() {
                self.lines.insert(line + 1 + idx, part.to_string());
            }
            let last_idx = line + parts.len() - 1;
            self.lines.insert(last_idx, last_line);
        }

        self.wrap_lines_from(line);
        self.revision += 1;
        Ok(())
    }

    /// Deletes `count` characters starting at the specified 0-indexed line and column.
    pub fn delete(&mut self, line: usize, col: usize, count: usize) -> Result<(), &'static str> {
        if col > MAX_COLUMNS {
            return Err("Column exceeds maximum line width");
        }
        if line >= self.lines.len() {
            return Err("Line out of bounds");
        }
        let line_char_count = self.lines[line].chars().count();
        if col > line_char_count {
            return Err("Column out of bounds");
        }

        if count == 0 {
            return Ok(());
        }

        // Validate that count doesn't exceed total available characters to EOF
        let available_in_cur_line = line_char_count - col;
        let mut total_available = available_in_cur_line;
        for subsequent in &self.lines[line + 1..] {
            total_available += 1 + subsequent.chars().count(); // +1 for '\n'
        }

        if count > total_available {
            return Err("Delete count exceeds document length");
        }

        let mut remaining = count;
        let cur_col = col;

        while remaining > 0 {
            let cur_line_len = self.lines[line].chars().count();
            let avail = cur_line_len - cur_col;

            if remaining <= avail {
                let prefix: String = self.lines[line].chars().take(cur_col).collect();
                let suffix: String = self.lines[line].chars().skip(cur_col + remaining).collect();
                self.lines[line] = format!("{}{}", prefix, suffix);
                remaining = 0;
            } else {
                // Delete to end of current line
                self.lines[line] = self.lines[line].chars().take(cur_col).collect();
                remaining -= avail;

                // Delete the newline by merging with the next line
                if line + 1 < self.lines.len() {
                    remaining -= 1;
                    let next_line = self.lines.remove(line + 1);
                    self.lines[line].push_str(&next_line);
                }
            }
        }

        self.wrap_lines_from(line);
        self.revision += 1;
        Ok(())
    }

    /// Cascading wrap for any lines that exceed MAX_COLUMNS (80), prepending
    /// overflowing characters into the next line.
    fn wrap_lines_from(&mut self, start_line: usize) {
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
    fn test_initial_document() {
        let doc = Document::new();
        assert_eq!(doc.revision, 0);
        assert_eq!(doc.text(), "");
        assert_eq!(doc.lines, vec![""]);
    }

    #[test]
    fn test_insert_single_line() {
        let mut doc = Document::new();
        doc.insert(0, 0, "Hello").unwrap();
        assert_eq!(doc.text(), "Hello");
        assert_eq!(doc.revision, 1);

        doc.insert(0, 5, " World").unwrap();
        assert_eq!(doc.text(), "Hello World");
        assert_eq!(doc.revision, 2);

        doc.insert(0, 5, ",").unwrap();
        assert_eq!(doc.text(), "Hello, World");
        assert_eq!(doc.revision, 3);
    }

    #[test]
    fn test_insert_free_form_grid_auto_pads_lines_and_columns() {
        let mut doc = Document::new();
        // Insert at line 2, col 4 in an empty doc
        doc.insert(2, 4, "Deep").unwrap();
        assert_eq!(doc.lines.len(), 3);
        assert_eq!(doc.lines[0], "");
        assert_eq!(doc.lines[1], "");
        assert_eq!(doc.lines[2], "    Deep");
        assert_eq!(doc.text(), "\n\n    Deep");
    }

    #[test]
    fn test_max_columns_enforced() {
        let mut doc = Document::new();
        let long_string = "a".repeat(MAX_COLUMNS);
        doc.insert(0, 0, &long_string).unwrap();
        assert_eq!(doc.lines[0].chars().count(), MAX_COLUMNS);

        // Exceeding MAX_COLUMNS by inserting at MAX_COLUMNS wraps to line 1
        doc.insert(0, MAX_COLUMNS, "b").unwrap();
        assert_eq!(doc.lines.len(), 2);
        assert_eq!(doc.lines[0], "a".repeat(MAX_COLUMNS));
        assert_eq!(doc.lines[1], "b");

        // Column > MAX_COLUMNS still returns bounds error
        assert_eq!(
            doc.insert(0, MAX_COLUMNS + 1, "b"),
            Err("Column exceeds maximum line width")
        );
    }

    #[test]
    fn test_column_wrapping_cascade() {
        let mut doc = Document::new();
        // Insert 80 'a's on line 0, 80 'b's on line 1
        doc.insert(0, 0, &"a".repeat(MAX_COLUMNS)).unwrap();
        doc.insert(1, 0, &"b".repeat(MAX_COLUMNS)).unwrap();
        assert_eq!(doc.lines.len(), 2);

        // Insert "XYZ" at col 40 of line 0
        // Line 0 becomes 80 chars: 40 'a's + "XYZ" + 37 'a's
        // 3 'a's overflow to line 1
        // Line 1 becomes 80 chars: 3 'a's + 77 'b's
        // 3 'b's overflow to line 2
        // Line 2 becomes "bbb"
        doc.insert(0, 40, "XYZ").unwrap();
        assert_eq!(doc.lines.len(), 3);
        assert_eq!(doc.lines[0].chars().count(), MAX_COLUMNS);
        assert_eq!(doc.lines[1].chars().count(), MAX_COLUMNS);
        assert_eq!(doc.lines[2], "bbb");
    }

    #[test]
    fn test_column_wrapping_utf8() {
        let mut doc = Document::new();
        // 79 Finnish characters + 1 emoji = 80 chars
        let content = format!("{}{}", "ä".repeat(79), "🦀");
        doc.insert(0, 0, &content).unwrap();
        assert_eq!(doc.lines[0].chars().count(), 80);

        // Inserting "ö" should wrap the emoji "🦀" to the next line
        doc.insert(0, 79, "ö").unwrap();
        assert_eq!(doc.lines.len(), 2);
        assert_eq!(doc.lines[0].chars().count(), 80);
        assert_eq!(doc.lines[1], "🦀");
    }

    #[test]
    fn test_insert_multi_line() {
        let mut doc = Document::new();
        doc.insert(0, 0, "Hello\nWorld").unwrap();
        assert_eq!(doc.lines, vec!["Hello", "World"]);
        assert_eq!(doc.text(), "Hello\nWorld");

        doc.insert(0, 5, " Beautiful\nWonderful").unwrap();
        assert_eq!(doc.lines, vec!["Hello Beautiful", "Wonderful", "World"]);
        assert_eq!(doc.text(), "Hello Beautiful\nWonderful\nWorld");
    }

    #[test]
    fn test_delete_within_line() {
        let mut doc = Document::new();
        doc.insert(0, 0, "Hello, World!").unwrap();
        doc.delete(0, 5, 2).unwrap(); // delete ", "
        assert_eq!(doc.text(), "HelloWorld!");
    }

    #[test]
    fn test_delete_across_lines() {
        let mut doc = Document::new();
        doc.insert(0, 0, "Hello\nWorld").unwrap();
        // Deleting from col 3 of "Hello" ("lo\nW" = 4 chars)
        doc.delete(0, 3, 4).unwrap();
        assert_eq!(doc.text(), "Helorld");
        assert_eq!(doc.lines, vec!["Helorld"]);
    }

    #[test]
    fn test_bounds_errors() {
        let mut doc = Document::new();
        assert_eq!(doc.delete(1, 0, 1), Err("Line out of bounds"));
        assert_eq!(doc.delete(0, 1, 1), Err("Column out of bounds"));
        assert_eq!(
            doc.delete(0, 0, 1),
            Err("Delete count exceeds document length")
        );
        assert_eq!(
            doc.delete(0, MAX_COLUMNS + 1, 1),
            Err("Column exceeds maximum line width")
        );
    }

    #[test]
    fn test_utf8_handling() {
        let mut doc = Document::new();
        doc.insert(0, 0, "päivää 🦀").unwrap();
        assert_eq!(doc.lines[0].chars().count(), 8);

        // Delete " 🦀" (2 chars)
        doc.delete(0, 6, 2).unwrap();
        assert_eq!(doc.text(), "päivää");
    }
}
