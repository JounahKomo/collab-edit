# Collaborative Document Editor (CollabEdit)

## Description

The project is a real-time collaborative document editor written in Rust. It enables multiple concurrent users to connect over an encrypted network connection (TLS) to collaboratively view, edit, and interact on a shared text document simultaneously. The core goal is to provide a responsive, synchronized multi-user editing experience with minimal latency and strict consistency.

The editor organizes text as a free-form document grid constrained to a standard page width (fixed at 80 columns, representative of font size 12 on standard margins). Key features include live text insertion and deletion, automatic line expansion, line wrapping at column 80, document revision tracking, full-state synchronization broadcasts on updates, and chat communication across channels. In future iterations, the editor will also display real-time interactive remote cursors showing peer positions.

## Clients

The primary client is a graphical desktop application developed in Rust using the **Slint** GUI toolkit (`slint` crate). The Slint UI provides an initial username login dialog, a status bar with connection state and cursor coordinates, and a responsive three-column desktop workspace:

1. **Channels Sidebar (Left):** Displays the list of available text channels (starting with `#general`), highlights the currently selected active channel, and features a `+` button that reveals a channel creation input field and `Add` button to dynamically create and join new channels on the fly.
2. **Document Editor (Center):** A multi-page document layout (automatically creating new standard-sized pages as line count grows, similar to Microsoft Word or Google Docs), monospace text rendering, a visual cursor indicator for the current user position, mouse-click cursor positioning, and page metadata (`Page X of Y`).
3. **Channel Chat Panel (Right):** Displays the active channel name (`# <channel>`), a scrollable chat history containing messages sent to that specific channel (styled with distinct sender tags and message bubbles), and an input field with a `Send` button allowing users to send messages directly into the active channel.

Under the hood, the client architecture is modularly separated:
- **UI Layer (`ui/app-window.slint`):** Handles visual styling, login prompt dialog, channels sidebar with dynamic channel creation dialog, multi-page document layout with page sheets and dropshadows, channel-filtered chat history and input bar, mouse click coordinate translation, and keyboard event capture (keystrokes, Enter line splits, Backspace deletions, and arrow key navigation).
- **Document State (`doc_state.rs`):** Tracks local document lines, cursor coordinates, and revision numbers, handling optimistic local line-splitting, cascading line wrapping past column 80, line-merging, direct cursor repositioning (`set_cursor_pos`), and server revision state reconciliation.
- **Chat State (`main.rs` - `ClientChatState`):** Tracks registered channels, active channel selection, and incoming chat messages, ensuring channel lists and active channel message views stay synchronized with server broadcasts.
- **Networking Layer (`net.rs`):** Abstracts TLS (`rustls`) and plain TCP stream handling, connection establishment, certificate validation, and binary frame encoding/decoding.
- **Application Controller (`main.rs`):** Chunks linear document lines into page data (`LINES_PER_PAGE = 35`), wires click, keyboard, channel selection, and chat message callbacks, runs the background I/O worker thread, and bridges network events to the Slint UI thread via `slint::invoke_from_event_loop`.

While the graphical client is focused on direct user interaction, the architecture also supports headless clients, such as our multi-threaded test client (`testclient`) for automated regression testing and benchmarking, and potential future headless bot clients (e.g., automated document spell-checkers or document export tools).

## Server Design

*(A comprehensive server design description will be added in a subsequent iteration following further architectural discussions on concurrency, state persistence, and conflict resolution options.)*

## Protocol Specification

Communication between clients and the server follows a binary-framed protocol over TCP/TLS:
`[4 bytes length (BE)] [4 bytes message_id (BE)] [4 bytes type] [payload]`

The current protocol version is **`komon-2`** and it is backwards compatible with `komon-1` and the `base-protocol`. The change introduced in `komon-2` is full support for multiple channels, dynamically creating channels whenever an `MSG` is sent to an unrecognized channel name.

### Current and Planned Messages

* **`TST ` (Echo / Ping):**
  * *Client -> Server:* `TST <payload>`
  * *Response:* Echoes the exact same message and ID for connection testing.
* **`USR ` (User Registration):**
  * *Client -> Server:* `USR <username>`
  * *Response:* Echoes on success, or `ERR` if the username is already taken. Associates the user identity with the connection.
* **`GET ` (Get Document State):**
  * *Client -> Server:* `GET ` (empty payload).
  * *Response:* `GET <revision>\n<document_content>` with the client's request ID.
  * *Broadcast:* When a document mutation occurs, the server broadcasts `GET <revision>\n<document_content>` to all registered clients using server-allocated message IDs.
* **`INS ` (Insert Text):**
  * *Client -> Server:* `INS <line> <col> <text>`
  * *Response:* Echoes the `INS` frame back on success (and triggers a document `GET` broadcast), or returns `ERR` if line/column bounds (target column `> 80`) are exceeded. If text insertion causes any line to exceed the 80 column limit, overflowing characters automatically cascade and wrap into subsequent lines.
* **`DEL ` (Delete Text):**
  * *Client -> Server:* `DEL <line> <col> <count>`
  * *Response:* Echoes the `DEL` frame back on success (and triggers a document `GET` broadcast), or returns `ERR` if the coordinates or count are invalid.
* **`CUR ` (Interactive Cursor - Planned):**
  * *Client -> Server:* `CUR <line> <col>`
  * *Broadcast:* `CUR <username> <line> <col>` sent to other peers to display live cursor locations on their screen.
* **`MSG ` (Chat / Messaging with Multi-Channel Support):**
  * *Client -> Server:* `MSG <channel> <message>`
  * *Behavior:* If `<channel>` is not yet recognized by the server, the server dynamically creates a channel with that name.
  * *Broadcast:* `MSG <sender> <channel> <message>` broadcast to all clients in the channel (echoed to sender with a newly allocated server ID).
* **`MET ` (Metrics):**
  * *Client -> Server:* `MET `
  * *Response:* `MET <json>` containing server connection count, messages processed, and error counters.
* **`BYE ` (Disconnect / Teardown):**
  * *Client -> Server:* `BYE ` (empty payload).
  * *Behavior:* Notifies the server of a graceful disconnect; the server closes the session and cleans up resources.
* **`ERR ` (Error Response):**
  * *Server -> Client:* `ERR <reason>` carrying the message ID of the triggering request.

## Development & Tooling

Antigravity was used as an AI assistant in the development of the project, assisting with system architecture design, protocol implementation, client/server debugging, and automated test suite creation.
