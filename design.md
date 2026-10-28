# Collaborative Document Editor (CollabEdit)

## Description

The project is a real-time collaborative document editor written in Rust. It enables multiple concurrent users to connect over an encrypted network connection (TLS) to collaboratively view, edit, and interact on a shared text document simultaneously. The core goal is to provide a responsive, synchronized multi-user editing experience with minimal latency and strict consistency.

The editor organizes text as a free-form document grid constrained to a standard page width (fixed at 80 columns, representative of font size 12 on standard margins). Key features include live text insertion and deletion, automatic line expansion, document revision tracking, full-state synchronization broadcasts on updates, and chat communication across channels. In future iterations, the editor will also display real-time interactive remote cursors showing peer positions.

## Clients

The primary client will be a graphical desktop application developed in Rust using the **Slint** GUI toolkit (`slint` crate). The Slint UI will provide an intuitive text-editing canvas, render user text input, and visualize peers' cursors and selections with distinct user colors. Under the hood, the client manages network communication using TLS (`rustls`) to send edits and consume incoming document broadcasts without blocking the UI event loop. While the main client is focused on direct user interaction, the architecture also supports headless clients, such as our multi-threaded test client (`testclient`) for automated regression testing and benchmarking, and potential future headless bot clients (e.g., automated document spell-checkers or document export tools).

## Server Design

*(A comprehensive server design description will be added in a subsequent iteration following further architectural discussions on concurrency, state persistence, and conflict resolution options.)*

## Protocol Specification

Communication between clients and the server follows a binary-framed protocol over TCP/TLS:
`[4 bytes length (BE)] [4 bytes message_id (BE)] [4 bytes type] [payload]`

The current protocol version is komon-1 and it is backwards compatible with the base-protocol.

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
  * *Response:* Echoes the `INS` frame back on success (and triggers a document `GET` broadcast), or returns `ERR` if line/column bounds or line width limits (max 80 columns) are exceeded.
* **`DEL ` (Delete Text):**
  * *Client -> Server:* `DEL <line> <col> <count>`
  * *Response:* Echoes the `DEL` frame back on success (and triggers a document `GET` broadcast), or returns `ERR` if the coordinates or count are invalid.
* **`CUR ` (Interactive Cursor - Planned):**
  * *Client -> Server:* `CUR <line> <col>`
  * *Broadcast:* `CUR <username> <line> <col>` sent to other peers to display live cursor locations on their screen.
* **`MSG ` (Chat / Messaging):**
  * *Client -> Server:* `MSG <channel> <message>`
  * *Broadcast:* `MSG <sender> <channel> <message>` broadcast to all channel participants.
* **`MET ` (Metrics):**
  * *Client -> Server:* `MET `
  * *Response:* `MET <json>` containing server connection count, messages processed, and error counters.
* **`ERR ` (Error Response):**
  * *Server -> Client:* `ERR <reason>` carrying the message ID of the triggering request.
