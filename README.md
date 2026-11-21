# Collaborative Document Editor (CollabEdit)

## 1. Scope of the Project and Implemented Features

The project is a real-time collaborative document editor and multi-channel chat system written in Rust. It enables multiple concurrent users to connect over an encrypted network connection (TLS) to collaboratively view, edit, and interact on a shared text document simultaneously, while also seeing real-time remote user cursors and participating in channel-based chat. The core goal is to provide a responsive, synchronized multi-user editing experience with minimal latency and strict consistency.

### Key Implemented Features
- **Free-form Document Grid & Page Layout:** Document lines are constrained to standard 80-column margins with automatic line padding and line expansion. Text wraps downwards across subsequent lines when reaching the 80-column boundary. The client chunks lines into standard pages (`LINES_PER_PAGE = 35`) with page numbering and dropshadow styling.
- **Optimistic Concurrent Editing:** Interactive typing, newline splitting, and backspace deletions with immediate local UI feedback. Local edits track pending request IDs; stale server document snapshots arriving while edits are in flight are safely ignored to prevent typing flicker or cursor resets.
- **Real-Time Remote Cursors over UDP (`komon-3`):** Ephemeral, low-latency cursor tracking showing peer positions with coloured bars and username tags. Clients announce a unique registration token over TCP and punch NAT over UDP; the server relays cursor positions via UDP datagrams and periodically broadcasts full-state cursor snapshots.
- **Multi-Channel Chat (`komon-2`):** Sidebar displaying channel lists (starting with `#general`), dynamic on-the-fly channel creation (`+` button), channel switching, and channel-filtered message history.
- **Full TLS Encryption & Plain TCP Fallback:** Encrypted communication using `rustls` with custom Root CA verification or plain TCP fallback for local development and legacy testing.
- **Server Robustness & Hostile Input Protection:** Frame size limits (64 KiB), document line limits (5,000 lines), connection drop guards that guarantee username cleanup even on panic, idle registration timeouts, and per-client outgoing queue backpressure.

---

## 2. Program Structure

The repository is organized as a Cargo workspace containing three crates:

```text
├── Cargo.toml                  # Workspace definition
├── Dockerfile                  # Containerized server build
├── certs/                      # Local testing certificates
├── client/                     # Slint GUI client & testclient binary
│   ├── build.rs                # Compiles Slint UI definitions
│   ├── ui/
│   │   ├── app-window.slint    # Slint markup (3-column layout, pages, chat, cursors)
│   │   └── fonts/              # Bundled true monospace font (DejaVu Sans Mono)
│   └── src/
│       ├── main.rs             # CLI parsing, setup, worker spawning, and UI run loop
│       ├── doc_state.rs        # Local document buffer, cursor math, line wrapping
│       ├── chat_state.rs       # Channels, active channel selection, message storage
│       ├── cursors.rs          # UDP cursor receiver, expiry tracking, and UI sync
│       ├── net.rs              # Unified stream abstraction (plain TCP / rustls TLS)
│       ├── worker.rs           # Background TCP reader/writer thread and event dispatch
│       ├── context.rs          # Thread-safe client context and request ID generation
│       ├── callbacks.rs        # Slint UI event callbacks (typing, clicks, channels, chat)
│       ├── ui_sync.rs          # Translates state to Slint ModelRc data structures
│       └── bin/
│           └── testclient.rs   # Multi-threaded stress and automated compliance tester
├── server/                     # Asynchronous Tokio server
│   └── src/
│       ├── main.rs             # Server CLI args, TLS/UDP setup, and listener loops
│       ├── connection.rs       # TCP accept loop and per-connection I/O task
│       ├── client.rs           # Client session framing and message processing
│       ├── handlers.rs         # Request handlers for TCP messages (USR, INS, DEL, etc.)
│       ├── protocol.rs         # Binary frame decoding and serialization (TCP & UDP)
│       ├── state.rs            # Thread-safe server state, metrics, and broadcast engine
│       ├── document.rs         # Server document model, bounds checks, cascading wrapping
│       ├── udp.rs              # UDP NAT-punching hello, cursor relay, and periodic snapshot
│       └── tls.rs              # TLS certificate and private key loader
│   └── tests/
│       └── integration.rs      # In-process socket integration and fuzzing test suite
└── http-client/                # Standalone lightweight HTTP/HTTPS benchmark client
    └── src/
        └── main.rs             # HTTP/1.1 request implementation using rustls
```

---

## 3. Libraries and Crates Used

The project relies on a curated set of Rust crates chosen for performance, safety, and asynchronous networking:

### Server Crates
- **`tokio` (`features = ["rt-multi-thread", "macros", "net", "io-util", "sync", "time"]`):** Core asynchronous runtime powering the server. Handles thousands of concurrent TCP connections and background timer loops without blocking OS threads.
- **`tokio-rustls` & `rustls-pki-types` & `rustls-pemfile`:** Modern, memory-safe TLS implementation for encrypted TCP transport without native OpenSSL dependencies.
- **`clap` (`features = ["derive", "env"]`):** Command-line argument parsing with environment variable support.
- **`serde` & `serde_json`:** Serialization of server metrics into JSON for the `MET` protocol command.
- **`tracing` & `tracing-subscriber`:** Structured, asynchronous diagnostic logging with per-client spans and configurable log levels (`RUST_LOG`).

### Client Crates
- **`slint` & `slint-build`:** Declarative desktop GUI toolkit compiling native UI components with reactive property bindings, hardware acceleration, and cross-platform support.
- **`rustls`, `rustls-pki-types`, `rustls-pemfile`, `webpki-roots`:** Client-side TLS client configuration, supporting custom CA certificates or Mozilla's root certificate bundle.
- **`clap`:** CLI parsing for target address, credentials, TLS switches, and protocol selection.

---

## 4. Concurrency Architecture

### Server-Side Concurrency
- **Multi-Threaded Asynchronous Runtime:** Built on Tokio's work-stealing multi-threaded scheduler (`rt-multi-thread`).
- **Independent Connection Tasks:** Each connected TCP client is spawned into an isolated `tokio::spawn` task. A slow or misbehaving client cannot delay other clients.
- **Connection Drop Guard:** Client registrations and occupied usernames are wrapped in a drop guard; even if a connection aborts abruptly or panics, resources and usernames are released immediately.
- **Shared State Locking:** Shared document, client registry, channel lists, and metrics are protected by an asynchronous `Arc<Mutex<State>>`. Critical sections are kept minimal (pure memory mutations) to avoid lock contention.
- **Non-blocking Broadcasting & Backpressure:** Outgoing frames to clients are dispatched across unbounded MPSC channels capped with bounded queues (256 frames). Slow clients have excess frames dropped rather than stalling the entire broadcast loop.
- **Dedicated UDP Task:** A standalone Tokio task processes incoming UDP datagrams (registration hellos and `CUR` messages) and forwards them asynchronously.

### Client-Side Concurrency
- **Decoupled GUI & Network Worker:** Slint requires exclusive ownership of the main OS thread. Network I/O runs in a dedicated background worker thread (`worker.rs`), keeping the UI responsive even during heavy network latency.
- **Thread Synchronization:** Network updates communicate with Slint via thread-safe closures scheduled onto the GUI event loop using `slint::invoke_from_event_loop`.
- **Optimistic Concurrency & Request De-duplication:** Typing immediately mutates local `ClientDocState` and queues network `INS`/`DEL` frames. To prevent remote snapshots from reverting unacknowledged local characters, server snapshots are ignored while local mutation requests are in flight.
- **Asynchronous UDP Cursor Loop:** A dedicated thread listens for incoming UDP datagrams, debounces cursor updates, and periodically prunes cursors inactive for over 5 seconds.

---

## 5. Protocol Specification

Communication follows a binary-framed protocol:
`[4 bytes length (BE)] [4 bytes message_id (BE)] [4 bytes type] [payload]`

The active protocol version is **`komon-3`** (backwards compatible with `komon-2`, `komon-1`, and `base-protocol`).

### Protocol Messages

* **`TST ` (Echo / Ping - TCP):**
  * *Client -> Server:* `TST <payload>`
  * *Response:* Echoes the exact same payload and message ID for connection and latency testing.
* **`USR ` (User Registration - TCP):**
  * *Client -> Server:* `USR <username>`
  * *Response:* Echoes on success, or returns `ERR` if the username is already registered. Associates the user identity with the connection.
* **`GET ` (Get Document State - TCP):**
  * *Client -> Server:* `GET ` (empty payload).
  * *Response:* `GET <revision>\n<document_content>` with the client's request ID.
  * *Broadcast:* When a mutation occurs, the server broadcasts `GET <revision>\n<document_content>` to all registered clients using server-allocated IDs.
* **`INS ` (Insert Text - TCP):**
  * *Client -> Server:* `INS <line> <col> <text>`
  * *Response:* Echoes the `INS` frame back on success (triggering a document `GET` broadcast), or returns `ERR` if column bounds (`> 80`) are exceeded. Overflowing characters cascade into subsequent lines.
* **`DEL ` (Delete Text - TCP):**
  * *Client -> Server:* `DEL <line> <col> <count>`
  * *Response:* Echoes the `DEL` frame back on success (triggering a document `GET` broadcast), or returns `ERR` if coordinates are out of bounds.
* **`UDP ` (UDP Endpoint Registration - TCP & UDP, `komon-3`):**
  * *Client -> Server (TCP):* `UDP <token>`, announces a secret random token for the connection.
  * *Response (TCP):* Echoes on success, or `ERR` if unregistered.
  * *Client -> Server (UDP):* Datagram `UDP <token>`. The server learns the real public UDP endpoint (traversing NAT) and replies with an empty `UDP ` datagram acknowledgement.
* **`CUR ` (Interactive Cursor - UDP, `komon-3`):**
  * *Client -> Server (UDP):* `CUR <line> <col>` sent from the registered UDP endpoint.
  * *Broadcast (UDP):* `CUR <username> <line> <col>` forwarded to all other peers. The server also sends a periodic full cursor snapshot once per second. Cursors not refreshed within 5 seconds are expired.
* **`MSG ` (Multi-Channel Chat - TCP, `komon-2`):**
  * *Client -> Server:* `MSG <channel> <message>`
  * *Behavior:* If `<channel>` does not exist, the server dynamically creates it.
  * *Broadcast:* `MSG <sender> <channel> <message>` broadcast to all clients in that channel.
* **`MET ` (Metrics - TCP):**
  * *Client -> Server:* `MET `
  * *Response:* `MET <json>` with total connections, messages processed, and error counters.
* **`BYE ` (Disconnect - TCP):**
  * *Client -> Server:* `BYE `
  * *Behavior:* Gracefully terminates the session and closes the socket.
* **`ERR ` (Error Response - TCP):**
  * *Server -> Client:* `ERR <reason>` echoing the triggering message ID.

---

## 6. Testing Practices

The project follows a multi-tiered testing strategy:

1. **Unit Tests (`cargo test`):**
   - **Document Model (`server/src/document.rs`):** Validates insertion, deletion, column padding, cascading multi-line wrapping, UTF-8/emoji safety, and bounds error checking.
   - **Client Document State (`client/src/doc_state.rs`):** Tests cursor movement, backspace line merging, optimistic edits, and server synchronization.
   - **Protocol Encoding (`server/src/protocol.rs`):** Tests serialization and deserialization of binary frames and edge cases in parsing.
2. **Integration Tests (`server/tests/integration.rs`):**
   - In-process integration tests spinning up real TCP/UDP listeners on random loopback ports.
   - Tests complete client-server interactions: registration handshake, dynamic channel creation, broadcast propagation, hostile oversized frames (> 64 KiB), invalid UTF-8 payloads, unauthenticated request rejection, and concurrent socket fuzzing.
3. **Automated Headless Test Client (`client/src/bin/testclient.rs`):**
   - Multi-threaded stress testing tool opening concurrent connections (minimum 5) to verify protocol compliance, synchronization barriers, broadcast latency, and UDP cursor exchange against local or remote servers.

---

## 7. Development Tools and Practices

- **Programming Environment:** Developed on Linux (x86_64) using the Rust 2024 edition toolchain, Cargo, Clippy, and Rustfmt.
- **UI Prototyping & Bundling:** Slint UI designed with `slint-build`, bundling the TrueType font `DejaVuSansMono.ttf` directly into the binary to ensure cross-platform monospace alignment.
- **Team & Division of Work:** The project was developed individually by Joona Komonen.
- **AI Tool Usage:** Antigravity and Claude Code were used as AI assistants throughout the development cycle. Specifically:
  - Architecture and protocol design discussions (e.g., NAT traversal strategy for UDP cursors).
  - Rapid prototyping of Slint UI layouts and responsive document styling.
  - Writing automated regression tests, edge-case test suites, and protocol fuzzing tests.
  - Assisting in debugging race conditions, buffer handling, and column wrapping mathematics.

---

## 8. Instructions for Running and Testing

### Prerequisites
- Rust 1.85+ toolchain (`cargo`, `rustc`).
- (Optional) OpenSSL CLI for generating custom TLS test certificates.

### 1. Running Locally with Plain TCP (Quickest)

```bash
# Terminal 1: Start Server
cargo run -p server -- --no-tls

# Terminal 2: Start GUI Client (Alice)
cargo run -p client -- --no-tls -u Alice

# Terminal 3: Start Second GUI Client (Bob)
cargo run -p client -- --no-tls -u Bob
```

### 2. Running Locally with TLS Encryption

```bash
# Generate local CA and server certificates (if not already present in certs/)
mkdir -p certs
openssl req -x509 -newkey rsa:2048 -nodes -keyout certs/localCA.key -out certs/localCA.crt -days 365 -subj "/CN=Local CA"
openssl req -newkey rsa:2048 -nodes -keyout certs/server.key -out certs/server.csr -subj "/CN=localhost"
openssl x509 -req -in certs/server.csr -CA certs/localCA.crt -CAkey certs/localCA.key -CAcreateserial -out certs/server.crt -days 365 -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Terminal 1: Start TLS Server
cargo run -p server -- --cert certs/server.crt --key certs/server.key

# Terminal 2: Start Client with TLS verification
cargo run -p client -- 127.0.0.1:5011 --ca-cert certs/localCA.crt -u Alice
```

### 3. Running Automated Tests

```bash
# Run all unit and integration tests across the workspace
cargo test --workspace

# Run linter checks
cargo clippy --workspace --all-targets

# Run the 5-connection automated test client against the local server
cargo run -p client --bin testclient -- --port 5011 --no-tls --protocol komon-3 -c 5
```

---

## 9. Lessons Learned and Known Shortcomings

### Lessons Learned
The most impactful lessons learned throughout the project revolved around fundamental network technologies and protocols:
- **Transport Protocols (TCP vs. UDP):** Gaining practical experience choosing the appropriate transport protocol for different needs—using reliable, stream-oriented TCP for document mutations and channel chat messages, while leveraging lightweight, unreliable UDP datagrams for high-frequency, low-latency remote cursor position updates and NAT traversal.
- **Transport Layer Security (TLS):** Configuring and managing TLS in Rust (`rustls`), handling certificate verification and custom root stores, and understanding the handshake and encryption pipeline in client-server network architectures.
- **Asynchronous Networking:** Managing state and events concurrently using Tokio on the backend and synchronizing with an active UI event loop on the frontend.

### Known Shortcomings
- **Performance & Scalability of Document Synchronization:** The current architecture broadcasts the entire document text snapshot (`GET <revision>\n<content>`) on mutations rather than differential patches, operational transforms (OT), or conflict-free replicated data types (CRDTs). While simple and reliable for documents within standard limits, this introduces transmission overhead as document sizes grow.
- **GUI Quality and Visual Polish:** The visual appearance and layout of the desktop client, while functional and responsive across three panels, remains basic.

### What Could Be Done Differently
If given another iteration or more time, primary effort would be directed toward refining the GUI: improving visual aesthetics, styling, and typography, as well as refining UI performance and transitions to make the editing environment feel even more fluid and polished.

---

## 10. Subjective Evaluation

Overall, the project succeeded well in meeting its core objectives and design criteria. The system delivers a fully functional collaborative document editing workflow with concurrent users, channel-based chat, and live cursor indicators across network boundaries.

The most satisfying aspect of the project is the implementation of the communication protocol: the custom binary framed protocol (`komon-3`) functions exactly as intended, gracefully handling edge cases and hostile inputs on the server, while interfacing smoothly with the asynchronous background worker and reactive graphical interface on the client.
