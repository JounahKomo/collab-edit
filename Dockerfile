FROM rust:1.96-bookworm AS builder

WORKDIR /usr/src/pronets-app

COPY ./ ./

RUN cargo build -p server --release

FROM debian:bookworm-slim AS runtime

WORKDIR /usr/local/bin

COPY --from=builder /usr/src/pronets-app/target/release/server ./server

EXPOSE 5011/tcp 5011/udp

CMD ["./server", "--port", "5011"]
