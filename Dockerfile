FROM rust:1.96 AS builder

WORKDIR /usr/src/pronets-app

COPY ./ ./

RUN cargo build -p server --release

FROM debian:bookworm-slim AS runtime

WORKDIR /usr/local/bin

COPY --from=builder /usr/src/pronets-app/target/release/server ./server

EXPOSE 5011

CMD ["./server", "--port", "5011"]
