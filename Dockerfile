FROM rust:1.88-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release

FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    rsync \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/git-sync-reloader-adapter /app/git-sync-reloader-adapter

ENTRYPOINT ["/app/git-sync-reloader-adapter"]
