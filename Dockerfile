# Multi-stage build for minimal runtime image
FROM rust:1.82-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY muninn-core/Cargo.toml muninn-core/
COPY muninn-storage/Cargo.toml muninn-storage/
COPY muninn-embedding/Cargo.toml muninn-embedding/
COPY muninn-consolidator/Cargo.toml muninn-consolidator/
COPY muninn-api/Cargo.toml muninn-api/
COPY muninn-server/Cargo.toml muninn-server/

# Build dependencies (cached layer)
RUN mkdir -p muninn-core/src muninn-storage/src muninn-embedding/src muninn-consolidator/src muninn-api/src muninn-server/src && \
    echo "pub fn placeholder() {}" > muninn-core/src/lib.rs && \
    echo "pub fn placeholder() {}" > muninn-storage/src/lib.rs && \
    echo "pub fn placeholder() {}" > muninn-embedding/src/lib.rs && \
    echo "pub fn placeholder() {}" > muninn-consolidator/src/lib.rs && \
    echo "pub fn placeholder() {}" > muninn-api/src/lib.rs && \
    echo "fn main() {}" > muninn-server/src/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf muninn-core/src muninn-storage/src muninn-embedding/src muninn-consolidator/src muninn-api/src muninn-server/src

# Copy source code
COPY . .

# Build release binary
RUN cargo build --release --bin muninn

# Runtime stage
FROM debian:bookworm-slim as runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r muninn && useradd -r -g muninn -d /app -s /sbin/nologin muninn

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/muninn /usr/local/bin/muninn

# Copy default config
COPY muninn.toml /etc/muninn/muninn.toml

# Create data directories
RUN mkdir -p /app/data/wal /app/data/snapshots && \
    chown -R muninn:muninn /app

USER muninn

EXPOSE 3000 50051 9090 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["muninn"]
CMD ["--config", "/etc/muninn/muninn.toml"]
# 1788294677
# 1788294677
# 1788294677
// commit 22 1788294954014840214
// commit 70 1788294954730882975
// commit 190 1788294956586539253
