# Build stage
FROM rust:1.93-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig ca-certificates
WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime stage: bare scratch image (binary is static with musl + rustls)
FROM scratch

COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /app/target/release/numaflow-mqtt-source /bin/numaflow-mqtt-source

ENTRYPOINT ["/bin/numaflow-mqtt-source"]
