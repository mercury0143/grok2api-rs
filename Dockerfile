# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       cmake \
       ninja-build \
       perl \
       pkg-config \
       clang \
       libclang-dev \
       llvm-dev \
    && rm -rf /var/lib/apt/lists/*

# Build application.
COPY . .
RUN LIBCLANG_PATH="$(llvm-config --libdir)" cargo build --release \
    && test "$(stat -c%s target/release/grok2api-rs)" -gt 5000000

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

COPY --from=builder /src/target/release/grok2api-rs /app/grok2api-rs
COPY config.defaults.toml /app/config.defaults.toml
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/grok2api-rs /app/entrypoint.sh \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser
EXPOSE 8000

ENV SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8000

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/grok2api-rs"]
