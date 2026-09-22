# syntax=docker/dockerfile:1
#
# IMPORTANT: this image's build context is the *parent* directory of this
# repo (the meta repo root), not the backend repo itself. That's because
# AnkiCollab-Backend/Cargo.toml declares:
#
#     htmldiff = { path = "../website/htmldiff" }
#
# ...i.e. it expects a sibling folder literally named `website/` containing
# the htmldiff crate. docker-compose.yml in this meta repo sets
#   build.context: .
#   build.dockerfile: backend/Dockerfile
# so that both `backend/` and `website/` are visible to this build.

########################
# 1) Build stage
########################
FROM rust:1.98-bookworm AS builder

# Build deps some of the crates in Cargo.toml need at compile time
# (openssl-sys/aws-lc-sys type crates pull these in transitively).
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    perl \
    make \
    clang \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only pull in the one sub-crate the backend actually needs from the
# website repo, not the whole website source tree.
COPY website/htmldiff ./website/htmldiff
COPY backend ./backend

# Keep build artifacts out of /app/backend so we don't accidentally drag
# a multi-GB target/ dir into later COPY --from steps.
ENV CARGO_TARGET_DIR=/build

WORKDIR /app/backend
RUN cargo build --release

########################
# 2) Runtime stage
########################
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app

WORKDIR /app

COPY --from=builder /build/release/anki-backend ./anki-backend
RUN chown app:app ./anki-backend

USER app
EXPOSE 5555
ENV RUST_LOG=info

CMD ["./anki-backend"]
