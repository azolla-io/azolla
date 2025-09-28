# syntax=docker/dockerfile:1.6

ARG RUST_VERSION=1.82
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-slim AS builder

ENV CARGO_TERM_COLOR=never
WORKDIR /app

# Install build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        ca-certificates \
        clang \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy source and build in release mode
COPY . .
RUN cargo build --locked --release --bin azolla-orchestrator --bin azolla-shepherd --bin azolla-worker --example benchmark_client_example --example benchmark_worker_example
RUN strip target/release/azolla-orchestrator target/release/azolla-shepherd target/release/azolla-worker target/release/examples/benchmark_client_example

FROM debian:bookworm-slim AS runtime-base
ARG USER=azolla
ARG UID=10001
ARG GID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid ${GID} ${USER} \
    && useradd --system --home /app --no-create-home --uid ${UID} --gid ${GID} ${USER}

WORKDIR /app

ENV AZOLLA_CONFIG_DIR=/app/config
RUN mkdir -p ${AZOLLA_CONFIG_DIR}

FROM runtime-base AS orchestrator
ARG APP_VERSION=dev
ARG GIT_SHA=unknown
ARG REPOSITORY=unknown/azolla
ARG USER=azolla

COPY --from=builder /app/target/release/azolla-orchestrator /usr/local/bin/azolla-orchestrator
COPY config/orchestrator.toml ${AZOLLA_CONFIG_DIR}/orchestrator.toml
COPY migrations ./migrations

RUN chown -R ${USER}:${USER} /app \
    && chown ${USER}:${USER} /usr/local/bin/azolla-orchestrator

USER ${USER}
ENV AZOLLA__DATABASE__URL=postgres://postgres:postgres@postgres:5432/azolla
EXPOSE 52710

LABEL org.opencontainers.image.title="Azolla Orchestrator"
LABEL org.opencontainers.image.version=${APP_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY}"

ENTRYPOINT ["/usr/local/bin/azolla-orchestrator"]

FROM runtime-base AS shepherd
ARG APP_VERSION=dev
ARG GIT_SHA=unknown
ARG REPOSITORY=unknown/azolla
ARG USER=azolla

COPY --from=builder /app/target/release/azolla-shepherd /usr/local/bin/azolla-shepherd
COPY --from=builder /app/target/release/azolla-worker /usr/local/bin/azolla-worker
COPY config/shepherd.toml ${AZOLLA_CONFIG_DIR}/shepherd.toml

RUN chown -R ${USER}:${USER} /app \
    && chown ${USER}:${USER} /usr/local/bin/azolla-shepherd /usr/local/bin/azolla-worker

USER ${USER}
ENV AZOLLA_ORCHESTRATOR_ENDPOINT=http://azolla-orchestrator:52710
ENV AZOLLA_WORKER_BINARY=/usr/local/bin/azolla-worker
EXPOSE 50052

LABEL org.opencontainers.image.title="Azolla Shepherd"
LABEL org.opencontainers.image.version=${APP_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY}"

ENTRYPOINT ["/usr/local/bin/azolla-shepherd"]

FROM runtime-base AS benchmark-client
ARG APP_VERSION=dev
ARG GIT_SHA=unknown
ARG REPOSITORY=unknown/azolla
ARG USER=azolla

COPY --from=builder /app/target/release/examples/benchmark_client_example /usr/local/bin/azolla-benchmark-client

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ARCH=$(dpkg --print-architecture) \
    && KUBECTL_VERSION=$(curl -fsSL https://dl.k8s.io/release/stable.txt) \
    && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl

RUN chown ${USER}:${USER} /usr/local/bin/azolla-benchmark-client

USER ${USER}

LABEL org.opencontainers.image.title="Azolla Benchmark Client"
LABEL org.opencontainers.image.version=${APP_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY}"

ENTRYPOINT ["/usr/local/bin/azolla-benchmark-client"]
