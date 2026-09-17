# FluxDB in one image: the Rust engine, the control plane, and the console it
# serves. There is no reverse proxy — the server handles static files, the API
# and the single-page fallback on one port, so there is no proxy configuration
# that can accidentally expose an endpoint.

FROM rust:1.96-bookworm AS engine
WORKDIR /build
COPY fluxdb ./fluxdb
COPY docs ./docs
WORKDIR /build/fluxdb
RUN cargo build --locked --release -p fluxdb-server

FROM node:22-bookworm-slim AS console
WORKDIR /app
COPY fluxdb-studio/package.json fluxdb-studio/package-lock.json ./
RUN npm ci
COPY fluxdb-studio ./
RUN npm run build

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home fluxdb \
    && mkdir -p /app/data \
    && chown -R fluxdb:fluxdb /app
COPY --from=engine /build/fluxdb/target/release/fluxdb /usr/local/bin/fluxdb
COPY --from=console --chown=fluxdb:fluxdb /app/dist /app/web
USER fluxdb
WORKDIR /app
ENV FLUXDB_ADDR=0.0.0.0:8086 \
    FLUXDB_DATA_DIR=/app/data \
    FLUXDB_STATIC_DIR=/app/web
EXPOSE 8086
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s \
    CMD curl --fail http://127.0.0.1:8086/health || exit 1
ENTRYPOINT ["fluxdb"]
