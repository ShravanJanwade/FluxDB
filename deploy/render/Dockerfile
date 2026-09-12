FROM rust:1.96-bookworm AS rust-build
WORKDIR /build
COPY fluxdb ./fluxdb
COPY docs ./docs
WORKDIR /build/fluxdb
RUN cargo build --release --locked -p fluxdb-server

FROM node:22-bookworm-slim AS web-build
WORKDIR /app
COPY fluxdb-studio/package*.json ./
RUN npm ci
COPY fluxdb-studio ./
ENV VITE_RENDER_DEMO=true
RUN npm run build

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends nginx python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home fluxdb \
    && mkdir -p /app/data && chown fluxdb:fluxdb /app/data
WORKDIR /app
COPY --from=rust-build /build/fluxdb/target/release/fluxdb /usr/local/bin/fluxdb
COPY --from=web-build /app/dist /app/web
COPY sdk/python /app/sdk/python
COPY scripts/demo.py /app/scripts/demo.py
COPY deploy/render /app/deploy/render
USER fluxdb
ENV PYTHONUNBUFFERED=1
EXPOSE 10000
CMD ["python3", "/app/deploy/render/start.py"]
