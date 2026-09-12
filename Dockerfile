FROM rust:1.96-bookworm AS build
WORKDIR /build
COPY fluxdb ./fluxdb
COPY docs ./docs
WORKDIR /build/fluxdb
RUN cargo build --locked --release -p fluxdb-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home fluxdb && mkdir /data && chown fluxdb:fluxdb /data
COPY --from=build /build/fluxdb/target/release/fluxdb /usr/local/bin/fluxdb
USER fluxdb
WORKDIR /data
ENV FLUXDB_ADDR=0.0.0.0:8086 FLUXDB_DATA_DIR=/data
EXPOSE 8086
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s CMD curl --fail http://127.0.0.1:8086/health || exit 1
ENTRYPOINT ["fluxdb"]
