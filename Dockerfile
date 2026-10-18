FROM rust:1.99-slim-trixie AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/cmr-api/Cargo.toml crates/cmr-api/Cargo.toml
COPY crates/cmr-api/src/ crates/cmr-api/src/
COPY crates/cmr-s3/Cargo.toml crates/cmr-s3/Cargo.toml
COPY crates/cmr-s3/src/ crates/cmr-s3/src/
RUN cargo build --locked --release -p cmr-s3

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/cmr-s3 /var/lib/cmr-s3 \
    && chown 10001:10001 /var/lib/cmr-s3
COPY --from=build /build/target/release/cmr-s3 /usr/local/bin/cmr-s3
ENV CMR_S3_CONFIG=/etc/cmr-s3/buckets.toml \
    CMR_S3_KEY_FILE=/etc/cmr-s3/master.key \
    CMR_S3_STATE_DIR=/var/lib/cmr-s3 \
    CMR_S3_BIND=0.0.0.0:9000
USER 10001:10001
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/cmr-s3"]
CMD ["serve"]
