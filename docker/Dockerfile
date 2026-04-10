# Development Dockerfile — builds and runs pgmqtt in a single image.
# For the shipping pipeline, see Dockerfile.build + Dockerfile.install-test.
#
# Builder stage
FROM rust:bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    lsb-release \
    wget \
    gnupg \
    && sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y \
    clang \
    gcc \
    libclang-dev \
    postgresql-server-dev-16 \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-pgrx
# Using the specific version to match Cargo.toml
RUN cargo install --locked cargo-pgrx --version 0.16.1

# Initialize pgrx for Postgres 16 using the system installation
# This avoids compiling Postgres from source
RUN cargo pgrx init --pg16 /usr/lib/postgresql/16/bin/pg_config

# Set up the work directory
WORKDIR /usr/src/extension/extension

# Copy the complete extension source
COPY extension/Cargo.toml ./
COPY extension/src ./src
COPY extension/pgmqtt.control ./

# Build the extension package
RUN cargo pgrx package && \
    mkdir -p /app/dist && \
    cp -r target/release/pgmqtt-pg16/* /app/dist/

# Runtime stage
FROM postgres:16-bookworm

# Install TimescaleDB
RUN apt-get update && apt-get install -y gnupg curl lsb-release \
    && curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey \
       | gpg --dearmor -o /usr/share/keyrings/timescaledb.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/timescaledb.gpg] \
       https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" \
       > /etc/apt/sources.list.d/timescaledb.list \
    && apt-get update \
    && apt-get install -y timescaledb-2-postgresql-16 \
    && rm -rf /var/lib/apt/lists/*

# Copy artifacts from builder
COPY --from=builder /app/dist/usr/share/postgresql/16/extension /usr/share/postgresql/16/extension
COPY --from=builder /app/dist/usr/lib/postgresql/16/lib /usr/lib/postgresql/16/lib

# Configure PostgreSQL for logical decoding.
# timescaledb must appear before pgmqtt in shared_preload_libraries.
RUN echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_replication_slots = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "shared_preload_libraries = 'timescaledb,pgmqtt'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "log_min_messages = warning" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "local   all             all                                     trust" >> /usr/share/postgresql/pg_hba.conf.sample && \
    echo "host    all             all             all                     trust" >> /usr/share/postgresql/pg_hba.conf.sample

# Auto-create extensions on first startup (timescaledb before pgmqtt)
RUN echo '#!/bin/bash' > /docker-entrypoint-initdb.d/01-create-extension.sh && \
    echo 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"' >> /docker-entrypoint-initdb.d/01-create-extension.sh && \
    echo 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS pgmqtt;"' >> /docker-entrypoint-initdb.d/01-create-extension.sh && \
    chmod +x /docker-entrypoint-initdb.d/01-create-extension.sh


# Expose ports: 5432 PostgreSQL | 8080 HTTP healthcheck | 1883 MQTT TCP | 9001 MQTT WebSocket
EXPOSE 5432 8080 1883 9001
