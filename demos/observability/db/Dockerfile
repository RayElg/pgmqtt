# Extends the base pgmqtt image with TimescaleDB for the observability demo.
# The root docker/Dockerfile stays lightweight — TimescaleDB is optional.
ARG BASE_IMAGE=pgmqtt-base
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y gnupg curl lsb-release \
    && curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey \
       | gpg --dearmor -o /usr/share/keyrings/timescaledb.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/timescaledb.gpg] \
       https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" \
       > /etc/apt/sources.list.d/timescaledb.list \
    && apt-get update \
    && apt-get install -y timescaledb-2-postgresql-16 \
    && rm -rf /var/lib/apt/lists/*
