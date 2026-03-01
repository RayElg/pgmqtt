# pgmqtt: PostgreSQL CDC-to-MQTT Broker

`pgmqtt` is a Postgres extension built with `pgrx` that introduces an embedded MQTT 5.0 broker powered directly by Change Data Capture (CDC).

With `pgmqtt`, your database changes (`INSERT`, `UPDATE`, `DELETE`) are automatically transformed into MQTT messages and published to connected clients, using pure SQL-configured topic mappings.

## Quickstart

To create a mapping, simply run:

```sql
SELECT pgmqtt_add_mapping(
    'public',
    'my_table',
    'topics/{{ op | lower }}',
    '{{ columns | tojson }}'
);
```

Ensure that `wal_level = logical` is set inside your `postgresql.conf` for the logical decoding output plugin to capture CDC events properly.

## Repository Structure

- `extension/`: Rust source code for the PostgreSQL extension (pgrx).
- `docker/`: Dockerfiles and environment setups for development and testing.
- `docs/`: User and developer documentation.
- `scripts/`: Utility scripts for building and packaging.
  - `scripts/build_package.sh`: Script to build distributable tarballs.
- `tests/`: Test suites.
  - `tests/conformance/`: MQTT conformance tests against reference brokers.
  - `tests/integration/`: Integration tests for CDC, MQTT, and WebSocket flows.

## Development

To start the development environment:

```bash
docker compose up -d --build
```

To run integration tests:

```bash
# Run python tests
python -m pytest tests/integration/

# Run WebSocket test (requires Node.js)
python tests/integration/test_websocket_runner.py
```

## Building Distributables

To build the extension for shipping:

```bash
bash scripts/build_package.sh
```
