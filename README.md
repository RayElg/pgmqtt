# pgmqtt: PostgreSQL CDC-to-MQTT Broker

`pgmqtt` is a Postgres extension built with `pgrx` that introduces an embedded MQTT 5.0 broker powered directly by Change Data Capture (CDC).

With `pgmqtt`, your database changes (`INSERT`, `UPDATE`, `DELETE`) are automatically transformed into MQTT messages and published to connected clients, using pure SQL-configured topic mappings. MQTT clients can also publish messages that are automatically written to PostgreSQL tables via inbound mappings.

## Quickstart

Ensure that `wal_level = logical` is set inside your `postgresql.conf` for the logical decoding output plugin to capture CDC events properly.

### Outbound: PostgreSQL changes → MQTT

Map a table so that every `INSERT`, `UPDATE`, or `DELETE` is published as an MQTT message:

```sql
SELECT pgmqtt_add_outbound_mapping(
    'public',
    'my_table',
    'topics/{{ op | lower }}',
    '{{ columns | tojson }}'
);
```

MQTT clients subscribed to `topics/insert`, `topics/update`, or `topics/delete` will receive the change as a JSON payload.

### Inbound: MQTT → PostgreSQL

Map an MQTT topic pattern so that incoming publishes are written to a table:

```sql
SELECT pgmqtt_add_inbound_mapping(
    'sensor/{site_id}/temperature',
    'sensor_readings',
    '{"site_id": "{site_id}", "value": "$.temperature"}'::jsonb
);
```

When a client publishes to `sensor/site-1/temperature` with payload `{"temperature": 22.5}`, a row is inserted into `sensor_readings` with `site_id = 'site-1'` and `value = '22.5'`.

### Subscribe and receive (any MQTT client)

```bash
mosquitto_sub -h localhost -t 'topics/#'
```

### Publish (any MQTT client)

```bash
mosquitto_pub -h localhost -t 'sensor/site-1/temperature' -m '{"temperature": 22.5}'
```

## Repository Structure

- `extension/`: Rust source code for the PostgreSQL extension (pgrx).
- `docker/`: Dockerfiles and environment setups for development and testing.
- `docs/`: User and developer documentation.
- `scripts/`: Utility scripts for building and packaging.
  - `scripts/build_package.sh`: Script to build distributable tarballs.
- `tests/`: Test suites.
  - `tests/conformance/`: MQTT conformance tests against reference brokers.
  - `tests/integration/`: Integration tests for CDC, MQTT, and WebSocket flows.
  - `tests/enterprise/`: Enterprise feature tests (license gating, JWT authentication).
  - `tests/helpers/`: Shared test helpers for enterprise tests (MQTT, JWT, TLS utilities).

## Development

### Setup

Install Python test dependencies:

```bash
pip install -r requirements.txt
```

Start the development environment:

```bash
docker compose up -d --build
```

### Running tests

```bash
# Core integration tests
python -m pytest tests/integration/

# Enterprise feature tests (license gating, JWT auth)
python -m pytest tests/enterprise/ -m enterprise

# All tests
python -m pytest tests/integration/ tests/enterprise/

# WebSocket conformance test (requires Node.js)
python tests/integration/test_websocket_runner.py
```

## Building Distributables

To build the extension for shipping:

```bash
bash scripts/build_package.sh
```
