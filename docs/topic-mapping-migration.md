# Migrating Topic Schemas

pgmqtt supports multiple mappings per table via the `mapping_name` parameter. This allows you to run old and new topic schemas in parallel, giving consumers time to migrate without downtime.

## Example

```sql
-- Existing mapping (mapping_name defaults to 'default')
SELECT pgmqtt_add_outbound_mapping('public', 'events',
    'events/{{ op | lower }}', '{{ columns | tojson }}');

-- Add a v2 mapping — both fire on every change to events
SELECT pgmqtt_add_outbound_mapping('public', 'events',
    'events/v2/{{ op | lower }}', '{"id": "{{ columns.id }}"}', 0, 'v2');

-- Once all consumers have moved to v2, drop the old mapping
SELECT pgmqtt_remove_outbound_mapping('public', 'events', 'default');
```

While both mappings are active, each `INSERT`, `UPDATE`, or `DELETE` on `events` produces two MQTT publishes — one per mapping. Mapping changes are applied in WAL order, so there is no window where a row is published under the wrong schema.
