# CLAUDE.md — producer/

One Python script (`producer.py`): invents one fake clickstream event per
second and sends it to the Kafka topic `clickstream` as JSON.

## Contracts this code must honor

- The event schema (field names, types, nesting of `geo_location`) is
  consumed by `flink/sql-jobs/clickstream-filtering.sql` — the Kafka source
  table's column list must match. If you add/rename/remove a field here,
  update that SQL file in the same change, and note that Iceberg sees the
  schema change only after the sink table is recreated.
- There is deliberately NO timestamp field in the event; Flink uses Kafka's
  broker arrival timestamp. Adding a real event timestamp is a planned,
  separate task — do not add it casually.
- `key=session_id` is intentional: it keeps a session's events ordered
  within one partition. Do not remove or change the key.
- Broker address comes ONLY from the `KAFKA_BROKER` env var (compose sets
  `broker:29092`, the Docker-internal listener). Never hardcode it.

## Style

- Dependencies limited to confluent-kafka + faker; no new ones without
  discussion.
- produce() is async; keep the poll()/flush() pattern so delivery reports
  and clean shutdown keep working.
- Log the full event JSON at INFO — it is the primary debugging window
  (`docker logs clickstream-producer`).

## Testing a change

```bash
docker compose up -d --build producer
docker logs clickstream-producer --tail 20      # events + "Message delivered"
./smoke-test.sh                                 # from repo root
```
