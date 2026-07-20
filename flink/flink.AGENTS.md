# CLAUDE.md — flink/

Two concerns, deliberately separated:

- `sql-client/` — THE TOOL: Dockerfile (downloads the 8 connector JARs)
  and flink-conf.yaml (how to reach the jobmanager). Changes rarely.
- `sql-jobs/` — THE WORK: streaming job definitions submitted at startup.
  Changes often. New jobs are new .sql files here (also add a bind mount +
  invocation in docker-compose.yaml for each).

## sql-client/ (Dockerfile, flink-conf.yaml)

- JAR versions are tightly coupled: Flink 1.18 needs the Iceberg runtime
  FOR 1.18 and the Kafka connector FOR 1.18. When bumping Flink, bump every
  connector in lockstep — mismatches fail at runtime with
  ClassNotFoundException, not at build time.
- Prefer downloading all JARs from the same Maven host (repo1.maven.org);
  mixed hosts caused a DNS build failure once (TROUBLESHOOTING.md #1).
- The SQL job files are bind-mounted at runtime, NOT baked into the image:
  editing a .sql file needs only `docker compose restart sql-client`, no
  rebuild. Keep it that way.

## sql-jobs/ (the streaming jobs)

- File structure is teaching material — keep the section order:
  SET (factory settings) → ADD JAR (plugins) → CREATE CATALOG (addresses)
  → source table (the lens on Kafka) → sink + INSERT (the actual job).
- Comments explain WHY. The existing comments encode hard-won facts
  (checkpoint-commit coupling, METADATA timestamp, CTAS schema inference);
  do not strip or "simplify" them.
- Sharp edges (see root CLAUDE.md, do not change without discussion):
  the leading DROP TABLE, ignore-parse-errors, earliest-offset,
  the 10s checkpoint interval.
- Iceberg data only becomes visible ON checkpoints. A job that is RUNNING
  but has never checkpointed has produced nothing.

## Testing a change

```bash
docker compose restart sql-client
docker logs sql-client --tail 30                # every statement should say "succeed"
docker exec jobmanager /opt/flink/bin/flink list   # expect (RUNNING)
docker exec mc bash -c "mc ls -r minio/warehouse/" # new files within ~30s
./smoke-test.sh --full                          # from repo root
```

Remember: restarting sql-client currently DROPS and recreates the sink
table (known tradeoff) — expect Trino counts to reset.
