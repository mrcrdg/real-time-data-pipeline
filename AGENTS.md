# AGENTS.md — project context for AI coding assistants

## What this project is

A learning-oriented, end-to-end streaming data pipeline: a Python producer
sends fake clickstream events to Kafka; Flink filters them (purchases only)
and writes Iceberg tables to MinIO; Trino queries them; Superset visualizes.
Everything runs locally via Docker Compose. The audience is data
engineers — clarity beats cleverness in both code and docs.

Read `docs/ARCHITECTURE.md` before design-level changes.
Read `docs/TROUBLESHOOTING.md` before "fixing" container behavior — several
things that look broken (slow Superset boot, mc idling) are known-normal.

Subfolders have their own CLAUDE.md with area-specific guidance:
`producer/`, `flink/`, `superset/`.

## Setup commands

- Start the core pipeline: `make up`
- Add the query engine: `make trino` — SQL shell: `make query`
- Add dashboards (heavy, builds image): `make superset`
- Kafka UI (rarely needed, memory hog): `docker compose --profile ui up -d control-center`
- Stop, keep data: `make down` — full reset including data: `make clean`
- Logs: `make logs` — container states: `make ps` (uses `ps -a`; plain
  `ps` hides dead containers)
- First-time setup: `cp .env.example .env` (defaults work as-is)

## Testing

- Run `./smoke-test.sh` after ANY change that touches the pipeline
  (fast, ~1 min: containers up, Flink job RUNNING, files in MinIO).
- Run `./smoke-test.sh --full` for end-to-end proof (~3 min: asserts the
  Trino row count grows — data actually flowing).
- Acceptance bar for compose/infra changes: a hands-free cold start —
  `docker compose down -v && docker compose up -d --build`, wait, smoke
  test passes with zero manual intervention.
- A Flink job showing RUNNING is not proof of output; only files landing
  in MinIO and a growing Trino count are. Verify both.

## Code style

- Comments explain WHY, not what — especially in the Flink SQL job.
- Python (producer): keep it simple and readable; standard library +
  confluent-kafka + faker only. No new dependencies without discussion.
- SQL (Flink job): keep the SET / ADD JAR / catalog / source / sink
  section order; the file doubles as teaching material.
- YAML (compose): every non-obvious choice carries a `# FIX:` or
  explanatory comment referencing its motivating incident.
- Docs are single-source: architecture in `docs/ARCHITECTURE.md`,
  incidents in `docs/TROUBLESHOOTING.md`; README links, never duplicates.
- Commits: conventional style (`fix(mc): ...`, `docs: ...`), imperative
  mood, body explains why. Disclose AI assistance in the PR description.

## Security

- Never commit `.env` (gitignored); `.env.example` carries demo values only.
- Credentials here are demo-only (`admin`/`password`) and intentionally
  simple — but the PATTERN must stay clean: new secrets go through `.env`,
  never hardcoded in new files.
- Known legacy exception: MinIO credentials are ALSO hardcoded in
  `flink/sql-jobs/clickstream-filtering.sql` and
  `trino/iceberg.properties`. If credentials change anywhere, change all
  three places and say so in the commit message.
- Never introduce real API keys, tokens, or cloud credentials anywhere in
  this repo, including examples and docs.

## Infrastructure conventions

- Pin ALL Docker image versions. Never `:latest` — three past incidents
  (mc, superset) came from unpinned images rotting over time.
- Core services only by default; target machine is ~8–16GB RAM. No new
  always-on heavy services.
- Failures must be loud: no infinite retry loops in init scripts — cap
  retries and `exit 1` so `docker compose ps -a` shows the truth.
- New services get: pinned image, healthcheck, `depends_on` with
  conditions, `restart: on-failure`, and a volume if they hold state.

## Known sharp edges (do not "fix" without discussion)

- `clickstream-filtering.sql` starts with `DROP TABLE IF EXISTS` — every
  sql-client rerun destroys the table and its snapshot history. Known demo
  tradeoff; changing it is a planned, deliberate task.
- `json.ignore-parse-errors=true` silently drops malformed Kafka messages.
  Known tradeoff.
- Event time is Kafka's broker arrival timestamp (`METADATA FROM
  'timestamp'`), not a field in the event — the producer generates none.
- Checkpoint interval is 10s: small Parquet files accumulate fast
  (small-files problem). Compaction/partitioning is future work.

## Roadmap context

Planned next: GitHub Actions CI running the smoke test; migrating Kafka to
KRaft mode (removing ZooKeeper); replacing the CTAS+DROP pattern with
CREATE IF NOT EXISTS + INSERT for persistent tables.
