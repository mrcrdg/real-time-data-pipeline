# CLAUDE.md — superset/

Superset build context: Dockerfile, superset_config.py, superset-init.sh.
Superset's ONLY pipeline connection is Trino
(`trino://trino@trino:8080/iceberg/db`); it knows nothing of Kafka, Flink,
or MinIO.

## Hard-won constraints (incident TROUBLESHOOTING.md #5)

- Base image MUST stay pinned (`FROM apache/superset:<version>`, currently
  6.1.0). `:latest` broke this build twice in different ways.
- Modern Superset runs from a uv-managed virtualenv at `/app/.venv` that
  contains NO pip. Python packages MUST be installed with:
  `RUN uv pip install --python /app/.venv/bin/python <pkg>`
  A plain `pip install` lands in the system Python and Superset never sees
  it — the error is "Could not load database driver" at runtime, not a
  build failure.
- The Trino driver package is `trino[sqlalchemy]`; `sqlalchemy-trino` is
  deprecated and does not register on current versions.
- When bumping the base image version, re-verify the driver with:
  `docker exec superset python -c "import sqlalchemy; sqlalchemy.create_engine('trino://trino@trino:8080/iceberg'); print('driver OK')"`

## Behavior notes

- Boot is slow BY DESIGN: superset-init.sh runs admin creation +
  `superset db upgrade` + role init on EVERY start, before the port opens.
  Several minutes on modest hardware is normal, not a failure
  (TROUBLESHOOTING.md #6).
- Superset metadata (users, dashboards) lives in SQLite inside the
  container, persisted via the `superset-data` volume mounted at
  `/app/superset_home`. Do not remove that mount.
- `SECRET_KEY` in superset_config.py is a demo placeholder; newer versions
  may refuse to boot with a default key — generate one with
  `openssl rand -base64 42` if that happens, and wire it via `.env`.

## Testing a change

```bash
docker compose up -d --build superset
docker logs superset -f            # wait for gunicorn "Listening at :8088"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8088/health   # 200
# then in the UI: Settings → Database Connections → test the Trino connection
```
