# Troubleshooting

Every entry in this guide is a real failure encountered while running this
pipeline, written down as: **symptom → diagnosis → fix → root cause**.

General diagnostic toolkit, in the order that usually pays off:

```bash
docker compose ps -a          # -a is essential: dead containers hide from plain ps
docker logs <container> --tail 50
docker exec jobmanager /opt/flink/bin/flink list    # job state: RUNNING vs RESTARTING
docker exec mc bash -c "mc ls -r minio/warehouse/"  # is data actually landing?
./smoke-test.sh               # all of the above, automated
```

A principle that recurs below: **errors surface far from their cause.** A
missing bucket appears as a Flink exception; a dead broker appears as a DNS
failure. Debug by walking upstream along the data flow.

---

## 1. Image build fails: `Could not resolve host: repo.maven.apache.org`

**Symptom.** `docker compose up` aborts while building the `sql-client`
image; a `curl` step downloading connector JARs fails with
`curl: (6) Could not resolve host`. No containers start at all (compose
stops before creating any).

**Diagnosis.** DNS resolution failure *inside the build environment*, not a
project bug. Often transient. Frequent with Docker Desktop, which runs
Docker inside a VM with its own DNS forwarding chain.

**Fix.**
1. Retry — downloaded layers are cached, the build resumes where it failed.
2. If it recurs: point Docker's daemon at public DNS. Docker Desktop →
   Settings → Docker Engine, add `"dns": ["8.8.8.8", "1.1.1.1"]`, apply and
   restart.
3. Alternative: edit the Dockerfile to fetch every JAR from the same host
   (`repo1.maven.org` mirrors `repo.maven.apache.org`).

**Root cause.** Container → Docker embedded DNS → VM → host resolver → ISP
is a long chain; one flaky link breaks name resolution for uncached hosts.

---

## 2. Flink error: `NoSuchBucketException: The specified bucket does not exist`

**Symptom.** The sql-client log shows the CREATE TABLE for the Iceberg sink
failing with a 404 `NoSuchBucketException`. No Flink job gets submitted;
`mc ls minio/warehouse/` errors or returns nothing.

**Diagnosis.** The `warehouse` bucket in MinIO was never created. Check the
initializer: `docker logs mc`. In the original repo you will see an endless
loop of `mc: <ERROR> 'config' is not a recognized command`.

**Fix.** Create the bucket manually, then resubmit the job:

```bash
docker exec mc bash -c "mc alias set minio http://minio:9000 admin password && mc mb --ignore-existing minio/warehouse"
docker compose restart sql-client
```

The hardened compose fixes this permanently (see root cause).

**Root cause.** Two bugs compounding. (a) The init script used
`mc config host add`, a command **removed** from modern `mc` releases; with
the image unpinned (`minio/mc` = latest), the script broke as MinIO shipped
new versions — the project rotted while its code never changed. (b) The
infinite `until` retry loop made the container look "Up" and healthy while
failing forever — the error only surfaced three services downstream, inside
Flink. Fixes: pin the image, use `mc alias set`, cap retries and `exit 1`
so failure is visible in `docker compose ps -a`.

**Bonus finding.** The original script also ran
`mc rm -r --force minio/warehouse` — deleting the entire warehouse on every
mc restart. Removed in the hardened compose (`mc mb --ignore-existing` makes
creation idempotent without destruction).

---

## 3. Flink job stuck RESTARTING: `No resolvable bootstrap urls given in bootstrap.servers`

**Symptom.** The Flink dashboard (http://localhost:18081) shows the job
flapping; `flink list` says `(RESTARTING)`. Restart counters in task names
climb into the hundreds. The jobmanager log's root exception says Kafka's
`bootstrap.servers` cannot be resolved.

**Diagnosis.** "No resolvable" means DNS: the hostname `broker` does not
resolve — which in Docker means **the broker container is not running**.
Docker's internal DNS only answers for live containers. Verify:

```bash
docker compose ps -a | grep broker        # plain `ps` HIDES exited containers
docker exec jobmanager getent hosts broker   # empty = unresolvable
docker logs broker --tail 20
```

In our incident the broker log showed:
`Timed out waiting for connection to Zookeeper server [zookeeper:2181]`.

**Fix.** Start the broker again:

```bash
docker compose up -d broker
```

The Flink job **self-heals** — it has been retrying all along; the next
attempt finds the broker and resumes from its last checkpoint. No data is
lost: events written to Kafka before/without Flink simply wait in the topic.

**Root cause.** A startup race: the broker's pre-flight check waits at most
~40s for ZooKeeper; on a slow or memory-pressured cold start ZooKeeper is
not ready in time, the broker exits, and nothing restarts it. Three
hardening fixes: a *real* ZooKeeper healthcheck, the broker depending on
`condition: service_healthy` (bare `depends_on` orders startup but never
waits for readiness), and `restart: on-failure` so the race self-heals even
when it fires. Note the original compose had a placebo broker healthcheck —
literally `sleep 1` — which always reported healthy.

---

## 4. Job runs but no data files appear in MinIO

**Symptom.** `flink list` shows `(RUNNING)`, the table's `metadata.json`
exists, but `mc ls -r minio/warehouse/` shows no files under `data/` even
after several minutes.

**Diagnosis.** Iceberg sinks commit data **only when a Flink checkpoint
completes** — never continuously. Check the job's Checkpoints tab in the
Flink UI (or the jobmanager log). If checkpointing is disabled or failing,
rows accumulate in memory forever and nothing lands in storage.

**Fix.** Ensure the job script sets a checkpoint interval
(`SET 'execution.checkpointing.interval' = '10s';` in
clickstream-filtering.sql) and give the job at least one full interval
before judging. Distinguish states: RESTARTING = see issue 3; RUNNING with
records flowing but zero checkpoints = checkpoint configuration problem.

**Root cause / mental model.** Streaming writes are batched into atomic
snapshot commits at checkpoint boundaries. "Running" and "producing output"
are different claims — verify both.

---

## 5. Superset: `Could not load database driver for: trino`

**Symptom.** Adding the Trino database connection in the Superset UI fails
with the driver error, even though the Dockerfile installs a Trino package.

**Diagnosis.** Two stacked problems; check both:

```bash
docker exec superset pip list | grep -i trino     # is the right package there?
docker exec superset pip show trino | grep Location   # ...in the right Python?
docker exec superset python -c "import sqlalchemy; sqlalchemy.create_engine('trino://trino@trino:8080/iceberg'); print('driver OK')"
```

(a) The original Dockerfile installs `sqlalchemy-trino` — the **deprecated**
driver; modern Superset needs the `trino` package. (b) Modern Superset
images run from a uv-managed virtualenv at `/app/.venv` **that contains no
pip**; plain `pip install` lands packages in the system Python that
Superset never sees (the traceback paths reveal this: they all point into
`/app/.venv/...`).

**Fix.** In `superset/Dockerfile`:

```dockerfile
FROM apache/superset:6.1.0          # pin it — ':latest' caused this
RUN uv pip install --python /app/.venv/bin/python psycopg2-binary "trino[sqlalchemy]"
```

Rebuild (`docker compose up -d --build superset`), wait ~2-4 min for init
(admin creation + db migrations run on every boot), hard-refresh the
browser, reconnect with `trino://trino@trino:8080/iceberg/db`.

**Root cause.** Unpinned `apache/superset:latest` again: the image's driver
ecosystem AND internal layout (uv venv) changed underneath a Dockerfile
that was correct when written.

---

## 6. Superset page won't load right after start

**Symptom.** Container is "Up" (possibly "unhealthy") but
http://localhost:8088 refuses connections.

**Diagnosis.** Not broken — booting. The entrypoint runs admin creation,
`superset db upgrade`, and role init *before* the web server opens the
port; several minutes on a modest machine. Watch for gunicorn's
`Listening at: http://0.0.0.0:8088` in `docker logs superset -f`, or probe:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8088/health
```

**Fix.** Wait. If the log shows a crash instead, the likely culprit is the
placeholder `SECRET_KEY` in `superset/superset_config.py` — newer versions
may refuse to boot with a known-default key; replace it with the output of
`openssl rand -base64 42`.

---

## 7. Data or dashboards vanish after `docker compose down`

**Symptom.** After recreating containers, the warehouse is empty and/or
Superset dashboards are gone — even without the `-v` flag.

**Diagnosis / root cause.** The original compose *declared* the
`minio_data` and `superset-data` volumes but **never mounted them** into
any service — all state lived inside container filesystems and died with
them. Separately, `flink/sql-jobs/clickstream-filtering.sql` begins with
`DROP TABLE IF EXISTS iceberg.db.clickstream_sink`, so every sql-client
rerun destroys the table and its entire snapshot history regardless of
volumes.

**Fix.** The hardened compose mounts `minio_data:/data` (MinIO) and
`superset-data:/app/superset_home` (Superset). The DROP TABLE behavior is a
deliberate demo choice; for persistence across job resubmissions, replace
the CTAS with `CREATE TABLE IF NOT EXISTS` + a separate `INSERT INTO`.

---

## Appendix: reading `docker compose ps` correctly

Three lessons this pipeline teaches about container status:

- **Plain `ps` hides the dead.** Always use `docker compose ps -a`; an
  exited broker is invisible otherwise (incident 3).
- **"Up" does not mean "working".** The original mc container stayed Up
  while failing in an infinite loop (incident 2); the broker's healthcheck
  was `sleep 1` (always "healthy"). Trust real healthchecks and logs.
- **"Restarting"/"unhealthy" during startup can be normal.** Slow initializers
  (Superset) look sick before they look ready; check logs before assuming
  failure (incident 6).
