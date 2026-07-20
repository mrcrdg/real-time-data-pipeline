#!/usr/bin/env bash
# smoke-test.sh — verifies the pipeline is actually working.
# Fast mode (default): services up, Flink job RUNNING, files in MinIO. ~1 min.
# Full mode (--full):  additionally proves data is FLOWING via Trino row counts. ~3 min.
#
# Motivated by real incidents:
#   - mc container looked "Up" while silently failing forever  -> we assert the bucket has files
#   - Flink job crash-looped 494 times with no visible error   -> we assert RUNNING, not just "job exists"
#   - broker died at startup, docker compose ps hid it          -> we assert each container individually

set -uo pipefail

FULL=0
[[ "${1:-}" == "--full" ]] && FULL=1

PASS=0
FAIL=0

ok()   { echo "  ✔ $1"; PASS=$((PASS+1)); }
bad()  { echo "  ✘ $1"; FAIL=$((FAIL+1)); }

echo "== 1. Containers running =="
for c in zookeeper broker minio iceberg-rest jobmanager taskmanager clickstream-producer; do
  state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [[ "$state" == "running" ]]; then ok "$c is running"; else bad "$c is $state"; fi
done

echo "== 2. Flink job is RUNNING (not RESTARTING) =="
joblist=$(docker exec jobmanager /opt/flink/bin/flink list 2>/dev/null)
if echo "$joblist" | grep -q "(RUNNING)"; then
  ok "Flink job in RUNNING state"
elif echo "$joblist" | grep -q "(RESTARTING)"; then
  bad "Flink job is RESTARTING (crash loop — check: docker logs jobmanager)"
else
  bad "No running Flink job found (did sql-client submit? check: docker logs sql-client)"
fi

echo "== 3. Data files exist in MinIO warehouse =="
files=$(docker exec mc bash -c "mc ls -r minio/warehouse/ 2>/dev/null" | wc -l)
if [[ "$files" -gt 0 ]]; then
  ok "warehouse bucket has $files objects"
else
  bad "warehouse bucket is empty (bucket missing or Flink never committed)"
fi

if [[ "$FULL" -eq 1 ]]; then
  echo "== 4. FULL: data is flowing (Trino count grows) =="
  c1=$(docker compose exec -T trino trino --execute \
       "SELECT count(*) FROM iceberg.db.clickstream_sink" 2>/dev/null | tr -d '"')
  if [[ -z "$c1" ]]; then
    bad "Trino query failed (is trino up? docker compose up -d trino)"
  else
    ok "current row count: $c1"
    echo "  ... waiting 45s for new checkpoints ..."
    sleep 45
    c2=$(docker compose exec -T trino trino --execute \
         "SELECT count(*) FROM iceberg.db.clickstream_sink" 2>/dev/null | tr -d '"')
    if [[ -n "$c2" && "$c2" -gt "$c1" ]]; then
      ok "row count grew: $c1 -> $c2 (pipeline is LIVE end to end)"
    else
      bad "row count did not grow ($c1 -> ${c2:-?}) — producer or Flink stalled"
    fi
  fi
fi

echo
echo "== Result: $PASS passed, $FAIL failed =="
[[ "$FAIL" -eq 0 ]] && echo "PIPELINE OK" || echo "PIPELINE BROKEN"
exit "$FAIL"
