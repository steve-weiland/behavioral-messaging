#!/usr/bin/env bash
# BM-135: do the admission caps earn their keep by bounding AGGREGATE
# concurrency against one MySQL pool?
#
# BM-133 established that the caps are not a fairness mechanism — per-workspace
# queues plus per-consumer prefetch already isolate tenants. The remaining
# hypothesis is different: with MANY simultaneously busy workspaces,
# N × PREFETCH goroutines thrash one connection pool, and bounding the aggregate
# keeps total in-flight work inside the pool so everyone drains faster.
#
# If that's true, caps ON should show HIGHER aggregate fan-out throughput than
# caps OFF. If it's false, the caps are pure cost and should be deleted.
#
# Measured quantity: enrollments written per second across all workspaces —
# the fan-out drain rate, read from MySQL (immune to scrape lag), which is the
# same authoritative signal the V2 ceiling runs used.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8090}"
WORKSPACES="${WORKSPACES:-8}"
CONC="${CONC:-25}"          # per workspace
DURATION="${DURATION:-30}"
LABEL="${LABEL:-run}"

enrollments() {
  docker compose exec -T mysql mysql -ubm -pbm bm -N -e \
    'SELECT COUNT(*) FROM journey_enrollments;' 2>/dev/null | tr -d '[:space:]'
}

echo "[$LABEL] $WORKSPACES workspaces × c=$CONC for ${DURATION}s (aggregate concurrency ≈ $((WORKSPACES*CONC)))"

BEFORE=$(enrollments)
START=$(date +%s)

pids=()
for i in $(seq 1 "$WORKSPACES"); do
  go run ./cmd/loadgen -url "$GATEWAY" -workspace "ws_bm135_$i" \
    -c "$CONC" -d "${DURATION}s" -people 20 >/dev/null 2>&1 &
  pids+=($!)
done
wait "${pids[@]}" 2>/dev/null || true

echo "[$LABEL] load finished; draining 20s"
sleep 20
AFTER=$(enrollments)
END=$(date +%s)

python3 - "$BEFORE" "$AFTER" "$START" "$END" "$LABEL" <<'PY'
import sys
before, after, start, end, label = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
delta = after - before
elapsed = max(1, end - start)
print(f"[{label}] enrollments written = {delta} over {elapsed}s")
print(f"[{label}] aggregate fan-out   = {delta/elapsed:.0f}/s")
PY
