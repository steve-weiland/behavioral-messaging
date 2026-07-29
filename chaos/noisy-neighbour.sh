#!/usr/bin/env bash
# V3-3 / BM-133: prove tenant isolation rather than asserting it.
#
# One workspace floods; another sends steady light traffic. The claim under
# test: the quiet tenant's work still gets served promptly while the noisy
# tenant's backlog grows.
#
#   ./chaos/noisy-neighbour.sh              caps on  (default)
#   WORKSPACE_CAPS=off ./chaos/noisy-neighbour.sh    the A/B baseline
#
# Requires both workspaces to exist in the workspaces table and the worker to
# have subscribed to them (it lists workspaces at boot), so run
# `make noisy-prep` first if this is a fresh database.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8090}"
NOISY="${NOISY:-ws_noisy}"
QUIET="${QUIET:-ws_quiet}"
DURATION="${DURATION:-30}"
NOISY_CONC="${NOISY_CONC:-40}"
QUIET_RATE="${QUIET_RATE:-2}"   # requests/sec from the quiet tenant
RABBIT="${RABBIT:-http://localhost:15672}"

post() { # $1 workspace, $2 person
  curl -sS -o /dev/null -w '%{time_total}\n' -X POST "$GATEWAY/events" \
    -H "X-Workspace-ID: $1" -H 'Content-Type: application/json' \
    -d "{\"person_id\":\"$2\",\"event_name\":\"signed_up\",\"payload\":{}}" 2>/dev/null || echo "ERR"
}

depth() { # $1 queue
  curl -fsS -u guest:guest "$RABBIT/api/queues/%2f/$1" 2>/dev/null \
    | python3 -c 'import sys,json;print(json.load(sys.stdin).get("messages",0))' 2>/dev/null || echo 0
}

echo "noisy-neighbour: $NOISY floods at concurrency=$NOISY_CONC, $QUIET sends ${QUIET_RATE}/s, ${DURATION}s"
echo

QUIET_LAT=$(mktemp)
trap 'rm -f "$QUIET_LAT"' EXIT

# Flood the noisy tenant.
for i in $(seq 1 "$NOISY_CONC"); do
  (
    end=$(( $(date +%s) + DURATION ))
    n=0
    while [ "$(date +%s)" -lt "$end" ]; do
      post "$NOISY" "noisy_p$(( n % 50 ))" >/dev/null
      n=$((n+1))
    done
  ) &
done

# Steady light traffic from the quiet tenant, recording each latency.
(
  end=$(( $(date +%s) + DURATION ))
  n=0
  while [ "$(date +%s)" -lt "$end" ]; do
    post "$QUIET" "quiet_p$(( n % 5 ))" >> "$QUIET_LAT"
    n=$((n+1))
    sleep "$(python3 -c "print(1/$QUIET_RATE)")"
  done
) &

wait
echo "load finished — letting the fan-out drain for 10s"
sleep 10

NOISY_DEPTH=$(depth "campaigns.fanout.$NOISY")
QUIET_DEPTH=$(depth "campaigns.fanout.$QUIET")

echo
echo "=== result ==="
python3 - "$QUIET_LAT" "$NOISY_DEPTH" "$QUIET_DEPTH" <<'PY'
import sys
lat = [float(x) for x in open(sys.argv[1]) if x.strip() and 'ERR' not in x]
nd, qd = sys.argv[2], sys.argv[3]
lat.sort()
def pct(p):
    return lat[min(len(lat)-1, int(len(lat)*p))] * 1000 if lat else float('nan')
print(f"  quiet tenant requests   : {len(lat)}")
print(f"  quiet intake p50 / p99  : {pct(0.50):.0f} ms / {pct(0.99):.0f} ms")
print(f"  noisy queue depth left  : {nd}")
print(f"  quiet queue depth left  : {qd}")
print()
# The isolation claim, stated as a check rather than a vibe.
if int(qd) <= 10:
    print("  PASS  the quiet tenant's queue drained despite the flood")
else:
    print(f"  FAIL  the quiet tenant is backlogged ({qd}) — it is being starved")
PY
echo
echo "per-workspace in-flight + admission waits (BM-132):"
curl -fsS 'http://localhost:8889/metrics' 2>/dev/null \
  | grep -E '^campaign_(inflight_events|admission_waits_total)' | head -8 || echo "  (scrape the collector for these)"
