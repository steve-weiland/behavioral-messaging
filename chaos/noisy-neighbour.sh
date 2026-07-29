#!/usr/bin/env bash
# V3-3 / BM-133: prove tenant isolation rather than asserting it.
#
# The first version of this script measured the quiet tenant's INTAKE latency
# and "passed" with the caps both on and off — because the bash+curl driver
# (~557/s, per this repo's V1 writeup) never saturated the shared MySQL pool, so
# the contention the caps arbitrate never happened. Two fixes:
#
#   1. Drive the noisy tenant with cmd/loadgen (keep-alive, ~6× the bash
#      driver) so it can actually saturate something.
#   2. Measure the quiet tenant's QUEUE DEPTH during the flood, not its intake
#      latency. Intake is the gateway, which isn't where contention bites; a
#      starved tenant shows up as its fan-out queue backing up.
#
# Contention also has to be reachable on this hardware, which means running the
# worker with a deliberately small MYSQL_MAX_OPEN_CONNS — see make bm133.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8090}"
NOISY="${NOISY:-ws_noisy}"
QUIET="${QUIET:-ws_quiet}"
DURATION="${DURATION:-30}"
NOISY_CONC="${NOISY_CONC:-100}"
QUIET_RATE="${QUIET_RATE:-2}"
RABBIT="${RABBIT:-http://localhost:15672}"
LABEL="${LABEL:-run}"

depth() {
  curl -fsS -u guest:guest "$RABBIT/api/queues/%2f/campaigns.fanout.$1" 2>/dev/null \
    | python3 -c 'import sys,json;print(json.load(sys.stdin).get("messages",0))' 2>/dev/null || echo 0
}

SAMPLES=$(mktemp); QUIET_LAT=$(mktemp)
trap 'rm -f "$SAMPLES" "$QUIET_LAT"' EXIT

echo "[$LABEL] $NOISY flooded by cmd/loadgen (c=$NOISY_CONC), $QUIET at ${QUIET_RATE}/s, ${DURATION}s"

# Noisy tenant: the Go driver, so the load is real.
go run ./cmd/loadgen -url "$GATEWAY" -workspace "$NOISY" \
  -c "$NOISY_CONC" -d "${DURATION}s" -people 50 >/dev/null 2>&1 &
LOADGEN=$!

# Quiet tenant: steady light traffic, latency recorded.
(
  end=$(( $(date +%s) + DURATION ))
  n=0
  while [ "$(date +%s)" -lt "$end" ]; do
    curl -sS -o /dev/null -w '%{time_total}\n' -X POST "$GATEWAY/events" \
      -H "X-Workspace-ID: $QUIET" -H 'Content-Type: application/json' \
      -d "{\"person_id\":\"quiet_p$(( n % 5 ))\",\"event_name\":\"signed_up\",\"payload\":{}}" \
      >> "$QUIET_LAT" 2>/dev/null || true
    n=$((n+1))
    sleep "$(python3 -c "print(1/$QUIET_RATE)")"
  done
) &
QUIETPID=$!

# Sample both queues every second — one cheap HTTP call each. This is the
# isolation signal: a starved tenant's queue grows.
(
  end=$(( $(date +%s) + DURATION ))
  while [ "$(date +%s)" -lt "$end" ]; do
    echo "$(depth "$NOISY") $(depth "$QUIET")" >> "$SAMPLES"
    sleep 1
  done
) &
SAMPLER=$!

wait "$LOADGEN" "$QUIETPID" "$SAMPLER" 2>/dev/null || true
echo "[$LABEL] load finished; draining 15s"
sleep 15

echo "[$LABEL] final depths: noisy=$(depth "$NOISY") quiet=$(depth "$QUIET")"
python3 - "$SAMPLES" "$QUIET_LAT" "$LABEL" <<'PY'
import sys
samples = [l.split() for l in open(sys.argv[1]) if len(l.split()) == 2]
noisy = [int(a) for a, _ in samples]
quiet = [int(b) for _, b in samples]
lat = sorted(float(x) for x in open(sys.argv[2]) if x.strip())
label = sys.argv[3]

def pct(xs, p):
    return xs[min(len(xs) - 1, int(len(xs) * p))] * 1000 if xs else float("nan")

print(f"[{label}] samples={len(samples)}")
print(f"[{label}] noisy queue  max={max(noisy) if noisy else 0:>7} mean={sum(noisy)//max(1,len(noisy)):>7}")
print(f"[{label}] QUIET queue  max={max(quiet) if quiet else 0:>7} mean={sum(quiet)//max(1,len(quiet)):>7}"
      f"  samples_backed_up={sum(1 for q in quiet if q > 5)}/{len(quiet)}")
print(f"[{label}] quiet intake p50={pct(lat,0.5):.0f}ms p99={pct(lat,0.99):.0f}ms  n={len(lat)}")
PY
