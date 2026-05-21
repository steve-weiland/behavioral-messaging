#!/usr/bin/env bash
# load-v1.sh — sustained-load driver for V1 track-api.
#
# Closed-loop, bash + curl. Each of $CONCURRENCY worker shells hot-loops
# POST /events for $DURATION seconds against a fixed pool of $PEOPLE
# pre-identified persons. The campaign welcome_pro is ensured at start
# so every event matches a trigger and the Dispatcher actually fans out.
#
# Set DURATION=0 to prep only (people + campaign) and exit.
#
# Env knobs (all optional):
#   WORKSPACE   default ws_alpha
#   DURATION    default 60   (seconds; 0 = prep only)
#   CONCURRENCY default 20
#   PEOPLE      default 50
#   TRACK_URL   default http://localhost:8090
#
# Output: one summary line per worker + one aggregate line at the end.

set -uo pipefail

WORKSPACE=${WORKSPACE:-ws_alpha}
DURATION=${DURATION:-60}
CONCURRENCY=${CONCURRENCY:-20}
PEOPLE=${PEOPLE:-50}
TRACK_URL=${TRACK_URL:-http://localhost:8090}

echo "== load-v1 =="
echo "workspace=$WORKSPACE duration=${DURATION}s concurrency=$CONCURRENCY people=$PEOPLE"
echo "track=$TRACK_URL"
echo

# --- prep -------------------------------------------------------------

echo "[prep] ensure welcome_pro campaign exists"
if ! curl -fsS -H "X-Workspace-ID: $WORKSPACE" "$TRACK_URL/campaigns/welcome_pro" > /dev/null 2>&1; then
    curl -fsS -X POST \
        -H 'Content-Type: application/json' \
        -H "X-Workspace-ID: $WORKSPACE" \
        -d '{"campaign_id":"welcome_pro","name":"Welcome Pro users","trigger":{"op":"and","conditions":[{"op":"attr_eq","key":"plan","value":"pro"},{"op":"event_seen","name":"signed_up"}]},"template":"Welcome {{.Person.PersonID}} — your {{.Attrs.plan}} plan is live."}' \
        "$TRACK_URL/campaigns" > /dev/null
    echo "[prep]   created"
else
    echo "[prep]   already exists"
fi

echo "[prep] identify $PEOPLE people (plan=pro)"
for i in $(seq 1 "$PEOPLE"); do
    curl -fsS -X POST \
        -H 'Content-Type: application/json' \
        -H "X-Workspace-ID: $WORKSPACE" \
        -d "{\"person_id\":\"load_p_$i\",\"attributes\":{\"plan\":\"pro\"}}" \
        "$TRACK_URL/people" > /dev/null
done
echo "[prep]   done"
echo

if [[ "$DURATION" -eq 0 ]]; then
    echo "DURATION=0 — prep only, exiting."
    exit 0
fi

# --- soak -------------------------------------------------------------

RESULTS=$(mktemp -t load-v1.XXXXXX)
trap 'rm -f "$RESULTS"' EXIT

worker() {
    local id=$1
    local deadline=$(($(date +%s) + DURATION))
    local sent=0 fail=0 pid
    while [[ $(date +%s) -lt $deadline ]]; do
        pid="load_p_$((RANDOM % PEOPLE + 1))"
        if curl -fsS -o /dev/null \
                -X POST \
                -H 'Content-Type: application/json' \
                -H "X-Workspace-ID: $WORKSPACE" \
                -d "{\"person_id\":\"$pid\",\"event_name\":\"signed_up\",\"payload\":{}}" \
                "$TRACK_URL/events"; then
            sent=$((sent + 1))
        else
            fail=$((fail + 1))
        fi
    done
    printf '%s %d %d\n' "$id" "$sent" "$fail" >> "$RESULTS"
}

echo "[soak] $CONCURRENCY workers × ${DURATION}s closed-loop POST /events"
START=$(date +%s)
for w in $(seq 1 "$CONCURRENCY"); do
    worker "$w" &
done
wait
ELAPSED=$(($(date +%s) - START))
echo "[soak] done in ${ELAPSED}s"
echo

# --- tally ------------------------------------------------------------

echo "== per-worker (id sent fail) =="
sort -n "$RESULTS"
echo

TOTAL_SENT=$(awk '{s+=$2} END {print s+0}' "$RESULTS")
TOTAL_FAIL=$(awk '{s+=$3} END {print s+0}' "$RESULTS")
RATE=$((TOTAL_SENT / (ELAPSED > 0 ? ELAPSED : 1)))

echo "== aggregate =="
printf 'events_sent     = %d\n' "$TOTAL_SENT"
printf 'failures        = %d\n' "$TOTAL_FAIL"
printf 'elapsed_s       = %d\n' "$ELAPSED"
printf 'achieved_rate   = %d events/s\n' "$RATE"
