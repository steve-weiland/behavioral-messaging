#!/usr/bin/env bash
# smoke.sh — end-to-end gate over the running stack (make smoke).
#
# Seven checks that walk every mechanism the README claims, including the
# surfaces hardened in the 2026-09 review:
#   1. all 13 services running
#   2. event → campaign dispatch → enrollment row (fan-out path)
#   3. the same event enrolls a journey; the scheduler executes
#      delay → send and the run reaches 'done' with its send gated
#      through idempotency_keys (FSM + lease + gate, BM-115..118/141)
#   4. poison → campaigns.fanout.dlq via the CONFIRMED dead-letter path,
#      with the why-headers (BM-121/124)
#   5. RabbitMQ panel metrics exist in Prometheus (both scrape jobs)
#   6. no alert is firing on a healthy stack (the zero-traffic SLO
#      regression check)
#   7. track-api logs queryable in Loki (Alloy + pinned project name)
#
# Assumes `make up` has completed. Exits non-zero on first failure.
set -euo pipefail

BASE="${BASE:-http://localhost:8090}"
PROM="${PROM:-http://localhost:9090}"
LOKI="${LOKI:-http://localhost:3100}"
WS="ws_alpha"
COMPOSE="${COMPOSE:-docker compose}"

pass=0
check() { pass=$((pass+1)); echo "  ok $pass: $1"; }
fail() { echo "  FAIL: $1" >&2; exit 1; }

retry() {  # retry <seconds> <description> <command...> — every 2s
    local deadline=$(( $(date +%s) + $1 )); local what=$2; shift 2
    until "$@" >/dev/null 2>&1; do
        (( $(date +%s) < deadline )) || fail "$what (timed out)"
        sleep 2
    done
}

api() {  # api <method> <path> [json]
    if [ $# -eq 3 ]; then
        curl -fsS -X "$1" -H "X-Workspace-ID: $WS" -H 'Content-Type: application/json' -d "$3" "$BASE$2"
    else
        curl -fsS -X "$1" -H "X-Workspace-ID: $WS" "$BASE$2"
    fi
}

sql() { $COMPOSE exec -T mysql mysql -ubm -pbm bm -sN -e "$1" 2>/dev/null; }

echo "smoke: waiting for readiness"
retry 120 "track-api /healthz" curl -fsS "$BASE/healthz"
retry 90  "prometheus ready"   curl -fsS "$PROM/-/ready"
retry 90  "loki ready"         curl -fsS "$LOKI/ready"

# 1 ── everything running
running=$($COMPOSE ps --status running --format '{{.Service}}' | wc -l | tr -d ' ')
[ "$running" -ge 13 ] || fail "only $running/13 services running"
check "all 13 services running"

# Fixed-id smoke definitions (idempotent create), unique person + event per run.
STAMP="$(date +%s)$$"
PERSON="p_smoke_$STAMP"
api GET /campaigns/smoke_welcome >/dev/null 2>&1 || api POST /campaigns \
    '{"campaign_id":"smoke_welcome","name":"Smoke welcome","trigger":{"op":"event_seen","name":"smoke_signed_up"},"template":"smoke hi {{.Person.PersonID}}"}' >/dev/null
api GET /journeys/smoke_journey >/dev/null 2>&1 || api POST /journeys \
    '{"journey_id":"smoke_journey","name":"Smoke journey","trigger":{"op":"event_seen","name":"smoke_signed_up"},"steps":[{"type":"delay","seconds":2},{"type":"send","template":"smoke send {{.Person.PersonID}}"}]}' >/dev/null
api POST /people '{"person_id":"'"$PERSON"'","attributes":{"plan":"pro"}}' >/dev/null

# 2 ── event → campaign dispatched → enrollment row
api POST /events '{"person_id":"'"$PERSON"'","event_name":"smoke_signed_up","payload":{}}' | grep -q event_id \
    || fail "POST /events not accepted"
enrolled() { [ "$(sql "SELECT COUNT(*) FROM journey_enrollments WHERE campaign_id='smoke_welcome' AND person_id='$PERSON'")" = "1" ]; }
retry 30 "campaign enrollment for $PERSON" enrolled
check "event fanned out — campaign dispatched + enrollment recorded"

# 3 ── the journey run executes delay → send and completes, gated
run_done() { [ "$(sql "SELECT status FROM journey_runs WHERE journey_id='smoke_journey' AND person_id='$PERSON'")" = "done" ]; }
retry 45 "journey run for $PERSON reaches done" run_done
gates=$(sql "SELECT COUNT(*) FROM idempotency_keys WHERE workspace_id='$WS' AND status='sent' AND idem_key LIKE CONCAT((SELECT run_id FROM journey_runs WHERE journey_id='smoke_journey' AND person_id='$PERSON'), ':%')")
[ "$gates" = "1" ] || fail "expected 1 sent gate row for the run, got $gates"
check "journey run done (delay → send), send gated through idempotency_keys"

# 4 ── poison → DLQ via the confirmed dead-letter path, with why-headers
dlq_depth() { $COMPOSE exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null | awk '$1=="campaigns.fanout.dlq"{print $2}'; }
before=$(dlq_depth); before=${before:-0}
curl -fsS -u guest:guest -H "Content-Type: application/json" \
    -X POST http://localhost:15672/api/exchanges/%2f/campaigns.fanout/publish \
    -d '{"properties":{"delivery_mode":2,"message_id":"smoke-poison-'"$STAMP"'"},"routing_key":"'"$WS"'","payload":"smoke poison {{{","payload_encoding":"string"}' \
    | grep -q '"routed":true' || fail "poison publish not routed"
dlq_grew() { [ "$(dlq_depth)" -gt "$before" ]; }
retry 30 "poison on campaigns.fanout.dlq" dlq_grew
./chaos/dlq.sh inspect 2>/dev/null | grep -q "cause=terminal" || fail "DLQ message missing x-failure-cause header"
check "poison dead-lettered with reason ($before → $(dlq_depth))"

# 5 ── both RabbitMQ scrape jobs feed the dashboards
prom_has() {
    curl -fsS "$PROM/api/v1/query" --data-urlencode "query=$1" \
        | python3 -c "import sys,json; sys.exit(0 if json.load(sys.stdin)['data']['result'] else 1)"
}
retry 60 "node-level rabbitmq metrics" prom_has "rabbitmq_connections"
retry 60 "per-queue rabbitmq metrics" prom_has 'rabbitmq_detailed_queue_messages{queue="campaigns.fanout.dlq"}'
retry 60 "track-api RED metrics" prom_has 'http_server_request_duration_seconds_count{service_name="track-api"}'
check "Prometheus holds node-level + per-queue broker metrics and RED metrics"

# 6 ── nothing firing on a healthy stack (zero-traffic SLO regression)
firing=$(curl -fsS "$PROM/api/v1/query" --data-urlencode 'query=ALERTS{alertstate="firing"}')
echo "$firing" | grep -q '"result":\[\]' || fail "alerts firing on a healthy stack: $firing"
check "no firing alerts"

# 7 ── logs flowing Alloy → Loki under the promoted service label
loki_has() {
    curl -fsS -G "$LOKI/loki/api/v1/query_range" \
        --data-urlencode 'query={service_name="track-api"}' \
        --data-urlencode 'since=15m' | grep -q '"values":\[\['
}
retry 90 "track-api logs in Loki" loki_has
check "logs flowing through Alloy → Loki"

echo
echo "SMOKE PASS ($pass checks). Stack left running: make stop / make down."
