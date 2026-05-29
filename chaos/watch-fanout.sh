#!/usr/bin/env bash
# watch-fanout.sh — sample the V2 fan-out path live while loadgen runs.
#
# Prometheus counters arrive via OTel push + scrape and lag several
# seconds, so they're useless for catching a transient backlog. The
# RabbitMQ management API and `docker stats` are real-time — this poller
# uses those and reports the PEAK queue backlog + per-container CPU, which
# is what names the V2-1 bottleneck.
#
# Env: DURATION (default 60s), WORKSPACE (ws_alpha), INTERVAL (default 1s).
set -uo pipefail

DURATION=${DURATION:-60}
WORKSPACE=${WORKSPACE:-ws_alpha}
INTERVAL=${INTERVAL:-1}
RABBIT=${RABBIT:-http://localhost:15672}
QUEUE="campaigns.fanout.${WORKSPACE}"

peak_backlog=0
peak_ready=0
peak_api_cpu=0
peak_worker_cpu=0
peak_rabbit_cpu=0
peak_mysql_cpu=0

echo "== watch-fanout == queue=$QUEUE duration=${DURATION}s interval=${INTERVAL}s"
printf '%-8s %8s %8s %8s | %8s %8s %8s %8s\n' "t" "ready" "unacked" "backlog" "api%" "worker%" "rabbit%" "mysql%"

deadline=$(( $(date +%s) + DURATION ))
while [[ $(date +%s) -lt $deadline ]]; do
    now=$(date +%s)

    read -r ready unacked < <(
        curl -fsS -u guest:guest "${RABBIT}/api/queues/%2F/${QUEUE}" 2>/dev/null |
        python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('messages_ready',0),d.get('messages_unacknowledged',0))" 2>/dev/null || echo "0 0"
    )
    backlog=$(( ready + unacked ))

    # one docker stats sample for the three containers (blocks ~1.5s)
    stats=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' \
        behavioral-messaging-track-api-1 \
        behavioral-messaging-campaign-worker-1 \
        behavioral-messaging-rabbitmq-1 \
        behavioral-messaging-mysql-1 2>/dev/null)
    api_cpu=$(echo "$stats"    | awk '/track-api/      {gsub(/%/,"",$2); print $2}')
    worker_cpu=$(echo "$stats" | awk '/campaign-worker/{gsub(/%/,"",$2); print $2}')
    rabbit_cpu=$(echo "$stats" | awk '/rabbitmq/       {gsub(/%/,"",$2); print $2}')
    mysql_cpu=$(echo "$stats"  | awk '/mysql/          {gsub(/%/,"",$2); print $2}')

    (( backlog    > peak_backlog ))    && peak_backlog=$backlog
    (( ready      > peak_ready ))      && peak_ready=$ready
    awk "BEGIN{exit !(${api_cpu:-0}    > $peak_api_cpu)}"    && peak_api_cpu=${api_cpu:-0}
    awk "BEGIN{exit !(${worker_cpu:-0} > $peak_worker_cpu)}" && peak_worker_cpu=${worker_cpu:-0}
    awk "BEGIN{exit !(${rabbit_cpu:-0} > $peak_rabbit_cpu)}" && peak_rabbit_cpu=${rabbit_cpu:-0}
    awk "BEGIN{exit !(${mysql_cpu:-0}  > $peak_mysql_cpu)}"  && peak_mysql_cpu=${mysql_cpu:-0}

    printf '%-8s %8s %8s %8s | %8s %8s %8s %8s\n' \
        "$((now % 100000))" "$ready" "$unacked" "$backlog" \
        "${api_cpu:-?}" "${worker_cpu:-?}" "${rabbit_cpu:-?}" "${mysql_cpu:-?}"

    sleep "$INTERVAL"
done

echo
echo "== peaks =="
echo "peak_backlog (ready+unacked) = $peak_backlog"
echo "peak_ready                   = $peak_ready"
echo "peak_cpu track-api           = ${peak_api_cpu}%"
echo "peak_cpu campaign-worker     = ${peak_worker_cpu}%"
echo "peak_cpu rabbitmq            = ${peak_rabbit_cpu}%"
echo "peak_cpu mysql               = ${peak_mysql_cpu}%"
