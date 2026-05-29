#!/usr/bin/env bash
# ceiling-run.sh — one measured V2 ceiling run.
#
# Brackets a loadgen soak with the fan-out watcher (peak backlog + CPU)
# and an enrollment-count sampler (the worker's steady drain rate, read
# straight from MySQL — the authoritative count, immune to the OTel→Prom
# scrape lag). Confirms losslessness via enroll_delta == events_sent and
# the publish-drop counter.
#
# Usage: C=100 D=60 LABEL=p32 ./chaos/ceiling-run.sh
set -uo pipefail

C=${C:-100}
D=${D:-60}
LABEL=${LABEL:-run}
WORKSPACE=${WORKSPACE:-ws_alpha}
LOADGEN=${LOADGEN:-/tmp/loadgen}
OUT=/tmp/ceiling_${LABEL}

enroll() { docker compose exec -T mysql mysql -ubm -pbm bm -N -e 'SELECT count(*) FROM journey_enrollments;' 2>/dev/null; }
drops()  { curl -fsS -G 'http://localhost:9090/api/v1/query' --data-urlencode 'query=sum(campaign_publish_dropped_total)' 2>/dev/null | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(int(float(r[0]['value'][1])) if r else 0)"; }

echo "############ LEVEL $LABEL : c=$C d=${D}s ############"
EB=$(enroll); DB=$(drops)
echo "enroll_before=$EB publish_drops_before=$DB"

# watcher: real-time backlog + CPU, runs a bit past the soak to catch drain
DURATION=$((D+10)) WORKSPACE=$WORKSPACE INTERVAL=1 ./chaos/watch-fanout.sh > ${OUT}_watch.txt 2>&1 &
WPID=$!

# enrollment sampler: every 5s, "<unix_t> <count>" — slope = worker drain rate
( for i in $(seq 1 $((D/5 + 2))); do echo "$(date +%s) $(enroll)"; sleep 5; done ) > ${OUT}_enroll.txt 2>&1 &
SPID=$!

# the load
$LOADGEN -c "$C" -d "${D}s" -prep=false 2>&1 | grep -E 'events_sent|non_202|failures|achieved_rate|latency' | sed 's/^/  loadgen: /'

# wait for the durable queue to fully drain
for i in $(seq 1 60); do
    Q=$(curl -fsS -u guest:guest "http://localhost:15672/api/queues/%2F/campaigns.fanout.${WORKSPACE}" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('messages',0))" 2>/dev/null)
    [ "$Q" = "0" ] && break
    sleep 1
done
wait $SPID 2>/dev/null
wait $WPID 2>/dev/null
sleep 8  # let the OTel→Prom drop counter settle
EA=$(enroll); DA=$(drops)

echo "  enroll_after=$EA  enroll_delta=$((EA-EB))  publish_drops_delta=$((DA-DB))"
echo "  --- worker steady drain rate (enrollment slope, mid-window) ---"
awk 'NR>1{dt=$1-pt; dc=$2-pc; if(dt>0) printf "    +%5ds  %6d enrolled  =>  %5.0f events/s\n", $1-t0, dc, dc/dt} {if(NR==1)t0=$1; pt=$1; pc=$2}' ${OUT}_enroll.txt
echo "  --- watcher peaks ---"
grep -A7 '== peaks ==' ${OUT}_watch.txt | sed 's/^/    /'
