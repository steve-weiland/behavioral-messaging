#!/usr/bin/env bash
# V3-2 DLQ operations (BM-124). A DLQ nobody can drain is a landfill with a
# dashboard, so inspecting and replaying it are first-class commands.
#
#   ./chaos/dlq.sh inspect   depth per queue + why each message gave up
#   ./chaos/dlq.sh replay    republish everything to the work exchange,
#                            x-attempt reset so it gets a full fresh ladder
#   ./chaos/dlq.sh purge     drop the contents (destructive)
#
# Uses the RabbitMQ management API rather than an AMQP client so it stays a
# shell tool with no build step.
set -euo pipefail

RABBIT="${RABBIT:-http://localhost:15672}"
CREDS="${CREDS:-guest:guest}"
DLQ="${DLQ:-campaigns.fanout.dlq}"
EXCHANGE="${EXCHANGE:-campaigns.fanout}"
VHOST="%2f"

api() { curl -fsS -u "$CREDS" "$@"; }

cmd_inspect() {
  echo "queue depths:"
  api "$RABBIT/api/queues/$VHOST" | python3 -c '
import sys, json
qs = json.load(sys.stdin)
rows = [(q["name"], q.get("messages", 0)) for q in qs
        if "dlq" in q["name"] or "retry" in q["name"]]
for n, m in sorted(rows):
    print(f"  {n:28} {m}")
if not rows:
    print("  (no dlq/retry queues declared)")
'
  echo "dead-lettered messages (peeked, left in place):"
  api -H 'Content-Type: application/json' \
      -X POST "$RABBIT/api/queues/$VHOST/$DLQ/get" \
      -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' \
    | python3 -c '
import sys, json
ms = json.load(sys.stdin)
if not ms:
    print("  (empty)")
for m in ms:
    h = m.get("properties", {}).get("headers", {}) or {}
    cause = h.get("x-failure-cause", "?")
    att = h.get("x-attempt", 0)
    when = h.get("x-dead-lettered-at", "?")
    reason = str(h.get("x-failure-reason", ""))[:80]
    print(f"  cause={cause:<10} attempts={att} at={when}")
    print(f"    reason: {reason}")
'
}

cmd_replay() {
  # ack_requeue_false removes each message as we take it, so a replay can't
  # loop forever on its own output.
  local msgs
  msgs=$(api -H 'Content-Type: application/json' \
      -X POST "$RABBIT/api/queues/$VHOST/$DLQ/get" \
      -d '{"count":1000,"ackmode":"ack_requeue_false","encoding":"auto"}')

  echo "$msgs" | RABBIT="$RABBIT" CREDS="$CREDS" EXCHANGE="$EXCHANGE" python3 -c '
import sys, json, os, base64, urllib.request

msgs = json.load(sys.stdin)
if not msgs:
    print("  DLQ empty — nothing to replay")
    sys.exit(0)

rabbit = os.environ["RABBIT"]
exchange = os.environ["EXCHANGE"]
auth = base64.b64encode(os.environ["CREDS"].encode()).decode()

def publish(routing_key, payload):
    body = json.dumps({
        # x-attempt deliberately omitted: a replay is a fresh decision, so the
        # message gets the whole ladder again rather than dying on first touch.
        "properties": {"delivery_mode": 2, "headers": {}},
        "routing_key": routing_key,
        "payload": payload,
        "payload_encoding": "string",
    }).encode()
    req = urllib.request.Request(
        f"{rabbit}/api/exchanges/%2f/{exchange}/publish",
        data=body,
        headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
        method="POST",
    )
    return json.load(urllib.request.urlopen(req)).get("routed", False)

replayed = unrouted = 0
for m in msgs:
    h = m.get("properties", {}).get("headers", {}) or {}
    # The DLX is a fanout, so the original routing key is only recoverable from
    # the header we set when dead-lettering.
    rk = h.get("x-original-routing-key") or ""
    if not rk:
        print("  WARNING: message has no x-original-routing-key — skipping")
        unrouted += 1
        continue
    if publish(rk, m["payload"]):
        replayed += 1
    else:
        print(f"  WARNING: publish to {rk} was not routed (no queue bound?)")
        unrouted += 1

print(f"  replayed {replayed} message(s) to {exchange}; {unrouted} skipped/unrouted")
if unrouted:
    sys.exit(1)
'
}

cmd_purge() {
  api -X DELETE "$RABBIT/api/queues/$VHOST/$DLQ/contents"
  echo "  purged $DLQ"
}

case "${1:-inspect}" in
  inspect) cmd_inspect ;;
  replay)  cmd_replay ;;
  purge)   cmd_purge ;;
  *) echo "usage: $0 {inspect|replay|purge}" >&2; exit 2 ;;
esac
