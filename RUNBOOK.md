# Runbook — Behavioral Messaging Platform

One runbook, scoped to the highest-blast-radius alert (the burn-rate
page on the track-api SLO). The slow-burn ticket shares the first three
diagnostic steps; mitigation urgency diverges.

---

## SLO

> 99% of `POST /events` requests return 2xx in ≤500 ms,
> measured over a rolling 28-day window.

Error budget = 1% of 28d ≈ **6h 43m** of allowable failure time per
window. The RED dashboard's "Error budget burned (28d)" stat reads
`slo:track_budget_burn_pct:28d`.

---

## Burn-rate page (`TrackBurnRatePage`)

Severity: **page**. Expected human response time: **<5 min**.

### What fired

The 1h **and** 5m windows of `slo:track_error_ratio` are both above
14.4×. At that burn the 28d budget is gone in **<2 days**. The
two-window AND filters single-blip noise; if it held for `for: 2m`,
it's real.

### First three checks (in order)

Open `http://localhost:3030/d/obs-red` and read top-to-bottom:

1. **HTTP request rate by service** — is `track-api` still receiving
   traffic? A drop to zero is upstream trouble, not an SLO problem
   (the ratio reads 0 on zero traffic — by construction, see the
   rules-file comment).
2. **HTTP error % (5xx)** — spiking on `track-api`? → Cause A or B.
3. **HTTP p95 latency** — above 500 ms with 2xx still flowing? The
   violation is the latency leg. → Cause B first.

### Likely root causes

This build *measured* its bottlenecks; the causes below are ranked by
that history, not by guesswork.

| # | Cause | Signal |
|---|---|---|
| **A** | **MySQL write ceiling** — the measured V2/V3 constraint. Every accepted event fans out to per-event MySQL writes; pool exhaustion backs up into handler latency. | USE dashboard MySQL row: `threads{kind="running"}` pinned; buffer-pool disk reads climbing; `make mysql` → `SHOW PROCESSLIST` full of waits. |
| **B** | **RabbitMQ unhealthy / flow control** — track-api's publisher buffers (1024) then **drops**: watch `campaign_publish_dropped_total{reason}`. Drops don't 5xx the handler, but broker back-pressure that reaches publish latency will. | `campaign_publish_dropped_total` rising; USE RabbitMQ row: memory/disk headroom near zero; mgmt UI `:15672` shows flow=true. |
| **C** | **Collector stalled** — masks rather than causes; verify the alert against raw access behavior before mitigating. | Collector container memory pressure; every service's RED panels flatline simultaneously while `make seed` still returns 202s. |

### Mitigation

Cause A (MySQL):
- `make mysql` → `SHOW PROCESSLIST;` — a rogue query gets `KILL <id>`.
- Genuine write load: lower `PREFETCH` / `SCHEDULER_CONCURRENCY` (env,
  `docker compose up -d` the service) to shed pressure; the queues
  absorb the backlog durably — that's what V2 bought.

Cause B (RabbitMQ):
- `docker compose restart rabbitmq` is safe: durable queues + manual
  acks + confirmed retry republishes mean no message loss; consumers
  crash-fast on the closed connection and the restart policy brings
  them back resubscribed (measured: <1 s).
- Check `make dlq-inspect` afterwards — anything the storm dead-lettered
  says why, and `make dlq-replay` re-drives it with attempts reset.

### Rollback

If the burn started right after a deploy:

```bash
git log --oneline main        # find the prior good commit
docker compose down           # stop (volumes survive)
git checkout <prior-sha>
docker compose up -d --build
```

After mitigation, **do not silence the alert**: the 5m window clears in
minutes, the 1h window needs the hour. It auto-resolves.

### Post-incident

Within 48h: write the 1-pager — bug, fix, what this runbook got right,
what it got wrong — and update this file.

---

## Burn-rate ticket (`TrackBurnRateTicket`)

Severity: **ticket**. Expected response: **same business day**.

Same first three checks. The burn is slower (6× over 6h+30m), so
investigate without paging anyone: correlate with recent deploys and
config changes (`git log` since the alert started), check whether a
retry-ladder backlog (`make retry-queues`) is feeding sustained latency,
and file the follow-up. At 6× there are ~5 days of budget headroom.
