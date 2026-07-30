# Behavioral Messaging Platform

A multi-tenant behavioral messaging platform — a product-shaped MVP
sized to fit a laptop. Event-driven by design: a person performs an
action, the platform stores the event, evaluates segment membership and
campaign triggers, and dispatches a per-person message via a stub
delivery worker.

The build is structured as **three scale tiers**, not the
make-it/break-it/fix-it pattern of Builds 1-5. Each tier makes one real
engineering decision and then measures what that decision cost or bought; the
artefact per tier is a bottleneck-migration writeup with hardware-pinned
numbers, including the tiers where the measurement contradicted the plan.

| | |
|--|--|
| **Spec** | [`spec.md`](./spec.md) — RFC-2119 V1 + V2 + V3 requirements |
| **Status** | **V1 + V2 + V3 complete** (spec 0.3.10). All three tiers built, measured, and written up — including one mechanism that measurement told us to delete. |
| **Stack** | Go 1.25 · MySQL 8.0 · RabbitMQ 3.13 · OpenTelemetry SDK + Collector 0.151 · Tempo 2.10 · Prometheus 3.11 · Loki 3.7 · Grafana Alloy v1.16 · Grafana 13.0 · `docker compose` |

## The tier ladder

| Tier | Mechanism | Measured | Where the bottleneck went |
|---|---|---|---|
| **V1** Foundation | Single process per service, in-process channel queue, scan segment eval, sync dispatch. Workspace-scoped from day 1. | intake 568/s, fan-out **196/s**, **69% silently dropped** | the single Dispatcher goroutine |
| **V2** Throughput | RabbitMQ durable fan-out + roaring-bitmap segments + batched idempotent enrollment | fan-out **~2,800/s** (~14×) with loss **structurally impossible**; backlogs 9,134 → 83 then 156,961 → 166 | **shared single-MySQL write throughput** — not render CPU or bitmap memory, which were the guesses going in |
| **V3** Durability | Persisted journey FSM (`SKIP LOCKED` claims + leases + version pinning) + retry ladder with a DLQ that fills | **1.9× throughput reduction against 3–5× predicted** — durability cost far less than feared | not established. The apparent ceiling was a scheduler executing batches *sequentially*; fixing that was **12×** end-to-end. See [§ V3 ceiling](#v3-ceiling-measured-2026-07-29) |

V3 is the one tier that *lowers* the ceiling on purpose — it spends throughput
to buy surviving a crash mid-journey. It also produced this build's most useful
result: a mechanism argued from first principles, measured twice, and deleted.
See [§ What the isolation tier taught](#what-the-isolation-tier-taught).

Multi-tenancy is **not** a tier — it's a cross-cutting V1 property
(`workspace_id` on every row, queue key, and span attribute).

## Run it

```bash
make up            # docker compose up -d --build (12 services)
make showcase      # every surface end-to-end: people → segment → campaign → events
make seed          # POST 10 events at 1 req/s
make seed-campaign # define welcome_pro + 2 people + fire signed_up — end-to-end smoke
make seed-journey  # V3-1a/b: define a journey, fire a trigger, redeliver it, watch it run
make journey-runs  # inspect journey_runs (step_index, status, wake_at)
make dlq-inspect   # V3-2: DLQ depth + why each message gave up
make dlq-replay    # V3-2: drain the DLQ back onto the work exchange
make load          # the ceiling-search run (= load-soak: 60s @ concurrency=20)
make load-quick    # 10s burst @ concurrency=5 — sanity-check the driver
make test          # go test ./...
make fmt vet       # gofmt + go vet
make mysql         # interactive mysql shell
make down          # tear it all down (compose down --volumes --remove-orphans)
```

Every tuning knob is a host env var, so the A/B runs behind the measurements
below are reproducible without editing compose:

```bash
PREFETCH=128 make up              # worker consumer concurrency
CAMPAIGN_HOT_PATH=mysql make up   # V2-2 A/B: per-event MySQL reads vs caches
CAMPAIGN_BATCH=off make up        # V2-3 A/B: one commit per enrollment
MYSQL_MAX_OPEN_CONNS=25 make up   # the pre-PR-12 pool size
```

> `make down` takes the volumes with it. That's deliberate — migration 004's
> `ALTER TABLE` only runs on an empty datadir, so a DB created before it would
> silently lack the UNIQUE key that makes enrollment idempotent.

## V1 ceiling on this hardware

> The headline scale number is "observed on this hardware," not absolute.
> Fabricated laptop numbers are transparent to anyone who has run load tests.
> The narrative is the **bottleneck-migration story**, not the throughput.

### Hardware

| | |
|---|---|
| **CPU** | Apple M4 Max — 14 cores (10 P + 4 E) |
| **RAM** | 36 GB |
| **OS**  | macOS 15.7.3 (build 24G419), aarch64 |
| **Container runtime** | Docker Engine 28.0.4, linux/arm64 |
| **Topology** | All services on one host (9 at the time of the V1 run; 12 from V2-2 on). `track-api` is one replica, default `GOMAXPROCS`, no resource limits. |

### Soak configuration

`./chaos/load-v1.sh` — closed-loop bash + curl driver. 20 background workers
each hot-loops `POST /events` for 60 seconds against a pool of 50
pre-identified persons (`plan=pro`). The `welcome_pro` campaign is defined
once at prep so every inbound `signed_up` event matches a trigger and the
Dispatcher actually fans out. No per-curl sleep — the system absorbs as
many requests as it can.

```
$ make load-soak
…
events_sent     = 34095
failures        = 0
elapsed_s       = 60
achieved_rate   = 568 events/s
```

### What the dashboards showed

| Signal | Value | Source |
|---|---|---|
| HTTP `POST /events` acceptance rate | **568 events/s** sustained, 0 failures | track-api response from the driver |
| `campaign_dispatched_total` over the run | **~196 events/s** steady-state | Prometheus counter, scoped to `workspace_id=ws_alpha,campaign_id=welcome_pro` |
| `campaign_queue_dropped_total` over the run | **~370 drops/s** steady-state | Prometheus counter |
| Drop ratio at the Dispatcher | **~69%** of accepted events never fanned out | derived |
| Producer:consumer throughput gap | **2.9×** | 568 / 196 |
| `journey_enrollments` rows written | matches `campaign_dispatched_total` exactly | end-of-run `make enrollments-count` |

### Named bottleneck: the single Dispatcher consumer goroutine

V1's Dispatcher is one goroutine reading from a 1024-deep buffered
channel. Per event it does — *serially* —

1. `MySQL: campaigns.ListByWorkspace(workspace_id)` (no cache)
2. `MySQL: people.Get(workspace_id, person_id)`
3. Decode trigger + evaluate against the inbound event
4. `text/template` render
5. `HTTP POST` to `stub-receiver` (sync, 5 s timeout)
6. `MySQL: INSERT INTO journey_enrollments`

Four DB hops + one outbound HTTP per event, single-threaded. Per-event
budget on this hardware lands at ~5 ms ⇒ ceiling near **200 events/s on
the fan-out path**. The intake path (handler + person ensure + events
insert + 202 to client) keeps up at 568 events/s — so the producer
outruns the consumer by ~2.9×, the 1024-deep buffer fills in roughly 3
seconds, and from that point the producer drops 69% of events to the
`campaign_queue_dropped_total` counter while the client keeps seeing 202s.

In production this is a silent dataloss bug. In V1 it's the deliberate
bottleneck V2 was always going to remove.

### V2-1 ceiling (measured 2026-05-29)

Re-measured on the same hardware with a new Go load driver
(`cmd/loadgen/`) — keep-alive connection reuse pushes ~6× past the bash
driver's ~557/s process-spawn ceiling, enough to actually stress the
stack. Fan-out sampled live with `chaos/watch-fanout.sh` (RabbitMQ mgmt
API + `docker stats`); the worker's drain rate is the `journey_enrollments`
slope, read straight from MySQL so the OTel→Prometheus scrape lag can't
distort it. Driver: 100 closed-loop goroutines, 60s, `welcome_pro`
matching every event.

`PREFETCH` is the worker's RabbitMQ consumer prefetch — how many
unacked deliveries the broker will hand one consumer at once, i.e. the
cap on events the worker processes concurrently. Each in-flight event
does the full fan-out (campaign lookup → trigger eval → render → stub
POST → enrollment insert), so prefetch is the worker's concurrency dial.

| `PREFETCH` | Worker fan-out rate | Peak queue backlog | Worker CPU | MySQL CPU | Loss |
|---|---|---|---|---|---|
| 32 (default) | ~1,675/s | 103,670 | 479% | 283% | 0 |
| 128 (before fixes) | collapse — 40s stall | 78,895 | **1,320%** | 312% | 0 |
| 128 (after fixes) | **~2,880/s** | 28,339 | 240% | 330% | 0 |
| 256 (after fixes) | ~2,750/s (no gain) | 17,683 | 211% | **342%** | 0 |

**V1 → V2-1 headline:** V1's single Dispatcher goroutine fanned out at
~196/s and **silently dropped ~69%** of accepted events. V2-1 makes loss
structurally impossible — the durable queue absorbs the
producer/consumer gap as a drainable backlog (`publish_dropped_total == 0`
in every run) — and after the fixes below the worker sustains **~2,800/s,
~14× the V1 fan-out rate**.

**New bottleneck:** per-event MySQL work (`ListByWorkspace` + `people.Get`
+ enrollment `INSERT`) on the single shared instance, bounded by the
worker's 25-connection pool. Past `PREFETCH=128`, more consumer
concurrency buys nothing — MySQL is the busiest dependency (~342%) while
the worker idles at ~211% CPU. V2-2 (roaring bitmaps + removing the
per-event `ListByWorkspace` scan) is positioned to lift exactly this.

#### What broke: two latent Go HTTP bugs

Naively bumping `PREFETCH` 32→128 to chase a higher number made
throughput *worse* and exposed two classic connection-leak bugs in the
worker's stub dispatch:

1. **Default transport pool** — the stub client wrapped
   `http.DefaultTransport` (`MaxIdleConnsPerHost = 2`), so under high
   prefetch almost every dispatch dialed a brand-new TCP connection.
2. **Undrained response body (the dominant one)** — the dispatch closed
   `resp.Body` without reading it to EOF. `net/http` only pools a
   connection when the body is fully drained *and* closed; closing an
   unread body discards the socket, defeating any pool sizing.

Together → ephemeral-port exhaustion (`connect: cannot assign requested
address`), ~73k failed dispatches, a nack-requeue storm, and a
13-of-14-core CPU burn while throughput collapsed. Fixing both (size the
transport pool to `PREFETCH`; `io.Copy(io.Discard, resp.Body)` before
close) dropped worker CPU 1,320%→240% and raised fan-out +70%
(1,675→2,880/s) with zero errors. This is the V2-1 "what broke and how I
fixed it" story.

#### What broke: the producer's shutdown race

Found at close-out, writing the first unit test for `Publisher`. `Submit` had
three `select` cases — shutdown (`<-p.done`), enqueue (`p.ch <- job`), and
`default` for a full buffer:

```go
select {
case <-p.done:                     // ready once Stop() closes done
    drop("shutdown")
case p.ch <- publishJob{...}:      // also ready — the buffer has room
    p.bufferDepth.Add(reqCtx, 1)
default:
    drop("buffer_full")
}
```

After `Stop()`, the first two cases are **both** ready, and Go picks uniformly
at random among ready cases. So roughly half of any event submitted during
shutdown was enqueued into a buffer nothing would ever drain — and counted as
neither drop reason. A silently dropped event, in the mechanism built
specifically to eliminate V1's silent drops.

The blast radius is small in practice: track-api drains HTTP before stopping
the publisher, so few submits arrive after `Stop`. But it's silent, and the fix
is to give shutdown its own `select` first so it has deterministic precedence.

The part worth keeping: **three PRs of load measurement could never have found
this.** Every ceiling run publishes into a running producer, so the shutdown
path was never on the measured path. "We measured it at scale" and "we tested
its edges" are different claims, and this build had only the first.

### V2-2 hot-path ceiling (measured 2026-05-29)

V2-2 moves the campaign-worker's per-event work off MySQL: campaign
definitions come from a TTL cache, person attributes from an in-process
cache fed by `people.changes` (+ boot backfill), so `Process` no longer
does `ListByWorkspace` + `person.Get` per event. The enrollment `INSERT`
(the audit write) stays. A/B on the same binary via `CAMPAIGN_HOT_PATH`,
`chaos/ceiling-run.sh`, c=100/200 @ 60s:

| Hot path | Intake | Worker fan-out | Peak backlog | Worker CPU | MySQL CPU |
|---|---|---|---|---|---|
| `mysql` (V1 reads, c=100) | 2,593/s | ~2,500/s, lagging | **9,134** | 224% | 324% |
| `cache` (c=100) | 2,662/s | keeps pace | **83** | 185% | 318% |
| `cache` (c=200) | 2,715/s (flat) | keeps pace | 507 | 145% | 306% |

**The fan-out worker is no longer the bottleneck.** Removing its two
per-event reads collapsed the queue backlog **9,134 → 83** — it now drains
as fast as intake delivers — and dropped its CPU (224% → 145%). Peak
throughput barely moved, and that's the finding: doubling concurrency
(c=100 → 200) doesn't raise it (2,662 → 2,715/s, latency doubles), while
worker (145%) and track-api (130%) both have CPU headroom and **MySQL is
pinned (~306%)**. The bottleneck migrated from the worker's per-event MySQL
**reads** to the **shared single MySQL's write throughput** — every event
is now one event `INSERT` (intake) + one enrollment `INSERT` (fan-out)
competing for one MySQL. The next lever is write-side: separate read/write
instances, batch enrollment inserts, or shard (V3/V4).

**Tradeoff (deliberate):** dropping the per-event `person.Get` trades
strong consistency (every event saw attributes as of that instant) for
**eventual** consistency — an event just after an attribute change may
evaluate the pre-change value until `people.changes` propagates. A cache
miss falls back to one `person.Get`, so a brand-new person is never
silently missed; only a very-recently-changed value is briefly stale. The
cold `/check` path is unchanged (still `backend=bitmap`).

### V2-3 write-side ceiling (measured 2026-05-31)

V2-2 left every event costing two MySQL inserts — one for the event (intake)
and one for the enrollment (fan-out) — with the enrollment paying a commit,
and therefore an fsync, each. V2-3 coalesces enrollments into one multi-row
commit per flush (`BATCH_MAX=64` rows or `BATCH_WINDOW=10ms`, whichever comes
first) and makes the insert idempotent so at-least-once redelivery re-inserts
as a no-op. `Submit` blocks until its batch commits, so the AMQP ack still
happens only after the audit row is durable.

A/B on the same binary via `CAMPAIGN_BATCH`, fresh DB, pool 50, prefetch 128,
c=200:

| Batching | Peak queue backlog | Rows per flush |
|---|---|---|
| `off` (one commit per enrollment) | **156,961** | 1 |
| `on` | **166** | ~63.5 (cap 64) |

**The write side stops being the laggard.** Zero duplicate enrollments across
700k+ rows. Absolute intake here (~5,750/s) is higher than PR 11's ~2,700/s
because this ran against a fresh database — small tables and indexes insert
faster — so the valid comparison is batching on-vs-off, not the absolute
number. The bottleneck stays the shared MySQL, now on the intake insert.

Before this, PR 12 raised the pool default 25 → 50 after finding requests
queuing on the pool *in front of* MySQL: 25 → 64 connections moved intake
**2,173 → 2,836/s** and p99 **306 → 197 ms**. The pool was throttling access
to the bottleneck, not the bottleneck itself.

## Surfaces

| URL | What |
|---|---|
| http://localhost:8090/healthz | `track-api` (V1) — `{"status":"ok"}` |
| `POST http://localhost:8090/people` | Identify / attribute upsert. `JSON_MERGE_PATCH` semantics — `null` value deletes the key. *(V1 PR 2)* |
| `GET  http://localhost:8090/people/<id>` | Read one person back. *(V1 PR 2)* |
| `POST http://localhost:8090/segments` | Define a segment with a JSON condition tree (`and`/`or`/`not` + `attr_eq`/`attr_exists`/`event_seen`). Depth cap 8. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>` | Read the stored definition. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>/check?person_id=<pid>` | Membership for one person. **V2-2: served from `segment-worker`'s roaring bitmaps** (no MySQL event scan); falls back to the V1 scan if the index is down. *(V1 PR 3 → V2 PR 10)* |
| http://localhost:8093/healthz | `segment-worker` (V2-2) — bitmap index; `POST /internal/check` membership API |
| `POST http://localhost:8090/campaigns` | Define a campaign — trigger (same condition tree as segments) + Go `text/template` body. Validates at insert time. *(V1 PR 4)* |
| `GET  http://localhost:8090/campaigns/<id>` | Read the stored campaign. *(V1 PR 4)* |
| `POST http://localhost:8090/journeys` | Define a multi-step journey — trigger + an ordered step list (`send` / `delay` / `branch_on_condition`, ≤ 20 steps). Branch targets are forward-only so every run terminates; `then:"end"` stops a run so branch arms don't fall through. *(V3-1a)* |
| `GET  http://localhost:8090/journeys/<id>` | Read the stored definition + current version. *(V3-1a)* |
| `PUT  http://localhost:8090/journeys/<id>` | Publish a new version. In-flight runs keep executing the version they enrolled on. *(V3-1c)* |
| `GET  http://localhost:8090/journeys/<id>/runs/<person_id>` | One person's position in a journey — step index, status, `wake_at`. *(V3-1a)* |
| http://localhost:8094/healthz | `journey-scheduler` (V3-1b) — the only component that executes journey steps: `SKIP LOCKED` claims, leases, gated sends |
| http://localhost:15672 (guest/guest) | RabbitMQ management UI — `campaigns.fanout` exchange, `campaigns.fanout.dlx` DLX, `campaigns.fanout.dlq` DLQ declared at track-api startup. *(V2 PR 6)* |
| http://localhost:8091/healthz | `stub-receiver` (V1) — receives the rendered template per dispatched campaign |
| http://localhost:3030/explore | Grafana — Tempo / Prometheus / Loki datasources provisioned. Try `{resource.service.name="track-api"}` in Tempo Search. |
| http://localhost:3030/d/obs-red | RED dashboard (carried from Build 5). V1 panels populate once traffic flows. |
| http://localhost:3030/d/obs-use | USE dashboard (Build 5 carryover), MySQL row rewritten for this build. |
| http://localhost:9090/alerts | Prometheus alert rules — `TrackBurnRatePage` / `TrackBurnRateTicket`. |
| http://localhost:9090/targets | Scrape health — `otel-collector` (app metrics) + `rabbitmq` (per-queue). |
| `mysql -h 127.0.0.1 -P 3307 -ubm -pbm bm` | MySQL directly |

## One event, one trace

`POST /events` produces a single connected trace spanning three services, the
broker, and MySQL. Verified shape (Tempo, `{resource.service.name="track-api"}`):

```
track-api.http                                track-api
  sql.stmt.exec            INSERT IGNORE INTO people …     ← person ensure (BM-29)
  sql.stmt.exec            INSERT INTO events …            ← the intake write
  amqp.publish campaigns.fanout
    amqp.consume campaigns.fanout.ws_alpha    campaign-worker
      campaign.process
        sql.stmt.query     SELECT … FROM campaigns …       ← only on cache miss / TTL refresh
        sql.stmt.query     SELECT … FROM people …
        HTTP POST
          stub-receiver.http                  stub-receiver
    amqp.consume segments.index.ws_alpha      segment-worker
```

It crosses the broker via W3C traceparent on the AMQP headers, and MySQL via
`otelsql`. That last part landed late — the DB spans were billed as "a small
follow-up commit" at V1 kickoff and stayed missing for the whole build, which
meant the two writes every tier measurement blames for the ceiling were the
only part of the system you couldn't see in a trace.

**The batched enrollment write is the interesting exception.** A flush commits
rows from many events at once, so it can't be a child of any single request —
claiming otherwise would attribute one trace's latency to another. It emits its
own span, linked to every event that contributed a row:

```
campaign.enroll_flush        enroll.batch_rows=64   links→ 64 contributing traces
  sql.stmt.exec              INSERT INTO journey_enrollments … (one multi-row statement)
```

So the enrollment INSERT is reachable from each request's trace without
pretending to belong to it. Span links exist for exactly this shape, and
batching is what creates it.

## Watching the queue

Prometheus scrapes the app metrics via the OTel collector, and RabbitMQ
directly for per-queue state. The queues are named
`campaigns.fanout.<workspace_id>`, so `workspace_id` is relabeled out of the
queue name — which is what makes per-workspace admission (BM‑82) actually
observable rather than merely implemented.

```promql
# backlog per workspace — the number every tier measurement in this README turns on
rabbitmq_detailed_queue_messages_ready{workspace_id="ws_alpha"}

# publish vs drain rate: if the first outruns the second, the backlog is growing
rate(rabbitmq_detailed_queue_messages_published_total{workspace_id="ws_alpha"}[1m])
rate(rabbitmq_detailed_queue_messages_delivered_total{workspace_id="ws_alpha"}[1m])

# fan-out actually completing (app-side, per campaign)
sum by (campaign_id) (rate(campaign_dispatched_total[1m]))

# producer health: these should be flat zero. buffer_full or shutdown means loss.
sum by (reason) (campaign_publish_dropped_total)
campaign_publish_buffer_depth

# poison-message watch: requeue is unbounded until V3-2 (Q13). A sustained
# redelivered=true rate with flat dispatch throughput is a message spinning —
# the DLQ stays empty in that case, so it can't be your only signal.
sum by (redelivered) (rate(messaging_consume_requeued_total[1m]))
rate(messaging_consume_dropped_total[1m])          # terminal → DLQ

# enrollment batching effectiveness (V2-3): should sit near BATCH_MAX under load
histogram_quantile(0.5, sum by (le) (rate(campaign_enroll_batch_rows_bucket[1m])))

# pool pressure (otelsql). PR 12 raised the pool after inferring that requests
# queued in front of MySQL; wait_count makes that directly visible now.
rate(db_client_connections_wait_count_total[1m])
db_client_connections_usage
```

`chaos/watch-fanout.sh` samples the same backlog live from RabbitMQ's
management API — it predates the scrape job and is still the quicker read
during a ceiling run.

## Host port matrix

| Service | Port | Notes |
|---|---|---|
| `track-api` | `:8090` | container `:8080`; host `8080` squatted |
| `stub-receiver` | `:8091` | container `:8081` |
| `campaign-worker` | `:8092` | container `:8082`; tiny `/healthz` only |
| `segment-worker` | `:8093` | container `:8083`; `/healthz` + `POST /internal/check` (V2-2) |
| `journey-scheduler` | `:8094` | container `:8084`; `/healthz` only — advances journey runs (V3-1b) |
| MySQL | `:3307` | container `:3306`; host `:3306` left for any local MySQL |
| RabbitMQ | `:5672` (AMQP), `:15672` (mgmt), `:15692` (Prom) | guest/guest creds; mgmt UI in browser |
| OTel collector | `:4317` | OTLP/gRPC; Prom scrape `:8889` |
| Tempo | `:3200` | |
| Prometheus | `:9090` | |
| Loki | `:3100` | |
| Grafana | `:3030` | container `:3000`; host `:3000` squatted |

## Project layout

```
behavioral-messaging/
├── spec.md, README.md, Makefile, docker-compose.yml, Dockerfile
├── go.mod / go.sum
├── cmd/                            (5 binaries, all built; V3's journey-scheduler not yet written)
│   ├── track-api/                  HTTP intake. Producer-only since V2-1b — publishes to RabbitMQ, never dispatches
│   ├── campaign-worker/            V2 PR 7 — consumes per-workspace queues, runs the per-event Processor
│   ├── segment-worker/             V2-2 — bitmap index: boot backfill + stream maintenance + /internal/check
│   ├── stub-receiver/              V1 delivery sink
│   ├── journey-scheduler/          V3-1b — claims due runs (SKIP LOCKED + leases), executes steps, gates sends
│   └── loadgen/                    Go load driver for the ceiling runs (replaced the bash driver in PR 8)
├── internal/
│   ├── event/                      shared Event struct
│   ├── eventstore/                 MySQL persistence + pool sizing + otelsql instrumentation (every query is a
│   │                               span; pool utilization as db_client_connections_* metrics)
│   ├── otelinit/                   Build 5 carryover — TracerProvider + MeterProvider
│   ├── logsx/                      Build 5 carryover — slog JSON + trace_id/span_id
│   ├── amqpx/                      V2 PR 6 — thin amqp091 wrapper (Connect/Publish/Consume + traceparent propagation)
│   ├── person/                     V1.x — person store
│   ├── segment/                    V1.x — condition tree parser + scan evaluator
│   ├── segmentidx/                 V2-2 — roaring bitmap engine (Observe* maintenance + Member)
│   ├── peoplefeed/                 V2-2 — people.changes attribute-stream exchange + publisher
│   └── campaign/                   trigger + template + fan-out. processor.go (per-event body, returns
│                                   amqpx.Outcome) · publisher.go (RabbitMQ producer, goroutine + bounded
│                                   retry) · topology.go (exchange/DLX/DLQ + per-workspace queue helper) ·
│                                   attrcache.go / campaigncache.go (V2-2c hot-path caches, and where the
│                                   trigger/template compile is memoized) · batcher.go (V2-3 batched
│                                   idempotent enrollment inserts)
├── deploy/                         Build 5 carryover — collector/tempo/prom/loki/alloy/grafana
│   ├── prometheus.yml              scrapes the collector + RabbitMQ per-queue (workspace_id relabeled out
│   │                               of the queue name — BM-82)
│   └── grafana/provisioning/dashboards/ — red.json + use.json
├── migrations/                     001_v1.sql (workspaces, people, events, journey_enrollments, idempotency_keys) · 002_segments.sql · 003_campaigns.sql · 004_idempotency.sql (unique enrollment key)
└── chaos/                          load-v1.sh (bash V1 driver) · watch-fanout.sh (live backlog+CPU sampler) ·
                                    ceiling-run.sh (V2 measured-run orchestrator; pairs with cmd/loadgen/)
```

## V3 ceiling (measured 2026-07-29)

Both arms on a fresh database, `ws_alpha`, 2 campaigns, a single-`send` journey,
loadgen c=100/30 s, tables truncated between arms. The baseline's validity was
confirmed behaviourally — `journey_runs = 0` with `JOURNEYS=off` — after an
earlier attempt was invalidated by a toggle that wasn't wired through compose.

> The V2 figure below is **re-measured for this comparison** and is not the
> ~2,800/s headline: different database size, campaign count, and pool. Only the
> two rows here are comparable to each other.

| | fan-out (worker) | end-to-end (runs `done`) |
|---|---|---|
| **V2** (`JOURNEYS=off`) | **956 events/s** | — |
| **V3**, first attempt | 637 events/s | **42 runs/s** |
| **V3**, after the fix | **762 events/s** | **503 runs/s** |

The spec predicted a **3–5× reduction** from write amplification, with MySQL
writes as the new bottleneck. Both halves were wrong, and the interesting part is
*how*.

**The first measurement showed ~20× — and that was a bug, not the
architecture.** The scheduler claimed batches correctly but executed them
`for i := range runs { executeRun(...) }`: one run at a time, in a single
goroutine, each doing a person read, a gate insert, an HTTP POST and two updates.
About 24 ms serially, which is exactly the 42/s observed. The *claim* was batched
and `SKIP LOCKED`-safe; the *execution* had no concurrency at all.

Bounded concurrency over the claimed batch — the same shape the fan-out worker
learned in V2-1, and always safe because each run is a distinct row already
leased to this replica — took end-to-end from **42 → 503 runs/s, a 12×
improvement**.

**Final verdict: a 1.9× reduction** (956 → 503), not 3–5×. Durability cost
materially less than the spec feared. And 27,664 of 41,870 runs had finished when
the window closed, so 503/s is a floor.

**What the bottleneck actually is: still unestablished.** Neither arm was driven
to saturation after the fix, and MySQL was never shown to be the constraint — so
spec Q17 ("where does high-churn run state belong?") stays premature. This design
hasn't earned the right to blame the database.

## What the isolation tier taught

V3-3 set out to prove tenant isolation: flood one workspace, show another stays
served. Two runs, two negative results, and the second one is the interesting
one.

**First run (bad measurement).** Caps on vs off, measured by the quiet tenant's
gateway latency. Both "passed" — which should have been the tell. The bash+curl
driver tops out near 557/s (this repo measured that in V1), so it never
saturated the 50-connection pool and the contention the caps arbitrate never
happened. The caps only added latency.

**Second run (bad assumption).** Fixed both flaws — drove the noisy tenant with
`cmd/loadgen` at c=100, measured the quiet tenant's *queue depth* rather than
intake latency, and shrank the pool to 16 so saturation was reachable:

| | noisy max queue | **quiet max queue** | quiet p99 |
|---|---|---|---|
| caps **ON** | 167,461 | **1** | 27 ms |
| caps **OFF** | 306,941 | **0** | 28 ms |

The quiet tenant was untouched either way, 0/25 samples backed up in both arms.

**Why: the isolation was already there, from V2-1.** Every workspace has its own
queue and its own consumer, and `Qos(global=false)` gives each consumer its own
prefetch. A noisy tenant's backlog accumulates in *its own* queue while its
consumer holds at most `PREFETCH` unacked; a quiet tenant needing two
connections a second gets them trivially, because the noisy work *completes* and
returns connections rather than holding them. There is no starvation path to
close.

So V3-3's admission caps were **not justified as a fairness mechanism**. That
left one hypothesis: maybe they bound *aggregate* concurrency usefully — N
workspaces × prefetch goroutines thrashing one pool.

**Third run (BM‑135), and the caps lost.** 8 simultaneously busy workspaces,
c=25 each (≈200 aggregate concurrency), pool of 16, identical 41-second windows:

| | enrollments | aggregate fan-out |
|---|---|---|
| caps **OFF** | 220,960 | **5,389/s** |
| caps **ON** | 31,548 | **769/s** |

A **7× throughput loss for no measured benefit.** The premise was wrong in the
opposite direction from expected: MySQL and the connection pool handle
oversubscription gracefully — requests queue briefly and complete — whereas a
hard in-flight cap of 12 limits the worker to 12 concurrent however much
capacity is free.

**So the caps were deleted.** `internal/campaign/limiter.go`, its wiring, and its
compose knobs are gone. The spec keeps BM‑130 and BM‑135 as *withdrawn*
requirements rather than deleting them, because the reasoning is the artefact: a
plausible mechanism, argued from first principles, killed by measurement.

**What survives from V3-3:** the fair scheduler claim. `ORDER BY wake_at LIMIT n`
genuinely did let one tenant's overdue backlog fill every batch forever — that
gap was real, and it's closed.

The tier ladder's premise — each tier raises the ceiling by making a real
engineering decision — only holds if a tier is allowed to conclude that its
decision wasn't needed.

## Why these decisions

**MySQL, not Postgres.** Deliberately the less-familiar engine of the two:
prior builds in this series used Postgres, so the transferable knowledge was
already banked and the gap worth closing was MySQL's — JSON columns without
`jsonb`, `ON DUPLICATE KEY UPDATE` instead of `ON CONFLICT`, and
`FOR UPDATE SKIP LOCKED` semantics that differ in the details.

**Workspace-scoped from V1.** Multi-tenancy is a cross-cutting property
(`workspace_id` on every row, queue key, span attribute) — not its own
tier. Otherwise V3 becomes a rewrite of every storage path, not a delta.

**In-process channel queue in V1, not RabbitMQ.** V1 doesn't have
multi-stage fan-out, so a broker buys nothing — adds infra, hides the
single-process ceiling V2 needs to point at. RabbitMQ arrives when
V2's campaign fan-out demands it.

**Bottleneck-migration narratives, not absolute throughput numbers.**
"V1 ceiling = single-writer MySQL contention at ~N writes/sec on this
hardware; V2 changes Y, new ceiling is Z" reads honest. Fabricated
laptop numbers are transparent to anyone who has run load tests.

**Drop-in Build 5 observability scaffold.** No reinventing OTel, dashboards,
or log shipping. Day-1 dashboards.

## What's next

V1–V3 are complete. The open threads, in the order I'd take them:

- **Find V3's real ceiling.** BM‑140 fixed the scheduler bottleneck but neither
  arm was then driven to saturation, so the constraint after the fix is
  unidentified. Until it is, spec Q17 ("where does high-churn run state
  belong?") stays premature — this design hasn't earned the right to blame the
  database.
- **Resolve Q16**, the one genuinely hard open question: a send-gate row stuck in
  `sending` means the process died between the gate and the POST, and there is no
  way to know whether the downstream received it. Retry risks a double-send,
  failing risks a silent non-send. Needs a real sender to decide against, and the
  answer differs per message class.
- **Journey re-entry policy** (Q15) and **exit/suppression** (Q18) — both
  deliberately outside the three-step-type cap, both needed by anything real.
- **V4 territory:** read/write split or sharding by `workspace_id`; per-workspace
  send quotas on the seam the deleted caps left behind.

Release history with per-tag detail lives in [spec §7](./spec.md#7-revision-history),
which is the canonical record — it was duplicated here and drifted.

## Reading

- `RoaringBitmap/roaring` — the compressed-bitmap library behind the V2-2 segment index
- Martin Kleppmann, *DDIA* ch. 11 (stream processing)
- Google SRE workbook ch. 5 (multi-window multi-burn-rate alerts) — carried over from Build 5
