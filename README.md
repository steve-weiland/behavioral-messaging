# Behavioral Messaging Platform

A multi-tenant behavioral messaging platform — Customer.io-inspired MVP
sized to fit a laptop. Event-driven by design: a person performs an
action, the platform stores the event, evaluates segment membership and
campaign triggers, and dispatches a per-person message via a stub
delivery worker.

The build is structured as **three scale tiers**, not the
make-it/break-it/fix-it pattern of Builds 1-5. Each tier raises the
architectural ceiling by making a real engineering decision; the
artefact per tier is the "what broke at the old ceiling, what we
changed, what now breaks" writeup with hardware-pinned measurements.

| | |
|--|--|
| **Spec** | [`spec.md`](./spec.md) — RFC-2119 V1 + V2 + V3 requirements |
| **Status** | `v0.2.0-throughput` + V2-2 in progress. V1 foundation + V2-1 RabbitMQ async fan-out live (ceiling **~2,800 events/s, zero loss (~14× V1)** — see [§ V2-1 ceiling](#v2-1-ceiling-measured-2026-05-29)). V2-2: roaring-bitmap `segment-worker` serves `/check` (no MySQL scan), and the campaign-worker hot path now reads campaigns + attributes from in-process caches — the fan-out worker is no longer the bottleneck (backlog 9,134 → 83), which **migrated to shared MySQL write throughput** ([§ V2-2 ceiling](#v2-2-hot-path-ceiling-measured-2026-05-29)). |
| **Stack** | Go 1.25 · MySQL 8.0 · OpenTelemetry SDK + Collector 0.151 · Tempo 2.10 · Prometheus 3.11 · Loki 3.7 · Grafana Alloy v1.16 · Grafana 13.0 · `docker compose` |

---

## The Tier Ladder

| Tier | Mechanism shift | Bottleneck removed → new bottleneck |
|---|---|---|
| **V1 Foundation** | Workspace-scoped from day 1. Single-process per service, single MySQL, in-process channel queue, scan-based segment eval, sync stub dispatch. | (baseline — sets the first ceiling) |
| **V2 Throughput** | Roaring bitmaps for segments + RabbitMQ async fan-out + idempotency keys + two-phase dispatch. | Render CPU / bitmap memory per workspace. |
| **V3 Durability** | Per-person journey state machine with delays (persisted FSM, SKIP-LOCKED scheduler) + DLQs + retry + per-workspace queue isolation. | Cross-workspace coordination overhead (V4 sharding territory). |

Multi-tenancy is **not** a tier — it's a cross-cutting V1 property
(`workspace_id` on every row, queue key, and span attribute).

## Run it

```bash
make up          # docker compose up -d --build (9 services in V1)
make seed        # POST 10 events at 1 req/s
make seed-campaign # define welcome_pro + 2 people + fire signed_up — end-to-end smoke
make load-quick  # 10s burst @ concurrency=5 — sanity-check the driver
make load-soak   # 60s steady @ concurrency=20 — the V1 ceiling-search run
make test        # go test ./...
make mysql       # interactive mysql shell
make down        # tear it all down (compose down --volumes --remove-orphans)
```

## V1 ceiling on this hardware

> The headline scale number is "observed on this hardware," not absolute.
> A Customer.io reviewer sees through fabricated laptop numbers immediately.
> The narrative is the **bottleneck-migration story**, not the throughput.

### Hardware

| | |
|---|---|
| **CPU** | Apple M4 Max — 14 cores (10 P + 4 E) |
| **RAM** | 36 GB |
| **OS**  | macOS 15.7.3 (build 24G419), aarch64 |
| **Container runtime** | Docker Engine 28.0.4, linux/arm64 |
| **Topology** | All 9 services on one host. `track-api` is one replica, default `GOMAXPROCS`, no resource limits. |

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

### What V2 changes

| V2 PR | Mechanism | Bottleneck removed |
|---|---|---|
| **V2-1** | RabbitMQ async fan-out + parallel render workers + per-workspace queue admission | Single consumer goroutine (this section). Replaces the 1024-deep in-process channel with durable, persisted queues; replaces the lone consumer with N workers. |
| **V2-2** | Roaring bitmaps with incremental segment maintenance (`RoaringBitmap/roaring` upstream) | Per-event `ListByWorkspace` scan + the scan-based segment evaluator (BM‑33, BM‑36). |
| **V2-3** | Idempotency keys + two-phase dispatch (`sending` → ack → `sent`) | Audit-failure-doesn't-reverse-dispatch gap (BM‑78). Retries become safe. |

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
| http://localhost:15672 (guest/guest) | RabbitMQ management UI — `campaigns.fanout` exchange, `campaigns.fanout.dlx` DLX, `campaigns.fanout.dlq` DLQ declared at track-api startup. *(V2 PR 6)* |
| http://localhost:8091/healthz | `stub-receiver` (V1) — receives the rendered template per dispatched campaign |
| http://localhost:3030/explore | Grafana — Tempo / Prometheus / Loki datasources provisioned. Try `{resource.service.name="track-api"}` in Tempo Search. |
| http://localhost:3030/d/obs-red | RED dashboard (carried from Build 5). V1 panels populate once traffic flows. |
| http://localhost:3030/d/obs-use | USE dashboard (Build 5 carryover). MySQL row needs queries rewritten in a follow-up commit; host + RabbitMQ rows from Build 5 will show partial data only (no RabbitMQ in V1). |
| http://localhost:9090/alerts | Prometheus alert rules — `TrackBurnRatePage` / `TrackBurnRateTicket`. |
| `mysql -h 127.0.0.1 -P 3307 -ubm -pbm bm` | MySQL directly |

## Host port matrix

| Service | Port | Notes |
|---|---|---|
| `track-api` | `:8090` | container `:8080`; host `8080` squatted |
| `stub-receiver` | `:8091` | container `:8081` |
| `campaign-worker` | `:8092` | container `:8082`; tiny `/healthz` only |
| `segment-worker` | `:8093` | container `:8083`; `/healthz` + `POST /internal/check` (V2-2) |
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
├── cmd/
│   ├── track-api/                  V1 → V2 HTTP intake (now a producer-only — publishes to RabbitMQ)
│   ├── campaign-worker/            V2 PR 7 — consumes per-workspace queues, runs the per-event Processor
│   ├── stub-receiver/              V1 delivery sink
│   ├── campaign-worker/            V1.x — campaign trigger + render (added next PR)
│   ├── segment-worker/             V2-2 — bitmap index: boot backfill + stream maintenance + /internal/check
│   ├── journey-scheduler/          V3 — SKIP-LOCKED scheduler
│   └── loadgen/                    V1.x — Go load driver for scale ceiling runs
├── internal/
│   ├── event/                      shared Event struct
│   ├── eventstore/                 MySQL persistence (sql.Open in V1, otelsql follow-up)
│   ├── otelinit/                   Build 5 carryover — TracerProvider + MeterProvider
│   ├── logsx/                      Build 5 carryover — slog JSON + trace_id/span_id
│   ├── amqpx/                      V2 PR 6 — thin amqp091 wrapper (Connect/Publish/Consume + traceparent propagation)
│   ├── person/                     V1.x — person store
│   ├── segment/                    V1.x — condition tree parser + scan evaluator
│   ├── segmentidx/                 V2-2 — roaring bitmap engine (Observe* maintenance + Member)
│   ├── peoplefeed/                 V2-2 — people.changes attribute-stream exchange + publisher
│   ├── campaign/                   V1 PR 4 → V2 PR 7 — trigger + template + processor.go (per-event fan-out
│   │                               body, returns amqpx.Outcome) + publisher.go (RabbitMQ producer with
│   │                               goroutine + bounded retry) + topology.go (exchange/DLX/DLQ + per-workspace queue helper)
│   └── journey/                    V3 — FSM with delays
├── deploy/                         Build 5 carryover — collector/tempo/prom/loki/alloy/grafana
│   └── grafana/provisioning/dashboards/ — red.json + use.json (panels TBD-rewritten for MySQL)
├── migrations/                     001_v1.sql (workspaces, people, events, journey_enrollments, idempotency_keys) · 002_segments.sql · 003_campaigns.sql
└── chaos/                          load-v1.sh (bash V1 driver) · watch-fanout.sh (live backlog+CPU sampler) ·
                                    ceiling-run.sh (V2 measured-run orchestrator; pairs with cmd/loadgen/)
```

## Why these decisions

**MySQL, not Postgres.** Customer.io stack alignment + fills the CV gap
explicitly called out in the role evaluation. Postgres depth from prior
builds transfers.

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
laptop numbers read as fabricated to a Customer.io reviewer.

**Drop-in Build 5 observability scaffold.** No reinventing OTel, dashboards,
or log shipping. Day-1 dashboards.

## What's next

| Version | Theme | Scope |
|---|---|---|
| `v0.1.0-foundation` | V1 PR 1 | Scaffold + Track API + stub receiver + MySQL + observability drop-in |
| `v0.1.x` | V1 PRs 2-4 | Person store · segment evaluation (scan) · campaign trigger + in-process Dispatcher |
| `v0.1.5-ceiling` | V1 PR 5 (tagged) | Load driver + V1 ceiling writeup (see [§ V1 ceiling](#v1-ceiling-on-this-hardware)). Bottleneck named: single Dispatcher consumer, 2.9× producer-consumer gap, ~69% drop rate at the soak rate. |
| `v0.1.6-amqp` | V2 PR 6 | RabbitMQ infra + `internal/amqpx/` + `campaigns.fanout` topology declared at track-api startup. Dispatch path unchanged. |
| `v0.1.7-fanout` | V2 PR 7 (this commit) | Atomic switch — `Dispatcher` retired, `Publisher` publishes per accepted event to `campaigns.fanout` (routing_key=workspace_id), new `cmd/campaign-worker/` consumes per-workspace queues with manual ack + DLQ on terminal nack. End-to-end trace continuity verified. 10 s burst @ c=5 = 345 events/s with 0 publish drops, 0 DLQ messages, queue depth = 0. |
| `v0.2.0-throughput` | V2 PR 8 (tagged) | V2-1 ceiling re-measured on the same hardware — fan-out **~2,800/s, zero loss (~14× V1's 196/s)**, new bottleneck named (per-event MySQL over the 25-conn pool). New Go `cmd/loadgen/` + `chaos/watch-fanout.sh` + `chaos/ceiling-run.sh`. Found + fixed two Go HTTP connection-leak bugs that were masking the ceiling. See [§ V2-1 ceiling](#v2-1-ceiling-measured-2026-05-29). |
| `v0.2.x-segments` | V2 PRs 9–11 | V2-2. PR 9: `internal/segmentidx` engine (9 tests). PR 10: `cmd/segment-worker/` + `/check` repoint + `people.changes` feed. PR 11: campaign-worker hot-path repoint — campaign + attribute caches replace the per-event MySQL reads. Re-measured: worker backlog 9,134 → 83, ceiling migrated to shared MySQL writes ([§ V2-2 ceiling](#v2-2-hot-path-ceiling-measured-2026-05-29)). **Next:** write-side (read/write split, batch enrollment inserts, or shard). |
| `v0.3.0-durability` | V3 | Journey FSM · DLQs · per-workspace queue isolation (implementation or design-doc-only depending on interview timing) |

## Reading

- Customer.io's `customerio/roaring` and `customerio/esdb` — public artefacts that tell you how they think
- Martin Kleppmann, *DDIA* ch. 11 (stream processing)
- Google SRE workbook ch. 5 (multi-window multi-burn-rate alerts) — carried over from Build 5
