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
| **Status** | `v0.1.5-foundation-complete`. Six V1 surfaces shipped — Track API, person store, segment store + scan eval, campaign trigger + in-process Dispatcher, stub receiver, full observability scaffold. V1 load-test run on 2026-05-21 — see [§ V1 ceiling](#v1-ceiling-on-this-hardware). |
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

V2's expected new ceiling once the consumer parallelizes: **render CPU
per workspace** and bitmap memory accounting. We'll re-run the same load
driver and re-measure.



| URL | What |
|---|---|
| http://localhost:8090/healthz | `track-api` (V1) — `{"status":"ok"}` |
| `POST http://localhost:8090/people` | Identify / attribute upsert. `JSON_MERGE_PATCH` semantics — `null` value deletes the key. *(V1 PR 2)* |
| `GET  http://localhost:8090/people/<id>` | Read one person back. *(V1 PR 2)* |
| `POST http://localhost:8090/segments` | Define a segment with a JSON condition tree (`and`/`or`/`not` + `attr_eq`/`attr_exists`/`event_seen`). Depth cap 8. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>` | Read the stored definition. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>/check?person_id=<pid>` | Scan-eval membership for one person. V2's bitmap path removes the scan. *(V1 PR 3)* |
| `POST http://localhost:8090/campaigns` | Define a campaign — trigger (same condition tree as segments) + Go `text/template` body. Validates at insert time. *(V1 PR 4)* |
| `GET  http://localhost:8090/campaigns/<id>` | Read the stored campaign. *(V1 PR 4)* |
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
| MySQL | `:3307` | container `:3306`; host `:3306` left for any local MySQL |
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
│   ├── track-api/                  V1 HTTP intake
│   ├── stub-receiver/              V1 delivery sink
│   ├── campaign-worker/            V1.x — campaign trigger + render (added next PR)
│   ├── segment-worker/             V2 — incremental bitmap segment maintenance
│   ├── journey-scheduler/          V3 — SKIP-LOCKED scheduler
│   └── loadgen/                    V1.x — Go load driver for scale ceiling runs
├── internal/
│   ├── event/                      shared Event struct
│   ├── eventstore/                 MySQL persistence (sql.Open in V1, otelsql follow-up)
│   ├── otelinit/                   Build 5 carryover — TracerProvider + MeterProvider
│   ├── logsx/                      Build 5 carryover — slog JSON + trace_id/span_id
│   ├── person/                     V1.x — person store
│   ├── segment/                    V1.x — condition tree parser; V2 — roaring bitmap engine
│   ├── campaign/                   V1 PR 4 — trigger eval + Go text/template render + Dispatcher (in-process channel + 1 consumer goroutine)
│   └── journey/                    V3 — FSM with delays
├── deploy/                         Build 5 carryover — collector/tempo/prom/loki/alloy/grafana
│   └── grafana/provisioning/dashboards/ — red.json + use.json (panels TBD-rewritten for MySQL)
├── migrations/                     001_v1.sql (workspaces, people, events, journey_enrollments, idempotency_keys) · 002_segments.sql · 003_campaigns.sql
└── chaos/                          load-v1.sh — bash+curl V1 driver (Go loadgen lands in V2)
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
| `v0.1.5-ceiling` | V1 PR 5 (this commit) | Load driver + V1 ceiling writeup (see [§ V1 ceiling](#v1-ceiling-on-this-hardware)). Bottleneck named: single Dispatcher consumer, 2.9× producer-consumer gap, ~69% drop rate at the soak rate. |
| `v0.2.0-throughput` | V2 | RabbitMQ fan-out · roaring bitmaps · idempotency keys |
| `v0.3.0-durability` | V3 | Journey FSM · DLQs · per-workspace queue isolation (implementation or design-doc-only depending on interview timing) |

## Reading

- Customer.io's `customerio/roaring` and `customerio/esdb` — public artefacts that tell you how they think
- Martin Kleppmann, *DDIA* ch. 11 (stream processing)
- Google SRE workbook ch. 5 (multi-window multi-burn-rate alerts) — carried over from Build 5
