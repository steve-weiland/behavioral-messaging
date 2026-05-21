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
| **Status** | `v0.1.0-foundation-scaffold`. Track API + MySQL + stub receiver wired end-to-end with trace context propagation through the full Build 5 observability stack. |
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
make up          # docker compose up -d --build (8 services in V1)
make seed        # POST 10 events at 1 req/s
make load        # (V1.x) sustained-load driver
make showcase    # (V1.x) steady + slow + chaos driver
make mysql       # interactive mysql shell
make down        # tear it all down
```

## Where to look (so far)

| URL | What |
|---|---|
| http://localhost:8090/healthz | `track-api` (V1) — `{"status":"ok"}` |
| `POST http://localhost:8090/people` | Identify / attribute upsert. `JSON_MERGE_PATCH` semantics — `null` value deletes the key. *(V1 PR 2)* |
| `GET  http://localhost:8090/people/<id>` | Read one person back. *(V1 PR 2)* |
| `POST http://localhost:8090/segments` | Define a segment with a JSON condition tree (`and`/`or`/`not` + `attr_eq`/`attr_exists`/`event_seen`). Depth cap 8. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>` | Read the stored definition. *(V1 PR 3)* |
| `GET  http://localhost:8090/segments/<id>/check?person_id=<pid>` | Scan-eval membership for one person. V2's bitmap path removes the scan. *(V1 PR 3)* |
| http://localhost:8091/healthz | `stub-receiver` (V1) |
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
│   ├── campaign/                   V1.x — single-step send
│   └── journey/                    V3 — FSM with delays
├── deploy/                         Build 5 carryover — collector/tempo/prom/loki/alloy/grafana
│   └── grafana/provisioning/dashboards/ — red.json + use.json (panels TBD-rewritten for MySQL)
├── migrations/001_v1.sql           workspaces, people, events, journey_enrolments, idempotency_keys
└── chaos/                          load + chaos scripts (added next PR)
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
| `v0.1.0-foundation` | V1 PR 1 (this commit) | Scaffold + Track API + stub receiver + MySQL + observability drop-in |
| `v0.1.x` | V1 PRs 2-5 | Person store + segment evaluation (scan) + campaign worker + load driver + load-test writeup |
| `v0.2.0-throughput` | V2 | RabbitMQ fan-out · roaring bitmaps · idempotency keys |
| `v0.3.0-durability` | V3 | Journey FSM · DLQs · per-workspace queue isolation (implementation or design-doc-only depending on interview timing) |

## Reading

- Customer.io's `customerio/roaring` and `customerio/esdb` — public artefacts that tell you how they think
- Martin Kleppmann, *DDIA* ch. 11 (stream processing)
- Google SRE workbook ch. 5 (multi-window multi-burn-rate alerts) — carried over from Build 5
