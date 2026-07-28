# Behavioral Messaging Platform

| Field   | Value                                  |
|---------|----------------------------------------|
| Version | 0.2.6                                  |
| Author  | Steve Weiland                          |
| Date    | 2026-07-28                             |
| Status  | V1 + V2 complete (V3 designed, unbuilt) |

---

## 1. Overview

A multi-tenant behavioral messaging platform — Customer.io-inspired MVP
sized to fit a laptop. Event-driven by design: a person performs an
action (`signed_up`, `paid`, `clicked_link`), the platform stores the
event, evaluates segment membership and campaign triggers, and dispatches
a per-person message via a stub delivery worker.

The build is structured as three scale tiers rather than the
make-it/break-it/fix-it pattern of Builds 1-5. Each tier raises the
architectural ceiling by making a real engineering decision; the
artefact per tier is the "what broke at the old ceiling, what we
changed, what now breaks" writeup with hardware-pinned measurements.

| Tier | Mechanism shift | What V(n+1) addresses |
|---|---|---|
| **V1 Foundation** | Workspace-scoped from day one. Single-process per service, single MySQL, in-process channel queue, scan-based segment evaluation, sync stub dispatch. | Single-writer + sync-dispatch ceiling. |
| **V2 Throughput** | Roaring bitmaps for segments (incremental membership maintenance) + RabbitMQ async fan-out + batched idempotent enrollment. | **Measured: the single shared MySQL's write throughput** — one event `INSERT` + one enrollment `INSERT` per event competing for one instance. Not render CPU or bitmap memory, which were the pre-measurement guesses and are never reached while one MySQL gates writes. |
| **V3 Durability** | Per-person journey state machine with delays (persisted FSM, `SELECT ... FOR UPDATE SKIP LOCKED` scheduler) + DLQs + retry semantics + per-workspace queue isolation. | Cross-workspace coordination overhead — V4 sharding territory. |

Multi-tenancy is **not** a tier — it's a cross-cutting V1 property
(`workspace_id` on every row, queue key, and span attribute).

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Workspace | Tenant boundary. All data is partitioned by `workspace_id`; no cross-workspace reads. |
| Person | One tracked end-user. Identified by `(workspace_id, person_id)`. Attributes are a JSON column on the `people` table. |
| Event | Append-only record of a behavioral action. PK `(workspace_id, event_id)`. Payload is JSON. |
| Track API | HTTP intake — `POST /events` with `X-Workspace-ID` header. Mirrors Customer.io's terminology. |
| Segment | Named set of persons matching a boolean condition tree over attributes + event history. V1 evaluates by scan; V2 by roaring bitmap with incremental membership maintenance. |
| Campaign | A trigger condition + a message template. V1: single-step send on event match. V3: multi-step journey with delays + branches. |
| Journey enrollment | The audit record when a campaign enrolls a person, with `event_id` cross-reference. |
| Stub receiver | V1's stand-in for a delivery provider (ESP / APNs / FCM / webhook). HTTP receiver that logs the payload. |
| Idempotency key | `(workspace_id, person_id, campaign_id, schedule_key)` row guaranteeing at-most-once *dispatch* even under retry. **Deferred to V3** (BM‑105) — the `idempotency_keys` table exists (BM‑13) but is unwritten. What V2-3 shipped instead is *enrollment* idempotency: a UNIQUE natural key on `journey_enrollments` making redelivery a no-op at the audit layer (BM‑100). Redelivery cannot double-enroll; it can still double-*send*. |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords:
**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 V1 — Foundation

#### 3.1.1 Components

| ID | Requirement |
|----|-------------|
| `BM‑01` | The V1 stack **MUST** consist of two Go services: `track-api`, `stub-receiver`. Both run as `cmd/<service>/main.go` in a single Go module. |
| `BM‑02` | The system **MUST** run a single-node MySQL 8.0 with the schema in `migrations/001_v1.sql` applied at first start. |
| `BM‑03` | The full Build 5 observability stack (OTel collector + Tempo + Prometheus + Loki + Alloy + Grafana) **MUST** be brought up by `docker compose up`. |

#### 3.1.2 Data model

| ID | Requirement |
|----|-------------|
| `BM‑10` | Every entity table **MUST** carry `workspace_id` as the first column of its primary key. |
| `BM‑11` | The `events` table **MUST** be append-only with PK `(workspace_id, event_id)`. Re-inserting the same `event_id` **MUST** be a no-op (`ON DUPLICATE KEY UPDATE event_id = event_id`). |
| `BM‑12` | `people.attributes` and `events.payload` **MUST** be MySQL `JSON` columns. |
| `BM‑13` | `journey_enrollments` + `idempotency_keys` tables **MUST** exist in V1 even though V1 doesn't yet write to them — adding tables later requires no code change in V2/V3. |

#### 3.1.3 Track API

| ID | Requirement |
|----|-------------|
| `BM‑20` | `track-api` **MUST** expose `POST /events` accepting JSON `{ person_id, event_name, payload }`. |
| `BM‑21` | The `X-Workspace-ID` request header **MUST** be required and validated against `[A-Za-z0-9_-]{1,64}`. |
| `BM‑22` | A UUIDv4 `event_id` **MUST** be generated server-side. |
| `BM‑23` | The event **MUST** be persisted to MySQL via `eventstore.Insert` before the response is sent. |
| `BM‑24` | `track-api` **MUST** respond `202 Accepted` with `{ "event_id": "<uuid>" }` on success. |
| `BM‑25` | `track-api` **MUST** expose `GET /healthz` → `{"status":"ok"}`. |

#### 3.1.4a Person store (V1 PR 2)

| ID | Requirement |
|----|-------------|
| `BM‑26` | `track-api` **MUST** expose `POST /people` accepting JSON `{ person_id, attributes }`. `X-Workspace-ID` header required (same validation as `/events`). Request bodies **MUST** be capped at 16 KiB; oversized requests **MUST** return `400`. |
| `BM‑27` | `attributes` **MUST** be a JSON object. `POST /people` **MUST** upsert via MySQL `JSON_MERGE_PATCH(existing, new)` — RFC 7396 semantics: new keys overwrite, omitted keys preserved, `null` values delete the key. `updated_at` **MUST** refresh on every upsert. |
| `BM‑28` | `track-api` **MUST** expose `GET /people/{person_id}` returning `{ workspace_id, person_id, attributes, created_at, updated_at }` or `404` when absent. |
| `BM‑29` | `POST /events` **MUST** ensure the referenced `people` row exists before the `events` insert. Implementation: `INSERT IGNORE INTO people (workspace_id, person_id, attributes) VALUES (?, ?, JSON_OBJECT())`. Idempotent — existing rows are never clobbered. |

#### 3.1.4b Segment store + scan-based evaluator (V1 PR 3)

| ID | Requirement |
|----|-------------|
| `BM‑30` | `track-api` **MUST** expose `POST /segments` accepting `{ segment_id, name, definition }`. `X-Workspace-ID` required. Request body **MUST** be capped at 16 KiB. |
| `BM‑31` | `definition` **MUST** be a JSON condition tree supporting these ops: boolean `and` (≥1 sub), `or` (≥1 sub), `not` (exactly 1 sub); leaf `attr_eq {key,value}`, `attr_exists {key}`, `event_seen {name}`. Unknown ops **MUST** be rejected with `400`. Maximum tree depth **MUST** be capped at 8. |
| `BM‑32` | `attr_eq` value comparison **MUST** use structural JSON equality (strings, numbers, booleans, nulls, arrays, objects all comparable). |
| `BM‑33` | `event_seen` in V1 **MUST** evaluate against the person's full event history within the workspace (scan-based). Time-window predicates (e.g. `event_seen_within_30d`) are out of scope; V1.x or V2 may add them. |
| `BM‑34` | `track-api` **MUST** expose `GET /segments/{segment_id}` returning the stored definition + timestamps, or `404`. |
| `BM‑35` | `track-api` **MUST** expose `GET /segments/{segment_id}/check?person_id={pid}` returning `{ workspace_id, segment_id, person_id, member }`. A person row that does not exist **MUST** evaluate to `member: false` (not `404`). |
| `BM‑36` | The scan-based check **MUST** read at most `segmentScanEventLimit` (V1 default 1000) events per person via `eventstore.RecentByPerson(... LIMIT ?)` ordered by `received_at DESC`. The cap protects V1 from hot-profile pathology; V2's incremental bitmap maintenance removes it. |
| `BM‑37` | Segment definitions **MUST** be validated at `POST` time (recognized ops, required fields per op, depth cap); validation failures return `400` with a human-readable reason. |
| `BM‑38` | The segment evaluator package (`internal/segment`) **MUST** expose its `Condition` + `EvalContext` + `Evaluate` API to be reused by the campaign-trigger handler in V1 PR 4. Same tree, two callers. |

#### 3.1.4c Campaign trigger + in-process fan-out (V1 PR 4)

| ID | Requirement |
|----|-------------|
| `BM‑70` | `track-api` **MUST** expose `POST /campaigns` accepting `{ campaign_id, name, trigger, template }`. `X-Workspace-ID` required. Request body **MUST** be capped at 32 KiB; `template` **MUST** be capped at 16 KiB. |
| `BM‑71` | `trigger` **MUST** be a `segment.Condition` tree validated against the same six-op grammar (BM‑31) — and/or/not, attr_eq, attr_exists, event_seen — including the depth cap of 8. Validation **MUST** run at `POST` time; failures return `400` with a human-readable reason. |
| `BM‑72` | `template` **MUST** be a Go `text/template` string, parsed at `POST` time with `Option("missingkey=zero")` so absent attributes render as the zero value rather than erroring. Parse failures return `400`. |
| `BM‑73` | `track-api` **MUST** expose `GET /campaigns/{campaign_id}` returning the stored row or `404`. |
| `BM‑74` | The `POST /events` handler **MUST** hand each accepted event to the in-process `Dispatcher` (after `eventstore.Insert` succeeds, BM‑23). The Dispatcher **MUST** be a single buffered channel (V1 default 1024) consumed by one goroutine. When the buffer is full the producer **MUST** drop the event and increment `campaign_queue_dropped_total{workspace_id}`; the response to the client is unaffected. |
| `BM‑75` | The Dispatcher **MUST**, per event, list all campaigns in the workspace, evaluate each trigger against `EvalContext{Person: p, Events: [inbound_event]}` — a single-event view where `event_seen(name)` matches iff the inbound event's name is the target — and, for each match: render the template, POST the rendered message to `STUB_RECEIVER_URL`, and insert a `journey_enrollments` row. |
| `BM‑76` | Trigger evaluation **MUST** reuse `internal/segment`'s `DecodeCondition` + `Validate` + `Evaluate` API verbatim. Same tree, two callers. |
| `BM‑77` | The render context **MUST** be `{ Person, Event, Attrs, Now }` where `Attrs` is the person's attributes JSON pre-decoded into `map[string]any`. A render failure at dispatch time **MUST** be logged + recorded on the span and **MUST NOT** block other matching campaigns for the same event. |
| `BM‑78` | A `journey_enrollments` row **MUST** be inserted on every successful dispatch with `(workspace_id, enrollment_id, campaign_id, person_id, triggered_by=event_id)`. Audit-insert failure after a successful HTTP dispatch **MUST NOT** reverse the dispatch — V1 acceptable; V2's two-phase dispatch closes this gap. |
| `BM‑79` | The Dispatcher **MUST** publish `campaign_queue_dropped_total{workspace_id}` and `campaign_dispatched_total{workspace_id, campaign_id}` counters via the global OTel meter. The async `campaign.process` span **MUST** join the producer's trace by re-using the inbound event's span context (`trace.ContextWithSpanContext(context.Background(), ...)` so the consumer outlives the HTTP handler). |

#### 3.1.4d Stub receiver (V1 PR 1)

*Renumbered from `BM‑30..32` to `BM‑43..45` (spec 0.2.6): those IDs were
already taken by the segment store in §3.1.4b, so "BM‑31" referred to two
different requirements and traceability broke. The section heading gained its
`d` for the same reason — it sat between `3.1.4c` and `3.1.5` as a bare
`3.1.4`.*

| ID | Requirement |
|----|-------------|
| `BM‑43` | `stub-receiver` **MUST** expose `POST /` accepting any JSON body up to 64 KiB. |
| `BM‑44` | The receiver **MUST** log one structured INFO line per delivery with `event_id` (when present) and `bytes`. |
| `BM‑45` | The receiver **MUST** return `200 OK` with `{"received":true}` on every successful read. |

#### 3.1.5 Observability

| ID | Requirement |
|----|-------------|
| `BM‑40` | Both services **MUST** initialise tracing + metrics via `internal/otelinit.Init` (Build 5 reuse) and structured JSON logging via `internal/logsx.Init`. |
| `BM‑41` | HTTP handlers **MUST** be wrapped in `otelhttp.NewHandler`. Outbound HTTP **MUST** use `otelhttp.NewTransport` so trace context propagates to `stub-receiver`. |
| `BM‑42` | A single `POST /events` call **MUST** produce one connected trace in Tempo. **V1 shape:** `track-api` server span → outbound HTTP client → `stub-receiver` server span. **V2-1b shape (current):** `track-api` server span → `amqp.publish` → `amqp.consume` (campaign-worker) → `campaign.process` → outbound HTTP client → `stub-receiver` server span — track-api no longer dispatches, so the trace crosses the broker via W3C traceparent on the AMQP headers (`internal/amqpx/propagation.go`). MySQL spans are **still absent**: `otelsql` remains unwired (see §5), so the DB hops don't appear in either shape. |

#### 3.1.6 Operational

| ID | Requirement |
|----|-------------|
| `BM‑50` | Each service **MUST** handle `SIGTERM` with graceful shutdown (drain HTTP, flush OTel exporters, exit ≤ 10 s). |
| `BM‑51` | All services **MUST** read connection settings (`MYSQL_DSN`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `STUB_RECEIVER_URL`, `PORT`) from environment variables. |
| `BM‑52` | A `Makefile` **MUST** expose targets `up`, `down`, `logs`, `seed`, `mysql`, `load`, `showcase` mirroring Build 5's surface. (`load` is an alias for `load-soak`, the standard ceiling-search run; `load-prep` / `load-quick` are the other two modes.) |

#### 3.1.7 V1 scale ceiling (target on this hardware)

| ID | Requirement |
|----|-------------|
| `BM‑60` | V1 **MUST** sustain ≥ 200 sustained `POST /events` per second from one `track-api` replica on the user's hardware. Actual measured ceiling is documented in `README.md` per-tier section, with hardware specs at the top. |
| `BM‑61` | When the ceiling is reached, the **observed bottleneck** (MySQL write contention vs. HTTP goroutine count vs. stub-receiver latency) **MUST** be named in the V1 writeup. That observation seeds the V2 spec. |

### 3.2 V2 — Throughput

V1 PR 5 measured the V1 ceiling on the reference hardware (Apple M4 Max,
36 GB, macOS 15.7.3; one `track-api` replica, single MySQL, in-process
Dispatcher). See [README § V1 ceiling](./README.md#v1-ceiling-on-this-hardware)
for the run config and Grafana excerpts. Headline finding:

- **Intake (HTTP → MySQL `events`):** ~568 events/s sustained at concurrency=20, zero failures.
- **Fan-out (Dispatcher consumer):** ~196 events/s steady-state.
- **Drop rate at the in-process channel:** ~69% during the soak.
- **Named bottleneck:** the single Dispatcher consumer goroutine doing four serial MySQL hops + one sync HTTP per event.

V2 closes that gap in three mechanism PRs, each one an interview answer
with metrics from the same load driver re-run.

#### 3.2.1 V2-1 — RabbitMQ async fan-out (BM‑80..89)

The in-process `chan job` becomes a durable RabbitMQ queue; the lone
consumer goroutine becomes a worker pool. This is the load-bearing PR —
it removes the V1 ceiling directly.

| ID | Requirement |
|----|-------------|
| `BM‑80` | The Dispatcher **MUST** publish each accepted event to a RabbitMQ exchange (`campaigns.fanout`) keyed by `workspace_id`. The producer-side `Submit(ctx, ev)` API **MUST NOT** change — V1's call site in `/events` is unchanged. |
| `BM‑81` | A new `cmd/campaign-worker/` service **MUST** consume from per-workspace queues bound to `campaigns.fanout`. Default deployment: one worker container, configurable concurrency (`PREFETCH`, default 32). Multiple replicas **MUST** be safe (queues are competing-consumer; ack only after dispatch + audit). |
| `BM‑82` | Per-workspace queue admission **MUST** be in place from V2-1, not deferred to V3. Concretely: one queue per workspace, named `campaigns.fanout.<workspace_id>`, bound to the exchange with `routing_key=<workspace_id>`. Per-workspace queue depth + processing rate **MUST** surface in Prometheus attributed by `workspace_id` — via the RabbitMQ scrape described in BM‑86. (The two queues sharing those prefixes that are *not* workspaces — the global `campaigns.fanout.dlq` and segment-worker's `segments.index.people` attribute feed — are excluded from the relabel so they don't appear as phantom workspaces.) |
| `BM‑83` | Messages **MUST** be persistent (`delivery_mode=2`) and the queue **MUST** be durable. RabbitMQ restarts **MUST NOT** drop accepted events. |
| `BM‑84` | Worker acks **MUST** be manual: ack only after `journey_enrollments` is written. nack-with-requeue on transient failure (MySQL connection error, stub timeout); reject-without-requeue on terminal failure (template render error, decode error). |
| `BM‑85` | A DLQ (`campaigns.fanout.dlx`) **MUST** be wired but acceptably empty in V2-1. V3-2 introduces real retry-with-backoff semantics; V2-1 just establishes the topology so V3-2 is a config change, not a refactor. |
| `BM‑86` | `campaign_queue_dropped_total` from V1 **MUST** be retired; in V2 the producer never drops for capacity reasons — the broker provides backpressure. Queue depth **MUST** be observable per workspace. **Amended (0.2.6):** this originally specified an app-emitted `campaign_queue_depth{workspace_id}` gauge. That was the wrong design — the broker owns the depth, and a gauge emitted by the producer would be a second source of truth that can disagree with the queue it claims to describe (the producer can't see what the worker has drained). Satisfied instead by scraping RabbitMQ: `rabbitmq_detailed_queue_messages_ready{workspace_id}`, with `workspace_id` relabeled out of the queue name (`deploy/prometheus.yml`). The producer's own in-process buffer is separately visible as `campaign_publish_buffer_depth`, and its drop reasons as `campaign_publish_dropped_total{workspace_id,reason}`. |
| `BM‑87` | The new V2 ceiling **MUST** be measured with the same `chaos/load-v1.sh` driver re-run on the same hardware. Expected new bottleneck per BM‑88. |
| `BM‑88` | The V2-1 writeup **MUST** name the new bottleneck. Expected candidates: render CPU on the worker; MySQL connection-pool contention from N parallel workers; broker publish throughput. |
| `BM‑89` | The producer (`/events` handler) **MUST** continue to respond ≤ 1 hop after `eventstore.Insert` returns — broker publish runs in a goroutine with bounded retry, never blocks the HTTP reply. (This preserves V1's intake-throughput characteristic; the goal of V2 is to grow the fan-out rate, not slow the intake.) |

##### V2-1 ceiling (measured 2026-05-29) — closes BM‑87/88

Re-measured on the same hardware (M4 Max, 36 GB) with a new Go load
driver (`cmd/loadgen/` — keep-alive connection reuse, so it pushes far
past the bash driver's ~557/s process-spawn ceiling). Fan-out side
sampled live via `chaos/watch-fanout.sh` (RabbitMQ mgmt API + `docker
stats`); drain rate read as the `journey_enrollments` slope (authoritative
— immune to the OTel→Prom scrape lag).

| `PREFETCH` | Worker fan-out rate | Peak queue backlog | Worker CPU | MySQL CPU | Loss |
|---|---|---|---|---|---|
| 32 (default) | ~1,675/s | 103,670 | 479% | 283% | 0 |
| 128 (bug, pre-fix) | collapse — 40s stall, requeue storm | 78,895 | **1,320%** | 312% | 0\* |
| 128 (post-fix) | **~2,880/s** | 28,339 | 240% | 330% | 0 |
| 256 (post-fix) | ~2,750/s (no gain) | 17,683 | 211% | **342%** | 0 |

\* nothing lost — RabbitMQ held the backlog durably; it drained on recovery.

**Named bottleneck (BM‑88 confirmed):** per-event MySQL work on the single
shared instance, bounded by the worker's 25-connection pool. Past
`PREFETCH=128` more consumer concurrency buys no throughput — MySQL is the
busiest dependency (~342%) while the worker sits at ~211% CPU. This is
exactly what V2-2 (remove the per-event `ListByWorkspace` scan via bitmaps
+ a campaign cache) is positioned to lift. Loss is now structurally
impossible: V1's 69% silent channel drop became a durable, drainable queue
backlog with `publish_dropped_total == 0` across every run.

**Lessons — two latent Go HTTP bugs masked the real ceiling.** Naively
raising `PREFETCH` 32→128 to chase a higher number made throughput *worse*
and unmasked two classic connection-leak bugs in the worker's stub
dispatch:

1. **Default transport pool.** The stub client wrapped
   `http.DefaultTransport`, whose `MaxIdleConnsPerHost` is 2 — so under
   high prefetch nearly every dispatch dialed a fresh TCP connection.
   *Fix:* clone the transport and size `MaxIdleConnsPerHost` /
   `MaxConnsPerHost` to `PREFETCH` (`cmd/campaign-worker/main.go`).
2. **Undrained response body (dominant).** `processor.go` closed
   `resp.Body` without reading it to EOF. net/http only returns a
   connection to the idle pool when the body is fully drained *and*
   closed; closing an unread body discards the TCP connection, defeating
   any pool sizing. *Fix:* `io.Copy(io.Discard, resp.Body)` before close
   (`internal/campaign/processor.go`).

Together they caused ephemeral-port exhaustion (`connect: cannot assign
requested address`), ~73k failed dispatches, a nack-requeue storm, and a
13-of-14-core CPU burn — with throughput going *down*. After both fixes:
worker CPU 1,320%→240%, fan-out +70% (1,675→2,880/s), zero errors.

**Lesson (found at close-out, 0.2.6) — a third silent-loss path, on the
producer.** `Publisher.Submit` had three `select` cases: `<-p.done`
(shutdown), `p.ch <- job` (enqueue), and `default` (buffer full). After
`Stop()` closes `done`, the first two are *both* ready — and Go chooses
uniformly at random among ready cases. So roughly half of any events
submitted during shutdown were enqueued into a buffer that nothing would
drain, and counted as neither `shutdown` nor `buffer_full`: exactly the
silent drop V1's channel had, reintroduced in the mechanism built to remove
it. Narrow in practice (track-api drains HTTP before stopping the publisher),
but silent, and the fix is to check `done` in its own `select` first so
shutdown has deterministic precedence.

The reason this survived three PRs of measurement: every load run publishes
into a *running* producer, so the shutdown race is never on the measured
path. It surfaced the moment the first unit test asked what `Submit` does
after `Stop` — a reminder that "we measured it at scale" and "we tested its
edges" are different claims.

#### 3.2.2 V2-2 — Roaring bitmaps for segment evaluation (BM‑90..99)

Maps to the interview question "Scale segment evaluation to 10M users."
Upstream `github.com/RoaringBitmap/roaring/v2`, not from-scratch
primitives (Q4). Inverts V1's *compute-on-read* scan to
*maintain-on-write* bitmaps.

| ID | Requirement |
|----|-------------|
| `BM‑90` | A bitmap engine (`internal/segmentidx`) **MUST** maintain, per workspace, base bitmaps of dense person ordinals: `eventSeen[name]`, `attrEq[key␀value]`, `attrExists[key]`. Membership **MUST** be answered by walking the existing `segment.Condition` tree with bitmap-backed leaves (`not` = negation; unknown person = non-member, preserving the BM‑35 `/check` contract). |
| `BM‑91` | Maintenance **MUST** be incremental: one bit flipped per event / attribute change. Attribute updates **MUST** move the bit (clear the old value); deletes **MUST** clear it. `Observe*` **MUST** be idempotent so at-least-once redelivery is safe. |
| `BM‑92` | A new `cmd/segment-worker/` service **MUST** own the index. At boot it **MUST** backfill from current MySQL state (`person.All` + `eventstore.DistinctPersonEvents`); then maintain from two streams — events via its own per-workspace queue on `campaigns.fanout` (`segments.index.<ws>`, distinct from the campaign queue so it gets a copy, not a steal), and attribute changes via the `people.changes` feed (BM‑94). |
| `BM‑93` | segment-worker **MUST** expose `POST /internal/check {workspace_id, person_id, definition}` → `{member}`, answering purely from bitmaps (the caller supplies the definition, so the read path needs no segment store). |
| `BM‑94` | `track-api` `POST /people` **MUST** publish the person's full merged attributes to a `people.changes` fanout exchange — deliberately OFF the high-throughput `/events` path so attribute indexing never regresses intake throughput. Best-effort: a publish failure logs and does not fail the request (the index is eventually consistent). |
| `BM‑95` | `track-api` `/segments/{id}/check` **MUST** delegate membership to segment-worker when `SEGMENT_INDEX_URL` is set, removing the per-person `RecentByPerson` event scan. It **MUST** fall back to the V1 scan if the index is unavailable, and **MUST** log which backend served the check. |
| `BM‑96` | The engine's `event_seen` is "ever," vs the V1 scan's last-`segmentScanEventLimit` window — strictly more complete. Documented as a deliberate semantic refinement, not a regression. |
| `BM‑97` | The new ceiling **MUST** be re-measured (the V2-1 bottleneck was per-event MySQL on the worker fan-out path; V2-2 moves segment/trigger evaluation off MySQL). **Re-measured 2026-05-29 (PR 11):** the campaign-worker hot path now reads campaigns + attributes from in-process caches (no per-event `ListByWorkspace`/`person.Get`). Result: the fan-out worker stopped being the bottleneck — backlog **9,134 → 83**, worker CPU 224% → 145% — and the ceiling **migrated to the shared single MySQL's write throughput** (event `INSERT` + enrollment `INSERT` competing; ~2,700/s, flat under added concurrency, MySQL pinned ~306% while worker/track-api have headroom). Not bitmap memory / render CPU as originally guessed — those aren't reached while one MySQL gates writes. Next lever: read/write split, batch enrollment inserts, or shard (V3/V4). |

**V2-2 status (complete; a/b 2026-05-29, c PR 11).** The engine
(`internal/segmentidx`, BM‑90/91) and segment-worker + `/check` repoint +
`people.changes` feed (BM‑92..96) shipped and were verified end-to-end via
`make seed-segments` — all checks served `backend=bitmap`, zero scan
fallbacks. The hot-path repoint (BM‑97) then shipped in PR 11.

**Where the plan changed, and why it matters.** This section originally said
the hot-path fix was to repoint the campaign-worker's per-event *trigger*
evaluation at the bitmap engine. That is **not** what shipped. PR 11 instead
put campaign definitions and person attributes behind in-process caches
(`campaigncache.go`, `attrcache.go`) and left trigger evaluation as an
in-process `cond.Evaluate` over a single-event `EvalContext`.

The distinction is worth being precise about, because it changes what the
tier proves. The V2-1 ceiling was *MySQL reads on the hot path*, not
condition-tree evaluation — the trigger eval was never the expensive part
(one tree, one event, microseconds). Removing the two reads is what collapsed
the backlog 9,134 → 83; routing trigger eval through the bitmap engine would
have removed no MySQL work the caches hadn't already removed, and would have
put a network hop where an in-process function call was. So the bitmap engine
earns its place on the `/check` read surface (where it removes an unbounded
per-person event scan, BM‑90/96) rather than on the fan-out path.

Residual per-event CPU on the hot path — a trigger JSON decode and a
`text/template` parse *per event* — was found and removed at close-out
(0.2.6): `Campaign.Compile()` hoists both to once per cache refresh. That is
the render-CPU candidate BM‑88 named, addressed without ever having become
the binding constraint.

#### 3.2.3 V2-3 — Batched idempotent enrollment (BM‑100..109)

Shipped as **Lean** (PR 13): batched, idempotent enrollment writes. The
full two-phase send-gate is **deferred** — on a stub sink with a
write-bound MySQL it adds writes for no payoff; it belongs with V3
retry/DLQ when a real sender exists.

| ID | Requirement |
|----|-------------|
| `BM‑100` | `journey_enrollments` **MUST** carry a UNIQUE natural key `(workspace_id, campaign_id, triggered_by)` (migration 004) — one row per (campaign, inbound event). The enrollment insert **MUST** be `ON DUPLICATE KEY UPDATE` (no-op), so an at-least-once redelivery never creates a duplicate. Partly closes BM‑78 (audit idempotency; double-*send* still possible — see BM‑105). **Deployment caveat:** migrations are mounted at `/docker-entrypoint-initdb.d`, which MySQL runs *only on an empty datadir*. On a volume created before this migration the `ALTER TABLE` never runs, and `ON DUPLICATE KEY UPDATE` then silently protects nothing — the guarantee is only true after `make down` (which takes volumes) or a manual `ALTER TABLE`. |
| `BM‑101` | The campaign-worker **MUST** coalesce enrollment inserts into one multi-row commit per flush (`internal/campaign/batcher.go`), bounded by `BATCH_MAX` rows or `BATCH_WINDOW` time — amortizing the per-commit fsync that made the fan-out write side the backlog-prone laggard. |
| `BM‑102` | A dispatch **MUST** ack its AMQP delivery only after its enrollment batch has COMMITTED (Submit blocks on the flush). A flush error **MUST** nack-requeue the event; the idempotent insert (BM‑100) makes re-dispatch safe. Preserves BM‑84 at-least-once. |
| `BM‑103` | Batching **MUST** be toggleable (`CAMPAIGN_BATCH=on\|off`) for A/B measurement; the off path inserts directly but is still idempotent (BM‑100). |
| `BM‑105` (deferred) | Two-phase send-gate via `idempotency_keys` (`sending` → POST → `sent`) to close the double-*send* window for a real downstream. Deferred — revisit with V3 retry/DLQ. |

**Measured (PR 13, fresh DB, pool 50, prefetch 128, c=200):** with batching
the fan-out write side keeps pace — peak queue backlog **166**; without it,
backlog **156,961** (each enrollment its own commit). Coalescing ran at
~63.5 rows/flush (cap 64). Zero duplicate enrollments across 700k+ rows.
*(Absolute intake here ~5,750/s is higher than PR 11's ~2,700/s because
this is a fresh DB — small tables/indexes insert faster; the valid
comparison is batching-on-vs-off backlog, not the absolute number.)*

### 3.3 V3 — Durability

*Drafted in PR 3's spec bump if implemented; otherwise written as a
standalone design doc. Three mechanism subsections: persisted journey
FSM with `SKIP LOCKED` scheduler, DLQs + retry, per-workspace queue
isolation.*

---

## 4. Inputs / Outputs

### HTTP — `track-api`

```
POST /events
Headers:
  X-Workspace-ID: ws_alpha   (required, [A-Za-z0-9_-]{1,64})
Body:
  person_id:  string (required, ≤ 128 chars)
  event_name: string (required, ≤ 128 chars)
  payload:    object (optional; defaults to {})

→ 202 Accepted
  event_id: string (UUIDv4)

→ 400 Bad Request
  error: "Human-readable message"
```

### HTTP — `track-api` `/people` (V1 PR 2)

```
POST /people
Headers:
  X-Workspace-ID: ws_alpha
Body:
  person_id:  string (required, ≤ 128 chars)
  attributes: object (required; JSON_MERGE_PATCH applied — null deletes a key)

→ 202 Accepted   {"status":"ok"}
→ 400 Bad Request {"error":"..."}

GET /people/{person_id}
Headers:
  X-Workspace-ID: ws_alpha

→ 200 OK         {"workspace_id":...,"person_id":...,"attributes":{...},"created_at":...,"updated_at":...}
→ 404 Not Found  {"error":"not found"}
```

### HTTP — `track-api` `/segments` (V1 PR 3)

```
POST /segments
Headers:
  X-Workspace-ID: ws_alpha
Body:
  segment_id: string (required, [A-Za-z0-9_-]{1,64})
  name:       string (required, ≤ 255 chars)
  definition: { "op": "and"|"or"|"not"|"attr_eq"|"attr_exists"|"event_seen", ... }

→ 201 Created    {"status":"ok"}
→ 400 Bad Request {"error":"definition: ..."}

GET /segments/{segment_id}
→ 200 OK         {"workspace_id":...,"segment_id":...,"name":...,"definition":{...},"created_at":...,"updated_at":...}
→ 404 Not Found

GET /segments/{segment_id}/check?person_id={pid}
→ 200 OK         {"workspace_id":...,"segment_id":...,"person_id":...,"member":bool}
→ 404 Not Found  (only when the segment is unknown)
```

Example definition: `plan="pro" AND has viewed_pricing`:

```json
{
  "op": "and",
  "conditions": [
    {"op": "attr_eq",    "key": "plan", "value": "pro"},
    {"op": "event_seen", "name": "viewed_pricing"}
  ]
}
```

### HTTP — `track-api` `/campaigns` (V1 PR 4)

```
POST /campaigns
Headers:
  X-Workspace-ID: ws_alpha
Body:
  campaign_id: string (required, [A-Za-z0-9_-]{1,64})
  name:        string (required, ≤ 255 chars)
  trigger:     condition tree (same grammar as segments — BM-31)
  template:    string (Go text/template body, ≤ 16 KiB)

→ 201 Created    {"status":"ok"}
→ 400 Bad Request {"error":"trigger: ..."} | {"error":"template: ..."}

GET /campaigns/{campaign_id}
→ 200 OK         {"workspace_id":..., "campaign_id":..., "name":..., "trigger":{...}, "template":"...", "created_at":..., "updated_at":...}
→ 404 Not Found
```

Example trigger + template:

```json
{
  "campaign_id": "welcome_pro",
  "name": "Welcome Pro Users",
  "trigger": {
    "op": "and",
    "conditions": [
      {"op": "attr_eq",    "key":  "plan",      "value": "pro"},
      {"op": "event_seen", "name": "signed_up"}
    ]
  },
  "template": "Welcome {{.Person.PersonID}} — your {{.Attrs.plan}} plan is live."
}
```

The template's render context exposes `Person` (full struct), `Event` (the
inbound `event.Event`), `Attrs` (decoded `map[string]any` from
`person.attributes`), and `Now` (UTC). Absent attributes render as
`<no value>` rather than erroring (`missingkey=zero`).

### HTTP — `stub-receiver`

```
POST /
Body: any JSON object ≤ 64 KiB
→ 200 OK { "received": true }
```

### Database

`migrations/001_v1.sql` — see file for ground truth. Tables:
`workspaces`, `people`, `events`, `journey_enrollments`,
`idempotency_keys`.

### Environment

All services:

```
MYSQL_DSN                       bm:bm@tcp(mysql:3306)/bm?parseTime=true
MYSQL_MAX_OPEN_CONNS            50        (idle == open; see §6 V2-2x)
OTEL_EXPORTER_OTLP_ENDPOINT     otel-collector:4317
OTEL_METRICS_EXEMPLAR_FILTER    trace_based
PORT                            8080 track-api / 8081 stub-receiver /
                                8082 campaign-worker / 8083 segment-worker
```

`track-api`:

```
AMQP_URL                        amqp://guest:guest@rabbitmq:5672/
SEGMENT_INDEX_URL               http://segment-worker:8083   (unset → V1 scan)
```

`campaign-worker`:

```
AMQP_URL                        amqp://guest:guest@rabbitmq:5672/
STUB_RECEIVER_URL               http://stub-receiver:8081
PREFETCH                        32        consumer concurrency (BM-81)
CAMPAIGN_HOT_PATH               cache|mysql   A/B for the V2-2c caches
CAMPAIGN_BATCH                  on|off        A/B for V2-3 batching (BM-103)
BATCH_MAX                       64        rows per enrollment flush
BATCH_WINDOW                    10ms      max wait for a flush to fill
```

`segment-worker`:

```
AMQP_URL                        amqp://guest:guest@rabbitmq:5672/
SEGMENT_PREFETCH                32
```

Every knob above is plumbed through `docker-compose.yml` as
`${VAR:-default}`, so an A/B run is `CAMPAIGN_BATCH=off make up` rather than a
compose edit. `STUB_RECEIVER_URL` is deliberately **not** set on `track-api`:
since V2-1b it is a producer only and never dispatches.

---

## 5. Out of Scope

Each line names the version that closes the gap. Items marked ✅ shipped and
are kept here only to show where the line moved.

**Closed since V1:**

- ✅ **Roaring bitmaps for segment evaluation** — shipped V2-2 (`internal/segmentidx`, `cmd/segment-worker`).
- ✅ **RabbitMQ async fan-out** — shipped V2-1 (`campaigns.fanout`, `cmd/campaign-worker`).
- ✅ **Enrollment idempotency** — shipped V2-3 Lean (UNIQUE natural key + `ON DUPLICATE KEY UPDATE`).

**Still out of scope:**

- **Two-phase dispatch / send-gate** (`idempotency_keys`, `sending` → `sent`) — deferred from V2-3 to V3; see BM‑105 and §8. The `events` PK plus the enrollment UNIQUE key give storage- and audit-layer idempotency; the double-*send* window remains open.
- **Multi-step journeys with delays** — V3.
- **Anonymous-to-identified merge** — V3 stretch.
- **`otelsql` for MySQL query spans** — still unwired. Originally billed as "a small follow-up commit after the kickoff scaffolding lands"; it never landed, and it's the reason BM‑42's trace has no DB spans. Cheap and worth doing, but it changes every query path, so it wants its own PR.
- **Retry-with-backoff + a bounded requeue count** — V3-2 (BM‑85). Until then a delivery that fails for a non-transient reason requeues indefinitely; `messaging.consume.requeued{queue,redelivered}` exists so that's visible rather than silent.
- **Custom objects** (non-person entities) — V4+.
- **Liquid templating** — V4+; V1+V2 use Go `text/template`. Liquid is a yak (lexer/parser, custom tags, sandboxing).
- **Authentication, multi-tenancy enforcement at the API gateway, TLS** — V4+ / out of portfolio scope.
- **Horizontal scaling beyond one replica per service** — V3 introduces per-workspace worker pools; multi-replica auto-scaling is V4. (Both workers are already multi-replica-*safe*: competing consumers on the campaign queues, per-replica ephemeral queues on `people.changes`.)
- **Send rate quotas, schema evolution tooling, Loki-side log retention policy** — V4+.

---

## 6. V1 → V3 Tier Table

Filled in incrementally as each tier ships. The artefact per row is a
bottleneck-migration narrative with hardware-pinned numbers.

| Tier | Mechanism added | Observed ceiling on this hardware | New bottleneck |
|---|---|---|---|
| V1 | Track API + MySQL + in-process channel + scan segment eval + stub HTTP dispatch | Intake ~568/s; fan-out **~196/s** (~69% dropped) | Single Dispatcher consumer goroutine — 4 serial MySQL hops + 1 sync HTTP per event |
| V2-1 | RabbitMQ durable fan-out + `campaign-worker` (manual ack, `PREFETCH` concurrency) | Fan-out **~2,800/s**, zero loss (~14× V1) | Per-event MySQL work (`ListByWorkspace` + `people.Get` + enrollment `INSERT`) over the then-default 25-conn pool on the single shared MySQL |
| V2-2 | Roaring bitmaps (`segment-worker` serves `/check`) + campaign/attribute caches remove the per-event MySQL reads from the fan-out hot path | Worker stops being the constraint: backlog **9,134 → 83**, worker CPU 224% → 145%. Intake flat at **~2,700/s** under doubled concurrency | **Shared single MySQL write throughput** — one event `INSERT` + one enrollment `INSERT` per event, MySQL pinned ~306% while worker (145%) and track-api (130%) have headroom |
| V2-2x | MySQL pool default 25 → 50 (PR 12) | Intake **2,173 → 2,836/s**, p99 **306 → 197 ms** going 25 → 64 conns | Still MySQL writes — the pool was throttling *access* to the bottleneck, not the bottleneck itself |
| V2-3 | Batched idempotent enrollment (multi-row commit per flush + UNIQUE natural key) | Write side keeps pace: peak backlog **166** batched vs **156,961** unbatched; ~63.5 rows/flush; 0 duplicate enrollments across 700k+ rows | Still the shared MySQL, now on the event-`INSERT` (intake) side — the fan-out write is amortized. Next levers: read/write split, or shard (V3/V4) |
| V3 | Journey FSM + DLQs + per-workspace queue isolation | *(not built)* | V4 sharding |

---

## 7. Resolved Decisions

| # | Question | Resolution |
|---|---|---|
| Q1 | Which RDBMS? | **MySQL 8.0.latest** — Customer.io stack alignment + fills the CV gap explicitly called out in the role evaluation. Postgres depth from prior builds transfers. |
| Q2 | Postgres or MySQL JSON columns? | **MySQL `JSON` type** with no schema enforcement on attributes/payload. Trade-off: no per-attribute index from day one (Customer.io's actual production answer is a per-workspace attribute index — V4). |
| Q3 | Queue technology for V1? | **In-process Go channel**, NOT RabbitMQ. V1 doesn't have multi-stage fan-out, so a broker buys nothing — adds infra, hides the single-process ceiling V2 needs to point at. Introduce RabbitMQ when V2's campaign fan-out demands it. |
| Q4 | Roaring bitmap library? | **Upstream `github.com/RoaringBitmap/roaring`** (or Customer.io's fork). The engineering lesson is *integrating* bitmaps into incremental segment maintenance — index design, when to rebuild vs. update incrementally, per-workspace memory accounting. **Do not write bitmap primitives from scratch.** |
| Q5 | Template engine? | **Go `text/template`** for V1+V2; Liquid is a yak (lexer/parser, custom tags, sandboxing). Mention as a V4 enhancement. |
| Q6 | Multi-tenancy — its own tier or cross-cutting? | **Cross-cutting from V1.** `workspace_id` on every row, queue key, and span attribute. Otherwise V3 becomes a rewrite, not a delta. |
| Q7 | Scale targets — absolute numbers or bottleneck narratives? | **Bottleneck-migration narratives** with hardware specs at the top of each tier's writeup. "V1 ceiling = single-writer MySQL contention at ~N writes/sec on this box; V2 changes Y, new ceiling is Z" reads honest. Fabricated laptop numbers read as fabricated to a Customer.io reviewer. |
| Q8 | V1 service boundary — one binary or many? | **Two binaries**: `track-api` + `stub-receiver`. Anything beyond a stub receiver lives behind the in-process channel; spinning out a `campaign-worker` binary is a V1.1 PR if needed. |
| Q9 | Journey FSM — V3 or V4? | **V3 implementation if interview timeline allows; V3 design-doc-only otherwise.** Capped at three step types: `send`, `delay`, `branch_on_condition`. Persisted rows in MySQL; single scheduler worker polling `WHERE wake_at <= NOW() FOR UPDATE SKIP LOCKED`. |
| Q10 | V1 segment evaluation: scan, materialised view, or roaring bitmap? | **Scan-based** in V1 — for each `/check`, read the person row + `LIMIT 1000` recent events, evaluate the condition tree in-process. Roaring bitmaps with incremental membership maintenance arrive in V2 (`customerio/roaring`-style). The V1 ceiling is the read-amplification of "scan events on every check"; the V2 mechanism removes it. Same `Condition` API survives the swap. |
| Q11 | What does the V1 segment grammar cover? | **Six ops:** `and`/`or`/`not` boolean; `attr_eq`/`attr_exists` over `person.attributes`; `event_seen` over the person's full event history in the workspace. Time-window predicates (`event_seen_within_30d`), count thresholds (`event_count > N`), arithmetic comparisons (`gt`/`lt`) — all out of V1; V1.x or V2. |

---

## 8. Open Questions

| # | Question | State |
|---|----------|-------|
| Q12 | Where does the two-phase send-gate belong (BM‑105)? | **Deferred out of V2-3, open for V3.** On a stub sink with a write-bound MySQL it adds two writes per dispatch for no observable payoff — the measurement said the write side was already the constraint. It only earns its cost against a real sender that can double-charge, and its natural partner is V3-2's retry/DLQ semantics (a send-gate without bounded retry just moves the failure). Decide when a real ESP replaces `stub-receiver`. |
| Q13 | How is a poison message bounded before V3-2? | **Open.** Requeue is currently unbounded (BM‑85 defers retry semantics). `messaging.consume.requeued{queue,redelivered}` makes a spin visible, but nothing stops it. Candidates: a `x-delivery-count` header maintained by the worker; RabbitMQ quorum queues with `delivery-limit` (also gets DLQ routing for free); or an app-side attempt counter in the message body. Quorum queues look cheapest and move the mechanism into the broker, which is where V2-1's design instinct already put backlog. |
| Q14 | Does the eventual-consistency window on attributes need bounding? | **Open, low priority.** V2-2c trades per-event `person.Get` for an in-process cache fed by `people.changes`; an event arriving just after an attribute change can evaluate the pre-change value. Cache-miss falls back to MySQL so a *new* person is never missed — only a *recently changed* value is briefly stale. Unmeasured: how wide the window actually is under load. Worth a number before claiming it's negligible. |

---

## 9. Revision History

| Version | Date       | Author        | Notes |
|---------|------------|---------------|-------|
| 0.2.6   | 2026-07-28 | Steve Weiland | Close-out review of V1 + V2 (no new tier). **Unmet MUSTs closed:** BM‑82/86 — per-workspace queue depth was specified but never observable; RabbitMQ is now scraped at `/metrics/detailed` with `workspace_id` relabeled out of the queue name (excluding the global DLQ and the `segments.index.people` feed, which would otherwise appear as phantom workspaces). BM‑86 amended: the app-emitted `campaign_queue_depth` gauge it specified was the wrong design — the broker owns the depth. BM‑103 — `CAMPAIGN_BATCH`/`BATCH_MAX`/`BATCH_WINDOW` now plumbed through compose, so the batching A/B is reproducible without editing it. BM‑52 — `make load` alias added. BM‑42 — trace shape updated to the V2-1b reality (via the broker; MySQL spans still absent, `otelsql` still unwired). **Bug found + fixed:** `Publisher.Submit` folded the shutdown check into the same `select` as the buffer send, so with both cases ready Go's uniform-random choice enqueued ~half of post-`Stop()` events into a buffer nothing drains — counted as neither drop reason. Silent loss is the exact V1 bug V2 replaced; shutdown now gets deterministic precedence. Surfaced by writing the first `Publisher` test. **Hot path:** trigger JSON decode + `text/template` parse were running per event; `Campaign.Compile()` hoists both to once per cache refresh (the `CAMPAIGN_HOT_PATH=mysql` baseline still recompiles per event, deliberately). **Observability:** `messaging.consume.requeued{queue,redelivered}` — requeue is unbounded until V3-2, so a poison message spinning was previously invisible behind an empty DLQ. **Spec hygiene:** duplicate `BM‑30/31/32` (segment store vs stub receiver) renumbered to `BM‑43..45`; §3.1.4 → §3.1.4d; §5 out-of-scope reconciled with what shipped; §6 tier table filled for V2-2/V2-2x/V2-3 and the "render CPU / bitmap memory" guess replaced with the measured answer (shared-MySQL writes) in §1 and §6; §4 environment completed; §8 gains Q12–Q14 (send-gate placement, poison-message bounding, attribute staleness window); revision history reordered newest-first. |
| 0.2.5   | 2026-05-31 | Steve Weiland | V2 PR 13 (V2-3 Lean): batched idempotent enrollment. Migration 004 adds UNIQUE `(workspace_id, campaign_id, triggered_by)` → `INSERT … ON DUPLICATE KEY UPDATE` makes redelivery a no-op (BM‑100, partly closes BM‑78). New `internal/campaign/batcher.go` coalesces enrollment inserts into one multi-row commit per flush (`BATCH_MAX`/`BATCH_WINDOW`); Submit blocks until commit then acks (BM‑101/102). `CAMPAIGN_BATCH` A/B toggle (BM‑103). Measured: backlog 156,961 → 166 with batching; 0 duplicate enrollments across 700k+ rows. Two-phase send-gate deferred (BM‑105). §3.2.3 filled. |
| 0.2.4b  | 2026-05-29 | Steve Weiland | V2 PR 12 (perf): MySQL pool default 25 → 50 (`MYSQL_MAX_OPEN_CONNS`, idle == open), `--max-connections=300` on the server so 3 services × 50 fits. Measured at the V2-2 write ceiling: intake **2,173 → 2,836/s**, p99 **306 → 197 ms** going 25 → 64 conns — requests were queuing on the pool *before* reaching the bottleneck, so the pool was throttling access to MySQL rather than MySQL being slower than measured. 50 banks most of the gain. *Recorded retroactively in 0.2.6: PR 12 shipped without a spec bump, and these numbers lived only in a comment in `internal/eventstore/eventstore.go`.* |
| 0.2.4   | 2026-05-29 | Steve Weiland | V2 PR 11 (V2-2c): campaign-worker hot-path repoint. New `internal/campaign/attrcache.go` (attributes from `people.changes` + boot backfill + MySQL fallback-on-miss) and `campaigncache.go` (per-workspace TTL list); `Processor` reads both from cache instead of per-event `ListByWorkspace` + `person.Get`. Per-replica ephemeral queue on the people.changes fanout keeps the cache complete at N replicas. `CAMPAIGN_HOT_PATH=cache\|mysql` escape hatch for A/B. Closes BM‑97 — worker backlog 9,134→83, ceiling migrated to shared-MySQL writes. Trade: per-event attribute reads become eventually consistent (fallback-on-miss bounds it). |
| 0.2.3   | 2026-05-29 | Steve Weiland | V2 PR 10 (V2-2b): `cmd/segment-worker/` — boot backfill (`person.All` + `eventstore.DistinctPersonEvents`) + incremental maintenance from `campaigns.fanout` (own `segments.index.<ws>` queues) and the new `people.changes` feed; `POST /internal/check` membership API. track-api repointed `/segments/{id}/check` at it (`SEGMENT_INDEX_URL`) with scan fallback + `backend` log, and publishes merged attributes on `POST /people`. BM‑92..96. Verified via `make seed-segments` (all `backend=bitmap`). §3.2.2 filled. |
| 0.2.2   | 2026-05-29 | Steve Weiland | V2 PR 9 (V2-2a): `internal/segmentidx` roaring-bitmap engine — base bitmaps (eventSeen / attrEq / attrExists), incremental `Observe*` (move-on-update, clear-on-delete), `Member` over the `segment.Condition` tree with bitmap leaves, unknown-person-non-member. 9 tests. BM‑90/91. Adds `RoaringBitmap/roaring/v2`. |
| 0.2.1   | 2026-05-29 | Steve Weiland | V2 PR 8 (V2-1 ceiling): re-measured fan-out on the same hardware with a new Go driver (`cmd/loadgen/`) + live fan-out sampling (`chaos/watch-fanout.sh`, `chaos/ceiling-run.sh`). Closes BM‑87/88 — new ceiling **~2,800/s fan-out, zero loss (~14× V1)**, bottleneck named as per-event MySQL over the 25-conn pool (BM‑88 "MySQL connection-pool contention" candidate confirmed). Two latent Go HTTP connection-leak bugs found + fixed en route (default transport pool; undrained response body → ephemeral-port exhaustion under high `PREFETCH`); `PREFETCH` made host-tunable in compose. §3.2.1 gains the V2-1 ceiling + Lessons block; §6 tier table filled for V1 + V2-1. |
| 0.2.0   | 2026-05-21 | Steve Weiland | V2 PR 7 (V2-1b switch): in-process `Dispatcher` retired; `Publisher` publishes per accepted event to `campaigns.fanout` with routing_key=workspace_id (goroutine + bounded retry — never blocks the HTTP reply, BM-89); new `cmd/campaign-worker/` consumes per-workspace queues (`PREFETCH`=32, manual ack) and runs `campaign.Processor.Process`. Outcomes: Ack on success/no-match; NackRequeue on transient infra failure; NackDrop on terminal (person row missing) — routes to DLQ via queue-level `x-dead-letter-exchange`. `campaign_queue_dropped_total` retired (BM-86); replaced with `campaign_publish_dropped_total{workspace_id,reason}` for buffer overflow / retry exhaustion / shutdown drops. Closes BM-80..86 + BM-89. Re-measurement (BM-87/88) lands in V2 PR 8. |
| 0.1.6   | 2026-05-21 | Steve Weiland | V2 PR 6 (V2-1a infra): RabbitMQ 3.13 added to compose; `internal/amqpx/` lifted from Build 5 (Connect/Publish/Consume + W3C traceparent over AMQP headers + Outcome enum + consume duration/dropped metrics); `internal/campaign/topology.go` declares `campaigns.fanout` (direct) + `campaigns.fanout.dlx` (fanout) + `campaigns.fanout.dlq` (durable, bound to DLX) at track-api startup. Dispatch path unchanged — broker is wired but unused. |
| 0.1.5   | 2026-05-21 | Steve Weiland | V1 PR 5 closes V1: `chaos/load-v1.sh` (bash+curl closed-loop driver, `make load-{prep,quick,soak}`); soak measured 568 events/s intake, ~196 events/s fan-out, ~69% drop at the in-process channel. Bottleneck named: single Dispatcher consumer doing four serial MySQL hops + one sync HTTP per event. §3.2 V2 — Throughput rewritten: V2-1 RabbitMQ async fan-out spelled out as `BM‑80..89` (per-workspace queues from day 1, durable + manual-ack, DLQ topology established, V2-1 writeup names the new ceiling); V2-2 + V2-3 stubbed for their PR time. README gains the V1 ceiling section with hardware banner. |
| 0.1.4   | 2026-05-21 | Steve Weiland | Spelling sweep: British → American. Schema rename `journey_enrolments` → `journey_enrollments`, column `enrolment_id` → `enrollment_id`, indexes `idx_enrolments_*` → `idx_enrollments_*` (`enrolled_at` kept — past-tense spelling is identical in both dialects). Go identifiers `EnrolmentID`/`enrolID`/`insertEnrolment` retitled. Prose `behaviour`/`behavioural`/`enrol(s)` retitled across spec/README/comments. No behavior change; live MySQL needs a one-time `RENAME TABLE` (or `make down -v && make up` reset). |
| 0.1.3   | 2026-05-21 | Steve Weiland | V1 PR 4: campaign trigger + in-process fan-out. `BM‑70..79` added — `POST/GET /campaigns`, `Dispatcher` (buffered channel + single consumer goroutine, drop-on-full with Prometheus counter, async `campaign.process` span joined to the producer trace), trigger reuses `segment.Condition` verbatim with a single-event `EvalContext` (so `event_seen` matches the inbound event), Go `text/template` rendering with `missingkey=zero`, `journey_enrollments` audit insert per dispatch. The kickoff-PR direct `forwardToStub` smoke path is now routed through the Dispatcher's `dispatch` HTTP call. §4 I/O surface gains `/campaigns`. |
| 0.1.2   | 2026-05-21 | Steve Weiland | V1 PR 3: segment store + scan-based evaluator. `BM‑30..38` added — six-op condition tree (and/or/not/attr_eq/attr_exists/event_seen), depth cap 8, structural JSON equality. `Condition` API will be reused by the V1 PR 4 campaign trigger. §4 I/O surface gains `/segments` + check endpoint. Q10–Q11 resolved (scan-based V1, V2 swaps to bitmaps). |
| 0.1.1   | 2026-05-21 | Steve Weiland | V1 PR 2: person store. `BM‑26..29` added — `POST/GET /people` and event-path person auto-create. JSON_MERGE_PATCH semantics (RFC 7396) for attribute upserts; null-value-deletes-key documented. §4 I/O surface updated. |
| 0.1     | 2026-05-20 | Steve Weiland | V1 draft. Two services, MySQL, in-process channel, scan-based segment eval (placeholder — V1 PR 2 wires it). Resolved Q1–Q9. |
