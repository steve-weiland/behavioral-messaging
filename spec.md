# Behavioral Messaging Platform

| Field   | Value         |
|---------|---------------|
| Version | 0.1 (V1 draft)|
| Author  | Steve Weiland |
| Date    | 2026-05-20    |
| Status  | Draft         |

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
| **V2 Throughput** | Roaring bitmaps for segments (incremental membership maintenance) + RabbitMQ async fan-out + idempotency keys + two-phase dispatch. | Render CPU / bitmap memory per workspace. |
| **V3 Durability** | Per-person journey state machine with delays (persisted FSM, `SELECT ... FOR UPDATE SKIP LOCKED` scheduler) + DLQs + retry semantics + per-workspace queue isolation. | Cross-workspace coordination overhead — V4 sharding territory. |

Multi-tenancy is **not** a tier — it's a cross-cutting V1 property
(`workspace_id` on every row, queue key, and span attribute).

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Workspace | Tenant boundary. All data is partitioned by `workspace_id`; no cross-workspace reads. |
| Person | One tracked end-user. Identified by `(workspace_id, person_id)`. Attributes are a JSON column on the `people` table. |
| Event | Append-only record of a behavioural action. PK `(workspace_id, event_id)`. Payload is JSON. |
| Track API | HTTP intake — `POST /events` with `X-Workspace-ID` header. Mirrors Customer.io's terminology. |
| Segment | Named set of persons matching a boolean condition tree over attributes + event history. V1 evaluates by scan; V2 by roaring bitmap with incremental membership maintenance. |
| Campaign | A trigger condition + a message template. V1: single-step send on event match. V3: multi-step journey with delays + branches. |
| Journey enrolment | The audit record when a campaign enrols a person, with `event_id` cross-reference. |
| Stub receiver | V1's stand-in for a delivery provider (ESP / APNs / FCM / webhook). HTTP receiver that logs the payload. |
| Idempotency key | `(workspace_id, person_id, campaign_id, schedule_key)` row guaranteeing at-most-once dispatch even under retry. V2 mechanism. |

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
| `BM‑13` | `journey_enrolments` + `idempotency_keys` tables **MUST** exist in V1 even though V1 doesn't yet write to them — adding tables later requires no code change in V2/V3. |

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

#### 3.1.4 Stub receiver

| ID | Requirement |
|----|-------------|
| `BM‑30` | `stub-receiver` **MUST** expose `POST /` accepting any JSON body up to 64 KiB. |
| `BM‑31` | The receiver **MUST** log one structured INFO line per delivery with `event_id` (when present) and `bytes`. |
| `BM‑32` | The receiver **MUST** return `200 OK` with `{"received":true}` on every successful read. |

#### 3.1.5 Observability

| ID | Requirement |
|----|-------------|
| `BM‑40` | Both services **MUST** initialise tracing + metrics via `internal/otelinit.Init` (Build 5 reuse) and structured JSON logging via `internal/logsx.Init`. |
| `BM‑41` | HTTP handlers **MUST** be wrapped in `otelhttp.NewHandler`. Outbound HTTP **MUST** use `otelhttp.NewTransport` so trace context propagates to `stub-receiver`. |
| `BM‑42` | A single `POST /events` call **MUST** produce one connected trace in Tempo spanning `track-api` server span → MySQL connection (in V1.1 once otelsql is wired) → outbound HTTP client → `stub-receiver` server span. |

#### 3.1.6 Operational

| ID | Requirement |
|----|-------------|
| `BM‑50` | Each service **MUST** handle `SIGTERM` with graceful shutdown (drain HTTP, flush OTel exporters, exit ≤ 10 s). |
| `BM‑51` | All services **MUST** read connection settings (`MYSQL_DSN`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `STUB_RECEIVER_URL`, `PORT`) from environment variables. |
| `BM‑52` | A `Makefile` **MUST** expose targets `up`, `down`, `logs`, `seed`, `mysql`, `load`, `showcase` mirroring Build 5's surface. |

#### 3.1.7 V1 scale ceiling (target on this hardware)

| ID | Requirement |
|----|-------------|
| `BM‑60` | V1 **MUST** sustain ≥ 200 sustained `POST /events` per second from one `track-api` replica on the user's hardware. Actual measured ceiling is documented in `README.md` per-tier section, with hardware specs at the top. |
| `BM‑61` | When the ceiling is reached, the **observed bottleneck** (MySQL write contention vs. HTTP goroutine count vs. stub-receiver latency) **MUST** be named in the V1 writeup. That observation seeds the V2 spec. |

### 3.2 V2 — Throughput

*Drafted in PR 2's spec bump. Three mechanism subsections: RabbitMQ async
fan-out, roaring bitmaps with incremental segment maintenance, idempotency
keys + two-phase dispatch.*

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

### HTTP — `stub-receiver`

```
POST /
Body: any JSON object ≤ 64 KiB
→ 200 OK { "received": true }
```

### Database

`migrations/001_v1.sql` — see file for ground truth. Tables:
`workspaces`, `people`, `events`, `journey_enrolments`,
`idempotency_keys`.

### Environment

```
MYSQL_DSN                       bm:bm@tcp(mysql:3306)/bm?parseTime=true
OTEL_EXPORTER_OTLP_ENDPOINT     otel-collector:4317
OTEL_METRICS_EXEMPLAR_FILTER    trace_based
STUB_RECEIVER_URL               http://stub-receiver:8081     (track-api only)
PORT                            8080 / 8081 per service
```

---

## 5. Out of Scope (V1)

Deferred to V2/V3 unless noted. Each line names the version that closes
the gap.

- **Multi-step journeys with delays** — V3.
- **Roaring bitmaps for segment evaluation** — V2.
- **RabbitMQ async fan-out** — V2 (V1 uses an in-process channel; V1 doesn't yet have multi-stage fan-out demanding a broker).
- **Idempotency keys + two-phase dispatch** — V2. PK on `events` provides storage-layer idempotency only.
- **Anonymous-to-identified merge** — V3 stretch.
- **Custom objects** (non-person entities) — V4+.
- **Liquid templating** — V4+; V1+V2 use Go `text/template`. Liquid is a yak.
- **`otelsql` for MySQL query spans** — small follow-up commit after the kickoff scaffolding lands.
- **Authentication, multi-tenancy enforcement at the API gateway, TLS** — V4+ / out of portfolio scope.
- **Horizontal scaling beyond one replica per service** — V3 introduces per-workspace worker pools; multi-replica auto-scaling is V4.
- **Liquid templating, send rate quotas, schema evolution tooling, Loki-side log retention policy** — V4+.

---

## 6. V1 → V3 Tier Table

Filled in incrementally as each tier ships. The artefact per row is a
bottleneck-migration narrative with hardware-pinned numbers.

| Tier | Mechanism added | Observed ceiling on this hardware | New bottleneck |
|---|---|---|---|
| V1 | Track API + MySQL + in-process channel + scan segment eval + stub HTTP dispatch | *(TBD when V1 load test runs)* | *(TBD)* |
| V2 | RabbitMQ fan-out + roaring bitmaps + idempotency | *(TBD)* | *(TBD)* |
| V3 | Journey FSM + DLQs + per-workspace queue isolation | *(TBD)* | V4 sharding |

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

*(none at V1; V2 + V3 will add tier-specific questions.)*

---

## 9. Revision History

| Version | Date       | Author        | Notes |
|---------|------------|---------------|-------|
| 0.1     | 2026-05-20 | Steve Weiland | V1 draft. Two services, MySQL, in-process channel, scan-based segment eval (placeholder — V1 PR 2 wires it). Resolved Q1–Q9. |
| 0.1.1   | 2026-05-21 | Steve Weiland | V1 PR 2: person store. `BM‑26..29` added — `POST/GET /people` and event-path person auto-create. JSON_MERGE_PATCH semantics (RFC 7396) for attribute upserts; null-value-deletes-key documented. §4 I/O surface updated. |
| 0.1.2   | 2026-05-21 | Steve Weiland | V1 PR 3: segment store + scan-based evaluator. `BM‑30..38` added — six-op condition tree (and/or/not/attr_eq/attr_exists/event_seen), depth cap 8, structural JSON equality. `Condition` API will be reused by the V1 PR 4 campaign trigger. §4 I/O surface gains `/segments` + check endpoint. Q10–Q11 resolved (scan-based V1, V2 swaps to bitmaps). |
