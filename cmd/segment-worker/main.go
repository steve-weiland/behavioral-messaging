// segment-worker: V2-2 roaring-bitmap segment index service.
//
// Owns the in-memory bitmap index (internal/segmentidx) and keeps it
// current from two streams, then answers membership over HTTP so
// track-api's /segments/{id}/check no longer scans MySQL per person.
//
// Boot:
//   - Backfill the index from current MySQL state: every person's
//     attributes (person.All) and every distinct (person, event_name)
//     (eventstore.DistinctPersonEvents).
//
// Steady state:
//   - event_seen: bind a per-workspace queue (segments.index.<ws>) to
//     the existing campaigns.fanout exchange. A SEPARATE queue name from
//     campaign-worker's campaigns.fanout.<ws>, so the direct exchange
//     delivers a COPY to each — we observe events without stealing them
//     from the campaign consumer.
//   - attributes: consume the people.changes feed (track-api publishes
//     the merged attributes on every upsert).
//
// Serve:
//   - POST /internal/check {workspace_id, person_id, definition} -> {member}.
//     track-api supplies the segment definition it already loaded, so this
//     service needs no segment store on the read path — it's a pure
//     membership oracle over the maintained base bitmaps.
//
// At-least-once redelivery is harmless: Observe* is idempotent
// (set-a-bit), so a redelivered event/change just re-sets the same bit.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/steve-weiland/behavioral-messaging/internal/amqpx"
	"github.com/steve-weiland/behavioral-messaging/internal/campaign"
	"github.com/steve-weiland/behavioral-messaging/internal/event"
	"github.com/steve-weiland/behavioral-messaging/internal/eventstore"
	"github.com/steve-weiland/behavioral-messaging/internal/logsx"
	"github.com/steve-weiland/behavioral-messaging/internal/otelinit"
	"github.com/steve-weiland/behavioral-messaging/internal/peoplefeed"
	"github.com/steve-weiland/behavioral-messaging/internal/person"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
	"github.com/steve-weiland/behavioral-messaging/internal/segmentidx"
)

const serviceName = "segment-worker"

func main() {
	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.SetDefault(logsx.Init(serviceName, "0.1.5"))

	shutdownTrace, err := otelinit.Init(rootCtx, serviceName, "0.1.5")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdownTrace(context.Background()) }()

	db, err := openDBWithRetry(envOr("MYSQL_DSN", "bm:bm@tcp(mysql:3306)/bm?parseTime=true"), 30*time.Second)
	if err != nil {
		log.Fatalf("mysql connect: %v", err)
	}
	defer db.Close()

	idx := segmentidx.New()
	if err := backfill(rootCtx, db, idx); err != nil {
		log.Fatalf("backfill: %v", err)
	}

	amqpURL := envOr("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
	amqpConn, amqpCh, err := amqpx.ConnectWithRetry(amqpURL, 30*time.Second)
	if err != nil {
		log.Fatalf("amqp connect: %v", err)
	}
	defer amqpConn.Close()
	defer amqpCh.Close()

	// Ensure the exchanges exist (idempotent) even if this worker boots
	// before track-api: campaigns.fanout for events, people.changes for
	// attribute updates.
	if err := campaign.DeclareProducerTopology(amqpCh); err != nil {
		log.Fatalf("declare campaigns topology: %v", err)
	}
	// Zombie-consumer guard: a channel error ends every delivery loop
	// silently; exit and let the restart policy resubscribe (BM review #5).
	amqpx.ExitOnClose(amqpConn, amqpCh, serviceName)

	prefetch := envInt("PREFETCH", 32)

	// Attribute feed: one queue bound to the people.changes fanout.
	peopleQueue, err := peoplefeed.DeclareConsumerQueue(amqpCh, "segments.index.people")
	if err != nil {
		log.Fatalf("declare people queue: %v", err)
	}
	if err := amqpx.Consume(rootCtx, amqpCh, peopleQueue, prefetch, serviceName+".people", peopleHandler(idx)); err != nil {
		log.Fatalf("consume %s: %v", peopleQueue, err)
	}

	// Event feed: a per-workspace queue bound to campaigns.fanout, named
	// distinctly so we don't compete with campaign-worker.
	workspaces, err := listWorkspaces(rootCtx, db)
	if err != nil {
		log.Fatalf("list workspaces: %v", err)
	}
	for _, ws := range workspaces {
		q, err := ensureEventQueue(amqpCh, ws)
		if err != nil {
			log.Fatalf("ensure event queue %s: %v", ws, err)
		}
		if err := amqpx.Consume(rootCtx, amqpCh, q, prefetch, serviceName+".events."+ws, eventHandler(idx)); err != nil {
			log.Fatalf("consume %s: %v", q, err)
		}
		slog.Info("event consumer started", slog.String("workspace_id", ws), slog.String("queue", q))
	}
	slog.Info("segment-worker ready",
		slog.Int("workspaces", len(workspaces)),
		slog.Int("prefetch", prefetch))

	// Membership API + liveness.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/internal/check", checkHandler(idx))
	addr := ":" + envOr("PORT", "8083")
	httpServer := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("listening", slog.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-rootCtx.Done()
	slog.Info("shutdown")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = httpServer.Shutdown(stopCtx)
}

// backfill rebuilds the index from current MySQL state before the live
// streams take over. Idempotent with the streams: a person/event seen by
// both backfill and a redelivery just re-sets the same bit.
func backfill(ctx context.Context, db *sql.DB, idx *segmentidx.Index) error {
	people, err := person.All(ctx, db)
	if err != nil {
		return err
	}
	for _, p := range people {
		attrs, err := decodeAttrs(p.Attributes)
		if err != nil {
			slog.WarnContext(ctx, "backfill: skipping person with bad attributes",
				slog.String("workspace_id", p.WorkspaceID),
				slog.String("person_id", p.PersonID),
				slog.Any("error", err))
			continue
		}
		idx.SetAttributes(p.WorkspaceID, p.PersonID, attrs)
	}

	events, err := eventstore.DistinctPersonEvents(ctx, db)
	if err != nil {
		return err
	}
	for _, pe := range events {
		idx.ObserveEvent(pe.WorkspaceID, pe.PersonID, pe.Name)
	}
	slog.InfoContext(ctx, "backfill complete",
		slog.Int("people", len(people)),
		slog.Int("distinct_person_events", len(events)))
	return nil
}

// eventHandler observes event_seen membership. Decode failure is a
// terminal drop (poison message) — Ack so it doesn't loop; the index
// is best-effort and there's no DLQ on this feed.
func eventHandler(idx *segmentidx.Index) amqpx.Handler {
	return func(ctx context.Context, d amqp.Delivery) amqpx.Outcome {
		var ev event.Event
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			slog.ErrorContext(ctx, "segment-worker: bad event body, dropping",
				slog.String("message_id", d.MessageId), slog.Any("error", err))
			return amqpx.OutcomeAck
		}
		idx.ObserveEvent(ev.WorkspaceID, ev.PersonID, ev.Name)
		return amqpx.OutcomeAck
	}
}

// peopleHandler reconciles a person's attribute bitmaps from the
// people.changes feed.
func peopleHandler(idx *segmentidx.Index) amqpx.Handler {
	return func(ctx context.Context, d amqp.Delivery) amqpx.Outcome {
		var c peoplefeed.Change
		if err := json.Unmarshal(d.Body, &c); err != nil {
			slog.ErrorContext(ctx, "segment-worker: bad people change body, dropping",
				slog.String("message_id", d.MessageId), slog.Any("error", err))
			return amqpx.OutcomeAck
		}
		attrs, err := decodeAttrs(c.Attributes)
		if err != nil {
			slog.ErrorContext(ctx, "segment-worker: bad attributes in change, dropping",
				slog.String("workspace_id", c.WorkspaceID),
				slog.String("person_id", c.PersonID), slog.Any("error", err))
			return amqpx.OutcomeAck
		}
		idx.SetAttributes(c.WorkspaceID, c.PersonID, attrs)
		return amqpx.OutcomeAck
	}
}

type checkRequest struct {
	WorkspaceID string          `json:"workspace_id"`
	PersonID    string          `json:"person_id"`
	Definition  json.RawMessage `json:"definition"`
}

type checkResponse struct {
	Member bool `json:"member"`
}

// checkHandler answers membership from the bitmaps. track-api supplies
// the definition, so no segment store lookup is needed here.
func checkHandler(idx *segmentidx.Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req checkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		cond, err := segment.DecodeCondition(req.Definition)
		if err != nil {
			http.Error(w, "definition decode failed", http.StatusBadRequest)
			return
		}
		member := idx.Member(req.WorkspaceID, cond, req.PersonID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(checkResponse{Member: member})
	}
}

// decodeAttrs turns a stored attributes JSON object into the map the
// engine consumes. An empty/absent object yields an empty map (clears
// all attributes for the person).
func decodeAttrs(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	return m, nil
}

// ensureEventQueue declares this worker's per-workspace event queue and
// binds it to campaigns.fanout with routing_key=workspaceID. The name is
// deliberately distinct from campaign-worker's queue so both receive a
// copy from the direct exchange.
func ensureEventQueue(ch *amqp.Channel, workspaceID string) (string, error) {
	name := "segments.index." + workspaceID
	if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		return "", err
	}
	if err := ch.QueueBind(name, workspaceID, campaign.ExchangeFanout, false, nil); err != nil {
		return "", err
	}
	return name, nil
}

func listWorkspaces(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT workspace_id FROM workspaces`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ws string
		if err := rows.Scan(&ws); err != nil {
			return nil, err
		}
		out = append(out, ws)
	}
	return out, rows.Err()
}

func openDBWithRetry(dsn string, total time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(total)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := eventstore.Open(dsn)
		if err == nil {
			return db, nil
		}
		lastErr = err
		slog.Warn("mysql not ready, retrying", slog.Any("error", err))
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
