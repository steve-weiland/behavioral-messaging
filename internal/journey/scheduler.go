package journey

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/steve-weiland/behavioral-messaging/internal/person"
	"github.com/steve-weiland/behavioral-messaging/internal/segment"
)

// Scheduler advances journey runs (V3-1b, BM-115..119).
//
// Shape, and why:
//
//   - Runs are claimed in bounded batches with FOR UPDATE SKIP LOCKED, and the
//     claim COMMITS before any step work happens (BM-115). That is what makes
//     N replicas safe with no distributed lock, and what keeps a slow send from
//     holding row locks.
//   - Idle ticks sleep until MIN(wake_at) rather than polling a fixed interval
//     (BM-116). The scheduler shares the write bottleneck it is adding load to;
//     spending that budget on empty SELECTs is the mistake V2-2 spent three PRs
//     undoing on the fan-out path.
//   - A claim carries a LEASE. If this process dies mid-step, the run becomes
//     reclaimable once claim_expires_at passes (BM-117) — no transaction is held
//     across the HTTP send, which is why reclaim must be paired with the
//     send-gate below.
type Scheduler struct {
	db      *sql.DB
	id      string // claimed_by — identifies this replica
	stubURL string
	hc      *http.Client

	batch       int
	concurrency int  // BM-140: bounded parallelism over a claimed batch
	fair        bool // BM-131: divide each batch across workspaces with due work
	tickN       int
	lease       time.Duration
	maxSleep    time.Duration
	stuckPol    string // send-gate stuck policy: "skip" | "retry" (Q16)
	stuckAfter  time.Duration

	versions sync.Map // (ws,journey,version) -> *Journey, immutable so cached forever

	steps     metric.Int64Counter
	claims    metric.Int64Histogram
	gateSkips metric.Int64Counter
	leaseLost metric.Int64Counter
}

// SchedulerConfig holds the knobs. Every one is env-driven in main.
type SchedulerConfig struct {
	ID          string
	StubURL     string
	Batch       int
	Concurrency int
	Fair        bool
	Lease       time.Duration
	MaxSleep    time.Duration
	StuckPolicy string
	StuckAfter  time.Duration
}

func NewScheduler(db *sql.DB, hc *http.Client, cfg SchedulerConfig) *Scheduler {
	// A zero here is fatal, not merely slow: the batch semaphore would be an
	// UNBUFFERED channel, every goroutine would block on its send, and
	// wg.Wait() would never return — the scheduler hangs after its first claim
	// with runs stranded in 'running'. Cost me a debugging round when a config
	// patch silently failed to apply, so it's guarded rather than trusted.
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Batch < 1 {
		cfg.Batch = 1
	}
	m := otel.Meter("journeys")
	steps, err := m.Int64Counter("journey_steps_total",
		metric.WithDescription("Journey steps executed. Labels: type, outcome."))
	if err != nil {
		panic(err)
	}
	claims, err := m.Int64Histogram("journey_claim_batch_runs",
		metric.WithDescription("Runs claimed per scheduler tick."))
	if err != nil {
		panic(err)
	}
	skips, err := m.Int64Counter("journey_send_gate_skips_total",
		metric.WithDescription("Sends skipped by the idempotency gate. Labels: reason."))
	if err != nil {
		panic(err)
	}
	leaseLost, err := m.Int64Counter("journey_lease_lost_total",
		metric.WithDescription("Transitions discarded because the run was reclaimed while this replica held a stale lease. Labels: verb."))
	if err != nil {
		panic(err)
	}
	return &Scheduler{
		db: db, id: cfg.ID, stubURL: cfg.StubURL, hc: hc,
		batch: cfg.Batch, concurrency: cfg.Concurrency, fair: cfg.Fair, lease: cfg.Lease, maxSleep: cfg.MaxSleep,
		stuckPol: cfg.StuckPolicy, stuckAfter: cfg.StuckAfter,
		steps: steps, claims: claims, gateSkips: skips, leaseLost: leaseLost,
	}
}

// Run loops until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	slog.Info("journey scheduler started",
		slog.String("id", s.id), slog.Int("batch", s.batch),
		slog.Duration("lease", s.lease), slog.Duration("max_sleep", s.maxSleep),
		slog.Bool("fair_claim", s.fair), slog.Int("concurrency", s.concurrency),
		slog.String("send_gate_stuck_policy", s.stuckPol))
	for {
		n, err := s.tick(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("scheduler tick failed", slog.Any("error", err))
		}
		if ctx.Err() != nil {
			slog.Info("journey scheduler stopped", slog.String("id", s.id))
			return
		}
		// Only sleep when the batch came back empty. A full batch means there
		// is more due work, so go straight round again.
		if n == 0 {
			s.sleepUntilNextDue(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) (int, error) {
	claim := s.claim
	if s.fair {
		claim = s.claimFair
	}
	runs, err := claim(ctx)
	if err != nil {
		return 0, err
	}
	s.claims.Record(ctx, int64(len(runs)))
	if len(runs) == 0 {
		return 0, nil
	}
	// Execute the claimed batch CONCURRENTLY, bounded (BM-140).
	//
	// This was the V3 ceiling: the batch was claimed in one go but executed
	// with `for i := range runs { executeRun(...) }` — one run at a time in a
	// single goroutine, each doing a person read + gate insert + HTTP POST +
	// two updates. ~24ms serially, measured at 42 runs/s end-to-end while the
	// fan-out worker managed 637 events/s. MySQL was never the constraint;
	// the scheduler simply never did two things at once.
	//
	// Same shape the fan-out worker already uses: bounded concurrency, sized
	// so total in-flight DB work stays sane. The claim was always safe to
	// parallelize — each run is a distinct row already locked to this replica
	// by the lease.
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func(r *Run) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s.executeRun(ctx, r)
		}(&runs[i])
	}
	wg.Wait()
	return len(runs), nil
}

// claim locks a bounded batch of due runs and marks them running with a fresh
// lease, committing before any step executes.
//
// Reclaim is folded into the same query: a run left 'running' by a dead
// scheduler is due again once its lease expires.
//
// BM-131 — the claim is FAIR across workspaces. A plain `ORDER BY wake_at
// LIMIT n` lets one tenant's backlog of overdue runs fill every batch
// indefinitely: 10k overdue runs in workspace A means workspace B's single due
// run never gets claimed, no matter how long it waits. So the batch is divided
// between the workspaces that currently have due work, and the starting
// workspace rotates each tick so none is systematically first.
func (s *Scheduler) claimFair(ctx context.Context) ([]Run, error) {
	wss, err := s.dueWorkspaces(ctx)
	if err != nil {
		return nil, err
	}
	if len(wss) == 0 {
		return nil, nil
	}
	// Rotate the starting point so the same workspace isn't always served
	// first when the batch doesn't divide evenly.
	s.tickN++
	off := s.tickN % len(wss)

	per := s.batch / len(wss)
	if per < 1 {
		per = 1
	}
	var out []Run
	for i := 0; i < len(wss) && len(out) < s.batch; i++ {
		ws := wss[(off+i)%len(wss)]
		room := s.batch - len(out)
		if per < room {
			room = per
		}
		runs, err := s.claimWorkspace(ctx, ws, room)
		if err != nil {
			return out, err
		}
		out = append(out, runs...)
	}
	return out, nil
}

// dueWorkspaces lists workspaces with at least one due run. One indexed query
// per tick, served by idx_runs_due.
func (s *Scheduler) dueWorkspaces(ctx context.Context) ([]string, error) {
	const q = `
		SELECT DISTINCT workspace_id
		FROM journey_runs
		WHERE (status IN ('ready','waiting') AND (wake_at IS NULL OR wake_at <= NOW()))
		   OR (status = 'running' AND claim_expires_at IS NOT NULL AND claim_expires_at <= NOW())
	`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("due workspaces: %w", err)
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

func (s *Scheduler) claimWorkspace(ctx context.Context, workspaceID string, limit int) ([]Run, error) {
	return s.claimWhere(ctx, workspaceID, limit)
}

func (s *Scheduler) claim(ctx context.Context) ([]Run, error) {
	return s.claimWhere(ctx, "", s.batch)
}

// claimWhere is the shared claim body. An empty workspaceID claims across all
// workspaces (the unfair path, kept for the A/B).
func (s *Scheduler) claimWhere(ctx context.Context, workspaceID string, limit int) ([]Run, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	sel := `
		SELECT workspace_id, run_id, journey_id, journey_version, person_id, step_index, attempt
		FROM journey_runs
		WHERE ((status IN ('ready','waiting') AND (wake_at IS NULL OR wake_at <= NOW()))
		   OR (status = 'running' AND claim_expires_at IS NOT NULL AND claim_expires_at <= NOW()))
	`
	args := []any{}
	if workspaceID != "" {
		sel += ` AND workspace_id = ?`
		args = append(args, workspaceID)
	}
	sel += `
		ORDER BY wake_at IS NOT NULL, wake_at
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, sel, args...)
	if err != nil {
		return nil, fmt.Errorf("select due runs: %w", err)
	}
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.WorkspaceID, &r.RunID, &r.JourneyID, &r.JourneyVersion,
			&r.PersonID, &r.StepIndex, &r.Attempt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan due run: %w", err)
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("due rows: %w", err)
	}
	if len(out) == 0 {
		return nil, tx.Commit()
	}

	const upd = `
		UPDATE journey_runs
		SET status = 'running', claimed_by = ?, claim_expires_at = DATE_ADD(NOW(), INTERVAL ? SECOND)
		WHERE workspace_id = ? AND run_id = ?
	`
	for i := range out {
		if _, err := tx.ExecContext(ctx, upd, s.id, int(s.lease.Seconds()),
			out[i].WorkspaceID, out[i].RunID); err != nil {
			return nil, fmt.Errorf("mark running: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return out, nil
}

// sleepUntilNextDue waits for the next wake_at, capped at maxSleep so newly
// created runs are noticed promptly (BM-116).
func (s *Scheduler) sleepUntilNextDue(ctx context.Context) {
	d := s.maxSleep
	const q = `
		SELECT MIN(wake_at) FROM journey_runs
		WHERE status IN ('ready','waiting') AND wake_at IS NOT NULL
	`
	var next sql.NullTime
	if err := s.db.QueryRowContext(ctx, q).Scan(&next); err == nil && next.Valid {
		if until := time.Until(next.Time); until > 0 && until < d {
			d = until
		}
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// executeRun runs the current step and persists the outcome. Any error path
// releases the lease so the run is retried rather than stranded.
func (s *Scheduler) executeRun(ctx context.Context, r *Run) {
	tracer := otel.Tracer("journeys")
	ctx, span := tracer.Start(ctx, "journey.step",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("workspace_id", r.WorkspaceID),
			attribute.String("journey_id", r.JourneyID),
			attribute.String("run_id", r.RunID),
			attribute.Int("step_index", r.StepIndex),
		))
	defer span.End()

	// V3-1c: resolve the EXACT version this run pinned (BM-114), not whatever
	// the journey currently says. A person halfway through a journey finishes
	// on the definition they enrolled on.
	j, err := s.pinnedVersion(ctx, r)
	if err != nil {
		s.fail(ctx, r, "pinned journey definition unreadable: "+err.Error())
		return
	}

	// Past the end of the step list → the run is finished.
	if r.StepIndex < 0 || r.StepIndex >= len(j.Steps) {
		s.finish(ctx, r)
		return
	}
	step := &j.Steps[r.StepIndex]
	span.SetAttributes(attribute.String("step_type", step.Type))

	switch step.Type {
	case StepDelay:
		// Executing a delay means scheduling the NEXT step, so a run never
		// sits on the same delay twice.
		if step.EndsRun() {
			s.finish(ctx, r)
		} else {
			s.advance(ctx, r, r.StepIndex+1, time.Now().UTC().Add(time.Duration(step.Seconds)*time.Second))
		}
		s.steps.Add(ctx, 1, metric.WithAttributes(
			attribute.String("type", StepDelay), attribute.String("outcome", "scheduled"),
			attribute.String("workspace_id", r.WorkspaceID)))

	case StepBranch:
		per, err := person.Get(ctx, s.db, r.WorkspaceID, r.PersonID)
		if err != nil {
			s.release(ctx, r, "person read failed: "+err.Error())
			return
		}
		cond := step.Cond()
		if cond == nil {
			s.fail(ctx, r, "branch condition uncompiled")
			return
		}
		next := *step.IfFalse
		taken := "if_false"
		if cond.Evaluate(&segment.EvalContext{Person: per}) {
			next, taken = *step.IfTrue, "if_true"
		}
		slog.InfoContext(ctx, "journey branch taken",
			slog.String("run_id", r.RunID), slog.String("taken", taken), slog.Int("next", next))
		s.advance(ctx, r, next, time.Time{})
		s.steps.Add(ctx, 1, metric.WithAttributes(
			attribute.String("type", StepBranch), attribute.String("outcome", taken),
			attribute.String("workspace_id", r.WorkspaceID)))

	case StepSend:
		s.executeSend(ctx, r, j, step)

	default:
		s.fail(ctx, r, "unknown step type "+step.Type)
	}
}

// pinnedVersion resolves and compiles the definition a run enrolled on.
// Versions are immutable, so the compiled result is cached indefinitely — this
// keeps the per-step read off MySQL entirely after first touch, which matters
// now that steps run concurrently.
func (s *Scheduler) pinnedVersion(ctx context.Context, r *Run) (*Journey, error) {
	key := r.WorkspaceID + "\x00" + r.JourneyID + "\x00" + strconv.FormatUint(uint64(r.JourneyVersion), 10)
	if v, ok := s.versions.Load(key); ok {
		return v.(*Journey), nil
	}
	j, err := GetVersion(ctx, s.db, r.WorkspaceID, r.JourneyID, r.JourneyVersion)
	if err != nil {
		return nil, err
	}
	if err := j.Compile(); err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	s.versions.Store(key, j)
	return j, nil
}

// executeSend is the gated send (BM-118). The gate is what makes lease reclaim
// safe: without it, reclaiming a run whose previous holder died after the POST
// would deliver twice.
func (s *Scheduler) executeSend(ctx context.Context, r *Run, j *Journey, step *Step) {
	// Deterministic per (run, step) — a retry of the same step reuses the key,
	// which is what makes it a gate rather than a log.
	key := fmt.Sprintf("%s:%d", r.RunID, r.StepIndex)

	claimed, existing, err := s.gateClaim(ctx, r.WorkspaceID, key)
	if err != nil {
		s.release(ctx, r, "send gate failed: "+err.Error())
		return
	}
	if !claimed {
		switch existing {
		case "sent":
			// Already delivered by a previous attempt — advance without
			// re-sending. This is the exactly-once path under reclaim.
			s.gateSkips.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "already_sent")))
			slog.InfoContext(ctx, "send already completed — skipping (gate)",
				slog.String("run_id", r.RunID), slog.Int("step_index", r.StepIndex))
			s.afterStep(ctx, r, step)
			return
		default:
			// 'sending': another scheduler holds it, or a previous holder died
			// between the gate and the POST. Q16 — unknowable without asking
			// the downstream, so the behavior is a documented choice.
			stuck, serr := s.gateStuckFor(ctx, r.WorkspaceID, key)
			if serr == nil && stuck > s.stuckAfter && s.stuckPol == "retry" {
				slog.WarnContext(ctx, "send gate stuck — retrying per policy (may double-send)",
					slog.String("run_id", r.RunID), slog.Duration("stuck_for", stuck))
				// fall through to the send
			} else {
				s.gateSkips.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", "in_flight")))
				slog.WarnContext(ctx, "send in flight elsewhere — releasing run",
					slog.String("run_id", r.RunID), slog.Duration("stuck_for", stuck))
				s.release(ctx, r, "send in flight")
				return
			}
		}
	}

	// Attributes are read fresh at send time, not from a cache: a delay step
	// may have parked this run for days, so a value cached at enrollment would
	// be meaningless. This read is on the scheduler path, not the per-event
	// fan-out path, so it doesn't touch V2's measured hot path.
	per, err := person.Get(ctx, s.db, r.WorkspaceID, r.PersonID)
	if err != nil {
		s.release(ctx, r, "person read failed: "+err.Error())
		return
	}
	body, err := s.render(j, step, per, r)
	if err != nil {
		// A render failure is terminal — retrying renders the same failure.
		s.steps.Add(ctx, 1, metric.WithAttributes(
			attribute.String("type", StepSend), attribute.String("outcome", "render_failed")))
		s.fail(ctx, r, "render failed: "+err.Error())
		return
	}
	if err := s.post(ctx, body); err != nil {
		s.steps.Add(ctx, 1, metric.WithAttributes(
			attribute.String("type", StepSend), attribute.String("outcome", "post_failed")))
		// Transient: leave the gate row in 'sending' and release the run. The
		// retry ladder proper is V3-2 (BM-120..123).
		s.release(ctx, r, "stub post failed: "+err.Error())
		return
	}
	if err := s.gateMarkSent(ctx, r.WorkspaceID, key); err != nil {
		// The send happened; failing to record it risks a double-send on
		// reclaim. Log loudly — this is the window V3-2's retry semantics and
		// Q16's policy exist to bound.
		slog.ErrorContext(ctx, "send succeeded but gate not marked sent — double-send possible on reclaim",
			slog.String("run_id", r.RunID), slog.String("idem_key", key), slog.Any("error", err))
	}
	s.steps.Add(ctx, 1, metric.WithAttributes(
		attribute.String("type", StepSend), attribute.String("outcome", "sent"),
		attribute.String("workspace_id", r.WorkspaceID)))
	slog.InfoContext(ctx, "journey step sent",
		slog.String("run_id", r.RunID), slog.String("journey_id", r.JourneyID),
		slog.String("person_id", r.PersonID), slog.Int("step_index", r.StepIndex))
	s.afterStep(ctx, r, step)
}

// afterStep applies the step's Then: continue to the next index, or finish.
func (s *Scheduler) afterStep(ctx context.Context, r *Run, step *Step) {
	if step.EndsRun() {
		s.finish(ctx, r)
		return
	}
	s.advance(ctx, r, r.StepIndex+1, time.Time{})
}

// --- send gate (idempotency_keys) ---

// gateClaim tries to take the send slot. claimed=false means a row already
// exists; existing is its status.
func (s *Scheduler) gateClaim(ctx context.Context, workspaceID, key string) (claimed bool, existing string, err error) {
	const ins = `INSERT INTO idempotency_keys (workspace_id, idem_key, status, attempt) VALUES (?, ?, 'sending', 1)`
	if _, err := s.db.ExecContext(ctx, ins, workspaceID, key); err == nil {
		return true, "", nil
	} else if !isDuplicateKey(err) {
		return false, "", err
	}
	const sel = `SELECT status FROM idempotency_keys WHERE workspace_id = ? AND idem_key = ?`
	if err := s.db.QueryRowContext(ctx, sel, workspaceID, key).Scan(&existing); err != nil {
		return false, "", err
	}
	return false, existing, nil
}

func (s *Scheduler) gateStuckFor(ctx context.Context, workspaceID, key string) (time.Duration, error) {
	const q = `SELECT TIMESTAMPDIFF(SECOND, updated_at, NOW()) FROM idempotency_keys WHERE workspace_id = ? AND idem_key = ?`
	var secs int64
	if err := s.db.QueryRowContext(ctx, q, workspaceID, key).Scan(&secs); err != nil {
		return 0, err
	}
	return time.Duration(secs) * time.Second, nil
}

func (s *Scheduler) gateMarkSent(ctx context.Context, workspaceID, key string) error {
	const q = `UPDATE idempotency_keys SET status = 'sent' WHERE workspace_id = ? AND idem_key = ?`
	_, err := s.db.ExecContext(ctx, q, workspaceID, key)
	return err
}

// --- render + post ---

type renderContext struct {
	Person  *person.Person
	Attrs   map[string]any
	Now     time.Time
	Journey struct {
		ID      string
		Version uint32
	}
	StepIndex int
}

type stubPayload struct {
	JourneyID string `json:"journey_id"`
	RunID     string `json:"run_id"`
	StepIndex int    `json:"step_index"`
	PersonID  string `json:"person_id"`
	Rendered  string `json:"rendered"`
}

// render builds the message. Note the context has no .Event: a journey send
// may happen days after the triggering event, so exposing a stale event would
// invite templates that quietly lie. Capturing a trigger snapshot at
// enrollment is a future feature, not an accident of omission.
func (s *Scheduler) render(j *Journey, step *Step, per *person.Person, r *Run) ([]byte, error) {
	tpl := step.Tpl()
	if tpl == nil {
		return nil, errors.New("template uncompiled")
	}
	var attrs map[string]any
	if len(per.Attributes) > 0 {
		if err := json.Unmarshal(per.Attributes, &attrs); err != nil {
			attrs = map[string]any{}
		}
	}
	rc := renderContext{Person: per, Attrs: attrs, Now: time.Now().UTC(), StepIndex: r.StepIndex}
	rc.Journey.ID = j.JourneyID
	rc.Journey.Version = r.JourneyVersion

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, rc); err != nil {
		return nil, err
	}
	return json.Marshal(stubPayload{
		JourneyID: r.JourneyID, RunID: r.RunID, StepIndex: r.StepIndex,
		PersonID: r.PersonID, Rendered: buf.String(),
	})
}

func (s *Scheduler) post(ctx context.Context, body []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.stubURL+"/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	// Drain before close or the connection isn't pooled — the V2-1 lesson.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("stub status=%d", resp.StatusCode)
	}
	return nil
}

// --- state transitions ---

// casTransition runs a lease-guarded state transition: the WHERE clause is
// the compare (this replica still holds the run, still 'running'), rows-
// affected is the verdict. false = the lease expired and another replica
// reclaimed the run while this one stalled — the stale writer's transition
// is discarded and it must stop touching the run (BM-141). Without the
// guard, a late advance/release could rewind step_index, zero the attempt
// counter, or flip status under the legitimate holder.
func (s *Scheduler) casTransition(ctx context.Context, r *Run, verb, q string, args ...any) bool {
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		slog.ErrorContext(ctx, verb+" failed — run keeps its lease and will be reclaimed",
			slog.String("run_id", r.RunID), slog.Any("error", err))
		return false
	}
	n, err := res.RowsAffected()
	if err != nil {
		slog.ErrorContext(ctx, verb+": rows-affected unreadable", slog.Any("error", err))
		return false
	}
	if n == 0 {
		s.leaseLost.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", verb)))
		slog.WarnContext(ctx, "lease lost — transition discarded (run was reclaimed)",
			slog.String("verb", verb), slog.String("run_id", r.RunID),
			slog.String("claimed_by", s.id))
		return false
	}
	return true
}

// leaseGuard is the shared compare half of every transition.
const leaseGuard = ` AND claimed_by = ? AND status = 'running'`

// advance moves to nextStep. A zero wakeAt means immediately due.
func (s *Scheduler) advance(ctx context.Context, r *Run, nextStep int, wakeAt time.Time) {
	status, wake := StatusReady, any(nil)
	if !wakeAt.IsZero() {
		status, wake = StatusWaiting, wakeAt
	}
	const q = `
		UPDATE journey_runs
		SET step_index = ?, status = ?, wake_at = ?, attempt = 0, last_error = NULL,
		    claimed_by = NULL, claim_expires_at = NULL
		WHERE workspace_id = ? AND run_id = ?` + leaseGuard
	s.casTransition(ctx, r, "advance", q, nextStep, status, wake, r.WorkspaceID, r.RunID, s.id)
}

// finish marks a run done (fell off the end of the step list).
func (s *Scheduler) finish(ctx context.Context, r *Run) {
	const q = `
		UPDATE journey_runs SET status = 'done', wake_at = NULL,
		    claimed_by = NULL, claim_expires_at = NULL
		WHERE workspace_id = ? AND run_id = ?` + leaseGuard
	if !s.casTransition(ctx, r, "finish", q, r.WorkspaceID, r.RunID, s.id) {
		return
	}
	slog.InfoContext(ctx, "journey run done",
		slog.String("run_id", r.RunID), slog.String("journey_id", r.JourneyID),
		slog.String("person_id", r.PersonID))
}

// fail marks a run terminally failed — the error will recur, so retrying is
// pointless. Bounded retry with backoff for the transient cases is V3-2.
func (s *Scheduler) fail(ctx context.Context, r *Run, reason string) {
	const q = `
		UPDATE journey_runs SET status = 'failed', last_error = ?, wake_at = NULL,
		    claimed_by = NULL, claim_expires_at = NULL
		WHERE workspace_id = ? AND run_id = ?` + leaseGuard
	if !s.casTransition(ctx, r, "fail", q, truncate(reason, 500), r.WorkspaceID, r.RunID, s.id) {
		return
	}
	slog.ErrorContext(ctx, "journey run failed",
		slog.String("run_id", r.RunID), slog.String("reason", reason))
}

// schedulerBackoff is the FSM-side ladder (BM-123). The scheduler needs no
// broker for retries: wake_at already exists, so the delay mechanism IS the
// retry mechanism — and unlike a broker-side retry of a scheduler action, these
// survive a scheduler restart because they live in the row.
var schedulerBackoff = []time.Duration{
	5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
}

// release puts a run back as due after a transient failure, spaced by the
// backoff ladder. Past the end of the ladder the run is failed rather than
// retried forever — the bound is the point (BM-121/123).
func (s *Scheduler) release(ctx context.Context, r *Run, reason string) {
	next := int(r.Attempt) // attempt is the count of prior failures
	if next >= len(schedulerBackoff) {
		s.fail(ctx, r, fmt.Sprintf("retries exhausted after %d attempts: %s", r.Attempt, reason))
		return
	}
	delay := schedulerBackoff[next]
	const q = `
		UPDATE journey_runs
		SET status = 'ready', attempt = attempt + 1, last_error = ?,
		    wake_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
		    claimed_by = NULL, claim_expires_at = NULL
		WHERE workspace_id = ? AND run_id = ?` + leaseGuard
	if !s.casTransition(ctx, r, "release", q, truncate(reason, 500), int(delay.Seconds()),
		r.WorkspaceID, r.RunID, s.id) {
		return
	}
	slog.WarnContext(ctx, "journey run released for retry",
		slog.String("run_id", r.RunID), slog.String("reason", reason),
		slog.Uint64("attempt", uint64(r.Attempt+1)),
		slog.Int("of", len(schedulerBackoff)),
		slog.Duration("backoff", delay))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
