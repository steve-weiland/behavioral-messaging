package campaign

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Batcher coalesces journey_enrollments inserts from many concurrent
// in-flight events into one multi-row, idempotent INSERT per flush.
//
// Why: the V2-2 diagnostics showed the fan-out write side is the
// fsync-sensitive laggard — one commit (≈ two fsyncs) per dispatch. A
// flush groups up to maxRows enrollments (or every window) into a single
// commit, so the per-commit fsync cost amortizes across the batch and the
// queue stops backing up under write pressure.
//
// Durability: Submit blocks until the row's batch has COMMITTED, so the
// caller acks the AMQP delivery only after the enrollment is durable
// (preserves BM-84 at-least-once). The insert is ON DUPLICATE KEY UPDATE
// against uniq_enrollment_dispatch (migration 004), so a redelivered event
// re-inserts as a no-op — at-least-once stays idempotent.
// execFunc is the subset of *sql.DB the batcher needs (db.ExecContext),
// injectable so the batching logic is unit-testable without a live MySQL.
type execFunc func(ctx context.Context, query string, args ...any) (sql.Result, error)

type Batcher struct {
	exec    execFunc
	maxRows int
	window  time.Duration

	ch     chan enrollReq
	stop   chan struct{}
	wg     sync.WaitGroup
	closed int32

	batchRows metric.Int64Histogram
}

type enrollRow struct {
	workspaceID  string
	enrollmentID string
	campaignID   string
	personID     string
	triggeredBy  string
}

type enrollReq struct {
	row  enrollRow
	done chan error
}

var errBatcherClosed = errors.New("enrollment batcher closed")

// NewBatcher builds a batcher. maxRows caps a flush; window caps how long
// the first row in a batch waits for company. The in-flight channel is
// sized generously vs maxRows so Submit rarely blocks on enqueue.
func NewBatcher(db *sql.DB, maxRows int, window time.Duration) *Batcher {
	return newBatcher(db.ExecContext, maxRows, window)
}

func newBatcher(exec execFunc, maxRows int, window time.Duration) *Batcher {
	if maxRows < 1 {
		maxRows = 1
	}
	m := otel.Meter("campaigns")
	h, err := m.Int64Histogram(
		"campaign_enroll_batch_rows",
		metric.WithDescription("Rows per journey_enrollments flush."),
	)
	if err != nil {
		panic(err)
	}
	return &Batcher{
		exec:      exec,
		maxRows:   maxRows,
		window:    window,
		ch:        make(chan enrollReq, maxRows*4),
		stop:      make(chan struct{}),
		batchRows: h,
	}
}

// Start launches the flush goroutine.
func (b *Batcher) Start() {
	b.wg.Add(1)
	go b.run()
}

// Stop signals shutdown, flushes anything queued, and waits for the
// goroutine to exit. Submits after Stop fail fast.
func (b *Batcher) Stop() {
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return
	}
	close(b.stop)
	b.wg.Wait()
}

// Submit enqueues one enrollment and blocks until its batch commits.
// Returns the flush error (nil = durably committed) or ctx.Err(). A row
// queued just before a ctx timeout may still be flushed — harmless, the
// caller nacks and the idempotent re-insert dedups on redelivery.
func (b *Batcher) Submit(ctx context.Context, r enrollRow) error {
	if atomic.LoadInt32(&b.closed) == 1 {
		return errBatcherClosed
	}
	req := enrollReq{row: r, done: make(chan error, 1)}
	select {
	case b.ch <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return errBatcherClosed
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) run() {
	defer b.wg.Done()
	for {
		var batch []enrollReq
		select {
		case <-b.stop:
			b.drain()
			return
		case req := <-b.ch:
			batch = append(batch, req)
		}

		timer := time.NewTimer(b.window)
	collect:
		for len(batch) < b.maxRows {
			select {
			case req := <-b.ch:
				batch = append(batch, req)
			case <-timer.C:
				break collect
			case <-b.stop:
				b.flush(batch)
				b.drain()
				return
			}
		}
		timer.Stop()
		b.flush(batch)
	}
}

// drain flushes everything currently queued (called on shutdown). New
// Submits are already rejected by the closed flag.
func (b *Batcher) drain() {
	for {
		var batch []enrollReq
		for {
			select {
			case req := <-b.ch:
				batch = append(batch, req)
				continue
			default:
			}
			break
		}
		if len(batch) == 0 {
			return
		}
		b.flush(batch)
	}
}

// flush executes one multi-row idempotent insert and signals every waiter
// with the result.
func (b *Batcher) flush(batch []enrollReq) {
	if len(batch) == 0 {
		return
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO journey_enrollments (workspace_id, enrollment_id, campaign_id, person_id, triggered_by) VALUES ")
	args := make([]any, 0, len(batch)*5)
	for i := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?)")
		r := batch[i].row
		args = append(args, r.workspaceID, r.enrollmentID, r.campaignID, r.personID, r.triggeredBy)
	}
	// uniq_enrollment_dispatch makes a redelivered (workspace,campaign,event)
	// a no-op rather than a duplicate row.
	sb.WriteString(" ON DUPLICATE KEY UPDATE enrollment_id = enrollment_id")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := b.exec(ctx, sb.String(), args...)
	cancel()

	b.batchRows.Record(context.Background(), int64(len(batch)))
	for i := range batch {
		batch[i].done <- err
	}
}
