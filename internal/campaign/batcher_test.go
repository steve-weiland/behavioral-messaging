package campaign

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingExec captures each flush's SQL + arg count and returns a
// configurable error.
type recordingExec struct {
	mu       sync.Mutex
	flushes  []int // rows per flush (args/5)
	lastSQL  string
	failWith error
	calls    int32
}

func (r *recordingExec) exec(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.mu.Lock()
	r.flushes = append(r.flushes, len(args)/5)
	r.lastSQL = query
	r.mu.Unlock()
	atomic.AddInt32(&r.calls, 1)
	return nil, r.failWith
}

func row(i string) enrollRow {
	return enrollRow{workspaceID: "ws", enrollmentID: i, campaignID: "c", personID: "p", triggeredBy: i}
}

func TestBatcherFlushOnSize(t *testing.T) {
	rec := &recordingExec{}
	b := newBatcher(rec.exec, 4, time.Hour) // window won't fire; size triggers
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if err := b.Submit(context.Background(), row(string(rune('a'+n)))); err != nil {
				t.Errorf("submit: %v", err)
			}
		}(i)
	}
	wg.Wait()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	total := 0
	for _, n := range rec.flushes {
		total += n
	}
	if total != 4 {
		t.Fatalf("flushed %d rows, want 4 (flushes=%v)", total, rec.flushes)
	}
	if !strings.Contains(rec.lastSQL, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("flush SQL not idempotent: %s", rec.lastSQL)
	}
}

func TestBatcherFlushOnWindow(t *testing.T) {
	rec := &recordingExec{}
	b := newBatcher(rec.exec, 1000, 20*time.Millisecond) // size won't trigger; window will
	b.Start()
	defer b.Stop()

	// One lonely row should still flush once the window elapses.
	if err := b.Submit(context.Background(), row("solo")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.flushes) != 1 || rec.flushes[0] != 1 {
		t.Fatalf("want one flush of 1 row, got %v", rec.flushes)
	}
}

func TestBatcherErrorOnFlush(t *testing.T) {
	boom := errors.New("db down")
	rec := &recordingExec{failWith: boom}
	b := newBatcher(rec.exec, 1, time.Hour) // every row flushes immediately
	b.Start()
	defer b.Stop()

	if err := b.Submit(context.Background(), row("x")); !errors.Is(err, boom) {
		t.Fatalf("want flush error propagated, got %v", err)
	}
}

func TestBatcherSubmitAfterStop(t *testing.T) {
	rec := &recordingExec{}
	b := newBatcher(rec.exec, 4, time.Hour)
	b.Start()
	b.Stop()
	if err := b.Submit(context.Background(), row("late")); !errors.Is(err, errBatcherClosed) {
		t.Fatalf("want errBatcherClosed after Stop, got %v", err)
	}
}

// BM-100/101: the flush must be ONE multi-row statement (that's the point —
// amortizing the per-commit fsync) and it must be idempotent, so an
// at-least-once redelivery re-inserts as a no-op instead of double-enrolling.
// The live-DB run showed 0 duplicates across 700k+ rows; this pins the SQL
// contract that makes that true.
func TestBatcherFlushSQLIsMultiRowAndIdempotent(t *testing.T) {
	rec := &recordingExec{}
	b := newBatcher(rec.exec, 3, time.Hour)
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for _, id := range []string{"e1", "e2", "e3"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := b.Submit(context.Background(), row(id)); err != nil {
				t.Errorf("Submit(%s): %v", id, err)
			}
		}(id)
	}
	wg.Wait()

	rec.mu.Lock()
	sqlText, flushes := rec.lastSQL, append([]int(nil), rec.flushes...)
	rec.mu.Unlock()

	if len(flushes) != 1 || flushes[0] != 3 {
		t.Fatalf("flushes = %v, want exactly one flush of 3 rows", flushes)
	}
	if !strings.Contains(sqlText, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("flush SQL is not idempotent — missing ON DUPLICATE KEY UPDATE:\n%s", sqlText)
	}
	// Three value tuples in one statement, not three statements.
	if got := strings.Count(sqlText, "(?,?,?,?,?)"); got != 3 {
		t.Errorf("value tuples = %d, want 3 in a single statement:\n%s", got, sqlText)
	}
	if n := strings.Count(strings.ToUpper(sqlText), "INSERT INTO"); n != 1 {
		t.Errorf("INSERT statements = %d, want 1", n)
	}
}
