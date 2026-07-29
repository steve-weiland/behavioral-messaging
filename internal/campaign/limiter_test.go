package campaign

import (
	"context"
	"sync"
	"testing"
	"time"
)

// BM-130's point: a noisy workspace must not be able to consume the whole
// global budget. Per-workspace prefetch already bounded each tenant's
// DELIVERIES (amqpx uses Qos global=false); what was missing was a bound on
// concurrent WORK against the shared MySQL pool.
func TestLimiter_PerWorkspaceCap(t *testing.T) {
	l := NewLimiter(100, 2) // generous global, tight per-workspace
	ctx := context.Background()

	r1, err := l.Acquire(ctx, "noisy")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	r2, err := l.Acquire(ctx, "noisy")
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if got := l.InFlight("noisy"); got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}

	// A third for the same workspace must block…
	blocked := make(chan struct{})
	go func() {
		r3, err := l.Acquire(ctx, "noisy")
		if err == nil {
			r3()
		}
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("third acquire for a capped workspace did not block")
	case <-time.After(80 * time.Millisecond):
	}

	// …while a different workspace is served immediately. This is the whole
	// isolation claim: one tenant at its cap does not stall another.
	quietCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rq, err := l.Acquire(quietCtx, "quiet")
	if err != nil {
		t.Fatalf("quiet workspace was starved by a capped noisy one: %v", err)
	}
	rq()

	r1()
	<-blocked // the queued acquire proceeds once a slot frees
	r2()
}

func TestLimiter_GlobalCap(t *testing.T) {
	// Global ceiling below the sum of per-workspace caps: the aggregate bound
	// is what keeps in-flight work inside the MySQL pool.
	l := NewLimiter(2, 5)
	ctx := context.Background()
	r1, _ := l.Acquire(ctx, "a")
	r2, _ := l.Acquire(ctx, "b")

	short, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(short, "c"); err == nil {
		t.Fatal("acquired past the global ceiling")
	}
	r1()
	r2()
}

func TestLimiter_ReleaseIsIdempotent(t *testing.T) {
	l := NewLimiter(1, 1)
	release, err := l.Acquire(context.Background(), "ws")
	if err != nil {
		t.Fatal(err)
	}
	release()
	release() // must not free a second slot
	if got := l.InFlight("ws"); got != 0 {
		t.Errorf("in-flight = %d, want 0", got)
	}
	// The single slot is available exactly once.
	if _, err := l.Acquire(context.Background(), "ws"); err != nil {
		t.Fatalf("slot not reusable after release: %v", err)
	}
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(short, "ws"); err == nil {
		t.Error("double release leaked a slot")
	}
}

func TestLimiter_ConcurrentAcquireRespectsCaps(t *testing.T) {
	l := NewLimiter(4, 4)
	var mu sync.Mutex
	peak, cur := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := l.Acquire(context.Background(), "ws")
			if err != nil {
				return
			}
			mu.Lock()
			cur++
			if cur > peak {
				peak = cur
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			cur--
			mu.Unlock()
			rel()
		}()
	}
	wg.Wait()
	if peak > 4 {
		t.Errorf("peak concurrency %d exceeded the cap of 4", peak)
	}
}
