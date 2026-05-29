// loadgen — Go closed-loop load driver for the V2 fan-out path.
//
// The V1 bash+curl driver (chaos/load-v1.sh) tops out at ~557 events/s
// because of per-request process-spawn overhead — that ceiling is the
// driver's, not the stack's. This driver reuses keep-alive connections
// across N goroutines, so it can push the intake hard enough to find the
// real V2-1 ceiling: the single publish goroutine / 1024-deep buffer in
// internal/campaign/publisher.go, or the campaign-worker consume rate.
//
// Closed-loop: each of -c goroutines hot-loops POST /events against a pool
// of -people pre-identified plan=pro persons, for -d. The welcome_pro
// campaign is ensured at prep so every signed_up matches the trigger and
// the worker actually fans out.
//
// Output: events_sent, failures, non-202, achieved_rate, and intake
// latency p50/p90/p99/p99.9/max. Watch the fan-out side (queue depth,
// publish drops, worker CPU) separately via chaos/watch-fanout.sh.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		base        = flag.String("url", "http://localhost:8090", "track-api base URL")
		workspace   = flag.String("workspace", "ws_alpha", "X-Workspace-ID")
		people      = flag.Int("people", 50, "size of the pre-identified person pool")
		duration    = flag.Duration("d", 30*time.Second, "soak duration")
		concurrency = flag.Int("c", 100, "number of closed-loop worker goroutines")
		event       = flag.String("event", "signed_up", "event_name to POST")
		prep        = flag.Bool("prep", true, "create welcome_pro + identify the person pool before the soak")
	)
	flag.Parse()

	tr := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		MaxConnsPerHost:     *concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	fmt.Printf("== loadgen ==\nurl=%s workspace=%s people=%d duration=%s concurrency=%d event=%s\n\n",
		*base, *workspace, *people, *duration, *concurrency, *event)

	if *prep {
		if err := doPrep(client, *base, *workspace, *people); err != nil {
			fmt.Fprintf(os.Stderr, "prep failed: %v\n", err)
			os.Exit(1)
		}
	}

	var sent, fail, non202 int64
	// Per-worker latency samples merged at the end — avoids lock contention
	// on the hot path.
	lat := make([][]time.Duration, *concurrency)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	eventBody := func(pid string) []byte {
		return []byte(fmt.Sprintf(`{"person_id":%q,"event_name":%q,"payload":{}}`, pid, *event))
	}

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)*2862933555777941757 + 1))
			samples := make([]time.Duration, 0, 4096)
			for ctx.Err() == nil {
				pid := fmt.Sprintf("load_p_%d", rng.Intn(*people)+1)
				t0 := time.Now()
				code, err := postEvent(ctx, client, *base, *workspace, eventBody(pid))
				elapsed := time.Since(t0)
				if err != nil {
					if ctx.Err() != nil {
						break // shutdown, not a real failure
					}
					atomic.AddInt64(&fail, 1)
					continue
				}
				samples = append(samples, elapsed)
				if code == http.StatusAccepted {
					atomic.AddInt64(&sent, 1)
				} else {
					atomic.AddInt64(&non202, 1)
				}
			}
			lat[id] = samples
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	all := make([]time.Duration, 0, 1<<20)
	for _, s := range lat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	rate := float64(atomic.LoadInt64(&sent)) / elapsed.Seconds()
	fmt.Printf("== aggregate ==\n")
	fmt.Printf("events_sent     = %d\n", sent)
	fmt.Printf("failures        = %d\n", fail)
	fmt.Printf("non_202         = %d\n", non202)
	fmt.Printf("elapsed_s       = %.1f\n", elapsed.Seconds())
	fmt.Printf("achieved_rate   = %.0f events/s\n", rate)
	fmt.Printf("latency p50     = %s\n", pct(all, 0.50))
	fmt.Printf("latency p90     = %s\n", pct(all, 0.90))
	fmt.Printf("latency p99     = %s\n", pct(all, 0.99))
	fmt.Printf("latency p99.9   = %s\n", pct(all, 0.999))
	if len(all) > 0 {
		fmt.Printf("latency max     = %s\n", all[len(all)-1])
	}
}

func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i].Round(time.Microsecond)
}

func postEvent(ctx context.Context, c *http.Client, base, ws string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/events", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Workspace-ID", ws)
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

func doPrep(c *http.Client, base, ws string, people int) error {
	fmt.Println("[prep] ensure welcome_pro campaign")
	campaign := `{"campaign_id":"welcome_pro","name":"Welcome Pro users","trigger":{"op":"and","conditions":[{"op":"attr_eq","key":"plan","value":"pro"},{"op":"event_seen","name":"signed_up"}]},"template":"Welcome {{.Person.PersonID}} — your {{.Attrs.plan}} plan is live."}`
	// 409/2xx both fine — campaign may already exist from a prior run.
	if _, err := post(c, base+"/campaigns", ws, []byte(campaign)); err != nil {
		return err
	}
	fmt.Printf("[prep] identify %d people (plan=pro)\n", people)
	for i := 1; i <= people; i++ {
		body := fmt.Sprintf(`{"person_id":"load_p_%d","attributes":{"plan":"pro"}}`, i)
		if _, err := post(c, base+"/people", ws, []byte(body)); err != nil {
			return err
		}
	}
	fmt.Println("[prep] done")
	fmt.Println()
	return nil
}

func post(c *http.Client, url, ws string, body []byte) (int, error) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Workspace-ID", ws)
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}
