package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type backendFunc func(context.Context, []Input) ([]Output, error)

func (f backendFunc) Infer(ctx context.Context, in []Input) ([]Output, error) { return f(ctx, in) }
func echo(in []Input) []Output {
	out := make([]Output, len(in))
	for i, item := range in {
		out[i] = Output{Text: item.Text}
	}
	return out
}
func configForTest() Config {
	c := DefaultConfig()
	c.MaxOutstanding = 64
	c.PerTenantOutstanding = 32
	c.MaxTenants = 16
	c.MaxBatch = 1
	c.MaxBatchWait = 0
	return c
}
func newTestGateway(t *testing.T, c Config, b Backend) *Gateway {
	t.Helper()
	g, err := New(c, b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := g.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return g
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
func asyncInfer(g *Gateway, ctx context.Context, tenant string) <-chan error {
	out := make(chan error, 1)
	go func() { _, err := g.Infer(ctx, tenant, "input"); out <- err }()
	return out
}
func gatedBackend(started chan struct{}, release <-chan struct{}) Backend {
	var first sync.Once
	return backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
		first.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return echo(in), nil
		}
	})
}

func TestDynamicFullBatches(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 4
	c.MaxBatchWait = 500 * time.Millisecond
	c.BackendConcurrency = 1
	g := newTestGateway(t, c, CPUBackend{Rounds: 1})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := g.Infer(context.Background(), "tenant", "text")
			if err == nil && len(r.Output) != 64 {
				err = errors.New("wrong CPU fixture output")
			}
			errs <- err
		}()
	}
	wg.Wait()
	for i := 0; i < 8; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	s := g.Stats()
	if s.Batches != 2 || s.BatchSizes[4] != 2 || s.BackendCalls != 2 {
		t.Fatalf("batch stats: %+v", s)
	}
	copy := g.Stats()
	copy.Trace[0].Tenants[0] = "mutated"
	copy.BatchSizes[4] = 99
	if g.Stats().Trace[0].Tenants[0] == "mutated" || g.Stats().BatchSizes[4] != 2 {
		t.Fatal("Stats exposed mutable state")
	}
}

func TestRoundRobinTenantsWithPerTenantBacklog(t *testing.T) {
	c := configForTest()
	c.BackendConcurrency = 1
	started, release := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, gatedBackend(started, release))
	warm := asyncInfer(g, context.Background(), "warm")
	<-started
	results := []<-chan error{}
	for index, tenant := range []string{"A", "B", "C"} {
		for i := 0; i < 5; i++ {
			results = append(results, asyncInfer(g, context.Background(), tenant))
		}
		want := (index + 1) * 5
		waitFor(t, func() bool { return g.Stats().Queued == want })
	}
	close(release)
	if err := <-warm; err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	trace := g.Stats().Trace
	for i := 1; i < len(trace); i++ {
		want := []string{"A", "B", "C"}[(i-1)%3]
		if trace[i].Tenants[0] != want {
			t.Fatalf("unfair dispatch %d: got %s want %s", i, trace[i].Tenants[0], want)
		}
	}
}

func TestBatchResultsAreBoundToTheOriginalRequest(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 4
	c.MaxBatchWait = time.Second
	g := newTestGateway(t, c, backendFunc(func(_ context.Context, in []Input) ([]Output, error) {
		out := make([]Output, len(in))
		for i, item := range in {
			out[i].Text = item.Tenant + ":" + item.Text
		}
		return out, nil
	}))
	type answer struct {
		want   string
		result Result
		err    error
	}
	answers := make(chan answer, 4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			tenant, text := fmt.Sprintf("t%d", i), fmt.Sprintf("input%d", i)
			r, err := g.Infer(context.Background(), tenant, text)
			answers <- answer{tenant + ":" + text, r, err}
		}(i)
	}
	for i := 0; i < 4; i++ {
		a := <-answers
		if a.err != nil || a.result.Output != a.want {
			t.Fatalf("cross-request output: got %q want %q err=%v", a.result.Output, a.want, a.err)
		}
	}
}

func TestTenantEDFOrdersUrgentRequestAheadOfEarlierSlowRequest(t *testing.T) {
	c := configForTest()
	c.BackendConcurrency = 1
	started, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	order := []string{}
	g := newTestGateway(t, c, backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
		mu.Lock()
		order = append(order, in[0].Text)
		first := len(order) == 1
		mu.Unlock()
		if first {
			close(started)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
			}
		}
		return echo(in), nil
	}))
	warm := asyncInfer(g, context.Background(), "warm")
	<-started
	answers := make(chan error, 2)
	go func() { _, err := g.Infer(context.Background(), "A", "slow"); answers <- err }()
	waitFor(t, func() bool { return g.Stats().Queued == 1 })
	urgent, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _, err := g.Infer(urgent, "A", "urgent"); answers <- err }()
	waitFor(t, func() bool { return g.Stats().Queued == 2 })
	close(release)
	<-warm
	for i := 0; i < 2; i++ {
		if err := <-answers; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if order[1] != "urgent" || order[2] != "slow" {
		t.Fatalf("EDF violated: %v", order)
	}
}

func TestTenantAdmissionIsolation(t *testing.T) {
	c := configForTest()
	c.MaxOutstanding = 8
	c.PerTenantOutstanding = 2
	c.MaxTenants = 4
	c.BackendConcurrency = 1
	started, release := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, gatedBackend(started, release))
	first := asyncInfer(g, context.Background(), "A")
	<-started
	second := asyncInfer(g, context.Background(), "A")
	waitFor(t, func() bool { return g.Stats().Queued == 1 })
	if _, err := g.Infer(context.Background(), "A", "third"); !errors.Is(err, ErrTenantFull) {
		t.Fatalf("got %v", err)
	}
	other := asyncInfer(g, context.Background(), "B")
	waitFor(t, func() bool { return g.Stats().Queued == 2 })
	close(release)
	for _, result := range []<-chan error{first, second, other} {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
}

func TestGlobalAndTenantCardinalityAdmission(t *testing.T) {
	for _, global := range []bool{true, false} {
		t.Run(fmt.Sprint(global), func(t *testing.T) {
			c := configForTest()
			c.BackendConcurrency = 1
			c.MaxOutstanding = 4
			c.PerTenantOutstanding = 4
			c.MaxTenants = 1
			if global {
				c.MaxOutstanding = 2
				c.PerTenantOutstanding = 2
				c.MaxTenants = 2
			}
			started, release := make(chan struct{}), make(chan struct{})
			g := newTestGateway(t, c, gatedBackend(started, release))
			first := asyncInfer(g, context.Background(), "A")
			<-started
			var second <-chan error
			if global {
				second = asyncInfer(g, context.Background(), "B")
				waitFor(t, func() bool { return g.Stats().Queued == 1 })
			}
			_, err := g.Infer(context.Background(), "C", "rejected")
			want := ErrTenantLimit
			if global {
				want = ErrOverloaded
			}
			if !errors.Is(err, want) {
				t.Fatalf("got %v want %v", err, want)
			}
			close(release)
			<-first
			if second != nil {
				<-second
			}
		})
	}
}

func TestQueuedCancellationNeverRunsBackend(t *testing.T) {
	c := configForTest()
	c.BackendConcurrency = 1
	started, release := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, gatedBackend(started, release))
	first := asyncInfer(g, context.Background(), "A")
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	second := asyncInfer(g, ctx, "B")
	waitFor(t, func() bool { return g.Stats().Queued == 1 })
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return g.Stats().Queued == 0 })
	close(release)
	<-first
	waitFor(t, func() bool { return g.Stats().Finished == 2 })
	if g.Stats().BackendCalls != 1 || g.Stats().Cancelled != 1 {
		t.Fatal(g.Stats())
	}
}

func TestOneCancelledBatchMemberDoesNotCancelItsPeer(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 2
	c.MaxBatchWait = time.Second
	started, release := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, gatedBackend(started, release))
	ctx, cancel := context.WithCancel(context.Background())
	a := asyncInfer(g, ctx, "A")
	b := asyncInfer(g, context.Background(), "B")
	<-started
	cancel()
	if err := <-a; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-b; err != nil {
		t.Fatalf("peer was cancelled: %v", err)
	}
}

func TestAllCancelledBatchMembersCancelCooperativeBackend(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 2
	c.MaxBatchWait = time.Second
	started, aborted := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
		close(started)
		<-ctx.Done()
		close(aborted)
		return nil, ctx.Err()
	}))
	aCtx, aCancel := context.WithCancel(context.Background())
	bCtx, bCancel := context.WithCancel(context.Background())
	a := asyncInfer(g, aCtx, "A")
	b := asyncInfer(g, bCtx, "B")
	<-started
	aCancel()
	bCancel()
	<-a
	<-b
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("backend did not observe cancellation")
	}
	waitFor(t, func() bool { return g.Stats().Outstanding == 0 })
}

func TestDeadlineFlushesBeforeLongCollectionWindow(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 8
	c.MaxBatchWait = time.Second
	c.DispatchSlack = 150 * time.Millisecond
	g := newTestGateway(t, c, CPUBackend{Rounds: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r, err := g.Infer(ctx, "A", "text")
	if err != nil {
		t.Fatal(err)
	}
	s := g.Stats()
	if len(s.Trace) != 1 || s.Trace[0].Flush != "deadline" || r.QueueMicros <= 0 {
		t.Fatalf("deadline was not reflected in dispatch: %+v", s)
	}
}

func TestRetryRequiresOperatorSafetyAndTransientClassification(t *testing.T) {
	for _, tc := range []struct {
		name            string
		safe, temporary bool
		want            int32
		success         bool
	}{{"safe", true, true, 2, true}, {"unsafe", false, true, 1, false}, {"permanent", true, false, 1, false}} {
		t.Run(tc.name, func(t *testing.T) {
			c := configForTest()
			c.MaxAttempts = 3
			c.RetrySafe = tc.safe
			var calls atomic.Int32
			g := newTestGateway(t, c, backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
				if calls.Add(1) == 1 {
					return nil, &BackendError{Code: "fixture", Retryable: tc.temporary}
				}
				return echo(in), nil
			}))
			_, err := g.Infer(context.Background(), "A", "text")
			if (err == nil) != tc.success || calls.Load() != tc.want {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestBreakerFencesOldCompletionAndAllowsOneProbe(t *testing.T) {
	now := time.Unix(100, 0)
	b := newBreaker(1, time.Second, func() time.Time { return now })
	a, _ := b.allow()
	old, _ := b.allow()
	b.finish(a, false, true)
	b.finish(old, true, true)
	if b.snapshot() != "open" {
		t.Fatal("stale success closed circuit")
	}
	if _, ok := b.allow(); ok {
		t.Fatal("open circuit allowed request")
	}
	now = now.Add(time.Second)
	probe, ok := b.allow()
	if !ok || !probe.probe {
		t.Fatal("missing half-open probe")
	}
	if _, ok = b.allow(); ok {
		t.Fatal("concurrent half-open probe allowed")
	}
	b.finish(probe, true, true)
	if b.snapshot() != "closed" {
		t.Fatal("successful probe did not close")
	}
	a, _ = b.allow()
	b.finish(a, false, true)
	now = now.Add(time.Second)
	probe, _ = b.allow()
	b.finish(probe, false, false)
	if b.snapshot() != "open" {
		t.Fatal("aborted probe left half-open state stuck")
	}
}

func TestCircuitOpenSkipsBackend(t *testing.T) {
	c := configForTest()
	c.BreakerFailures = 1
	c.BreakerCooldown = time.Second
	var calls atomic.Int32
	g := newTestGateway(t, c, backendFunc(func(context.Context, []Input) ([]Output, error) {
		calls.Add(1)
		return nil, &BackendError{Code: "unavailable", Retryable: true}
	}))
	_, _ = g.Infer(context.Background(), "A", "text")
	_, err := g.Infer(context.Background(), "A", "text")
	if !errors.Is(err, ErrCircuitOpen) || calls.Load() != 1 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestLateBackendResultCannotPublishSuccessAfterItsBudget(t *testing.T) {
	c := configForTest()
	c.BackendTimeout = 5 * time.Millisecond
	c.BreakerFailures = 1
	g := newTestGateway(t, c, backendFunc(func(_ context.Context, in []Input) ([]Output, error) {
		time.Sleep(25 * time.Millisecond)
		return echo(in), nil
	}))
	_, err := g.Infer(context.Background(), "A", "late")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late backend output escaped deadline validation: %v", err)
	}
	if g.Stats().Breaker != "open" {
		t.Fatal("backend timeout was not classified as an availability failure")
	}
}

func TestBackendConcurrencyIsBounded(t *testing.T) {
	c := configForTest()
	c.MaxBatch = 2
	c.MaxBatchWait = 100 * time.Millisecond
	c.BackendConcurrency = 2
	var current, peak atomic.Int32
	release := make(chan struct{})
	g := newTestGateway(t, c, backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
		n := current.Add(1)
		defer current.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return echo(in), nil
		}
	}))
	results := []<-chan error{}
	for i := 0; i < 16; i++ {
		results = append(results, asyncInfer(g, context.Background(), fmt.Sprintf("t%d", i%4)))
	}
	waitFor(t, func() bool { return current.Load() == 2 })
	close(release)
	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if peak.Load() != 2 || g.Stats().PeakActiveBatches > 2 {
		t.Fatal("concurrency budget violated")
	}
}

func TestGracefulDrainAndForcedCancellation(t *testing.T) {
	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprint(forced), func(t *testing.T) {
			c := configForTest()
			c.BackendConcurrency = 1
			started, release := make(chan struct{}), make(chan struct{})
			g := newTestGateway(t, c, gatedBackend(started, release))
			first := asyncInfer(g, context.Background(), "A")
			<-started
			var second <-chan error
			if !forced {
				second = asyncInfer(g, context.Background(), "B")
				waitFor(t, func() bool { return g.Stats().Queued == 1 })
			}
			done := make(chan error, 1)
			go func() {
				timeout := time.Second
				if forced {
					timeout = 20 * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				done <- g.Close(ctx)
			}()
			waitFor(t, func() bool { return g.Stats().Draining })
			if _, err := g.Infer(context.Background(), "C", "late"); !errors.Is(err, ErrDraining) {
				t.Fatal(err)
			}
			if !forced {
				close(release)
			}
			err := <-done
			if forced && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if !forced && err != nil {
				t.Fatal(err)
			}
			<-first
			if second != nil {
				<-second
			}
			select {
			case <-g.Done():
			case <-time.After(time.Second):
				t.Fatal("workers did not stop")
			}
			if g.Stats().Outstanding != 0 {
				t.Fatal("drain leaked request slots")
			}
		})
	}
}

func TestAdmissionCloseRaceDoesNotLeakOrHang(t *testing.T) {
	for iteration := 0; iteration < 10; iteration++ {
		c := configForTest()
		g, err := New(c, CPUBackend{Rounds: 1})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, _ = g.Infer(ctx, "A", "text")
			}()
		}
		close(start)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = g.Close(ctx)
		cancel()
		wg.Wait()
		if err != nil || g.Stats().Outstanding != 0 {
			t.Fatalf("iteration %d: err=%v stats=%+v", iteration, err, g.Stats())
		}
	}
}

func TestBackendProtocolErrorsAndPanicAreContained(t *testing.T) {
	for _, panicBackend := range []bool{false, true} {
		t.Run(fmt.Sprint(panicBackend), func(t *testing.T) {
			c := configForTest()
			var calls atomic.Int32
			g := newTestGateway(t, c, backendFunc(func(ctx context.Context, in []Input) ([]Output, error) {
				if calls.Add(1) == 1 {
					if panicBackend {
						panic("private backend details")
					}
					return nil, nil
				}
				return echo(in), nil
			}))
			if _, err := g.Infer(context.Background(), "A", "text"); err == nil {
				t.Fatal("invalid backend succeeded")
			}
			if _, err := g.Infer(context.Background(), "A", "text"); err != nil {
				t.Fatalf("worker did not survive: %v", err)
			}
		})
	}
}

func TestActualHTTPServerContract(t *testing.T) {
	c := configForTest()
	g := newTestGateway(t, c, CPUBackend{Rounds: 1})
	server := httptest.NewServer(g.Handler())
	defer server.Close()
	for _, tc := range []struct {
		body, tenant string
		status       int
	}{{`{"input":"hello"}`, "A", 200}, {`{"input":"hello","unsafe":true}`, "A", 400}, {`{"input":"one"} {"input":"two"}`, "A", 400}, {`{"input":"hello"}`, "", 400}, {`{"input":"hello"}`, "bad tenant", 400}, {`{"input":"` + strings.Repeat("x", 5000) + `"}`, "A", 400}} {
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/infer", strings.NewReader(tc.body))
		req.Header.Set("X-Tenant-ID", tc.tenant)
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status {
			t.Fatalf("status=%d body=%s", res.StatusCode, body)
		}
	}
	res, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var stats Stats
	if err = json.NewDecoder(res.Body).Decode(&stats); err != nil || stats.Accepted != 1 {
		t.Fatalf("metrics err=%v accepted=%d", err, stats.Accepted)
	}
}

func TestHTTPDisconnectCancelsQueuedWork(t *testing.T) {
	c := configForTest()
	c.BackendConcurrency = 1
	started, release := make(chan struct{}), make(chan struct{})
	g := newTestGateway(t, c, gatedBackend(started, release))
	server := httptest.NewServer(g.Handler())
	defer server.Close()
	call := func(ctx context.Context) <-chan error {
		out := make(chan error, 1)
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/infer", bytes.NewBufferString(`{"input":"hello"}`))
			req.Header.Set("X-Tenant-ID", "A")
			res, err := server.Client().Do(req)
			if res != nil {
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
			}
			out <- err
		}()
		return out
	}
	first := call(context.Background())
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	second := call(ctx)
	waitFor(t, func() bool { return g.Stats().Queued == 1 })
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return g.Stats().Queued == 0 && g.Stats().Cancelled == 1 })
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if g.Stats().BackendCalls != 1 {
		t.Fatal("cancelled queued HTTP request reached backend")
	}
}

func TestHTTPAdmissionDeadlineAndBreakerStatusCodes(t *testing.T) {
	t.Run("admission_and_deadline", func(t *testing.T) {
		c := configForTest()
		c.PerTenantOutstanding = 1
		c.BackendConcurrency = 1
		started, release := make(chan struct{}), make(chan struct{})
		g := newTestGateway(t, c, gatedBackend(started, release))
		warm := asyncInfer(g, context.Background(), "A")
		<-started
		server := httptest.NewServer(g.Handler())
		defer server.Close()
		defer close(release)
		for _, tc := range []struct {
			tenant, timeout string
			status          int
		}{{"A", "", 429}, {"B", "5", 504}} {
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/infer", strings.NewReader(`{"input":"text"}`))
			req.Header.Set("X-Tenant-ID", tc.tenant)
			req.Header.Set("X-Timeout-Ms", tc.timeout)
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != tc.status {
				t.Fatalf("got %d want %d", res.StatusCode, tc.status)
			}
		}
		_ = warm
	})
	t.Run("breaker", func(t *testing.T) {
		c := configForTest()
		c.BreakerFailures = 1
		c.BreakerCooldown = time.Second
		g := newTestGateway(t, c, backendFunc(func(context.Context, []Input) ([]Output, error) { return nil, &BackendError{Code: "private_error"} }))
		server := httptest.NewServer(g.Handler())
		defer server.Close()
		for _, want := range []int{502, 503} {
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/infer", strings.NewReader(`{"input":"text"}`))
			req.Header.Set("X-Tenant-ID", "A")
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != want || strings.Contains(string(body), "private_error") {
				t.Fatalf("status %d body %s", res.StatusCode, body)
			}
		}
	})
}

func TestValidationAndExpiredContextDoNotAdmit(t *testing.T) {
	c := configForTest()
	g := newTestGateway(t, c, CPUBackend{Rounds: 1})
	for _, input := range []struct{ tenant, text string }{{"", "x"}, {"A", " "}, {"A", strings.Repeat("x", c.MaxInputBytes+1)}} {
		if _, err := g.Infer(context.Background(), input.tenant, input.text); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Infer(ctx, "A", "text"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := g.Infer(nil, "A", "text"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if g.Stats().Accepted != 0 || g.Stats().Outstanding != 0 {
		t.Fatal("invalid request consumed admission")
	}
	for _, modify := range []func(*Config){func(c *Config) { c.MaxBatch = 129 }, func(c *Config) { c.PerTenantOutstanding = 0 }, func(c *Config) { c.MaxAttempts = 4 }, func(c *Config) { c.BackendTimeout = 0 }} {
		bad := c
		modify(&bad)
		if _, err := New(bad, CPUBackend{Rounds: 1}); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
