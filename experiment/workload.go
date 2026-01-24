// Package experiment runs real loopback HTTP workloads and controlled scheduler
// experiments. The CPU backend is a deterministic fixture, never a model benchmark.
package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"sync"
	"time"

	"servegate/gateway"
)

type Distribution struct {
	Count     int     `json:"count"`
	P50Millis float64 `json:"p50_ms"`
	P95Millis float64 `json:"p95_ms"`
	P99Millis float64 `json:"p99_ms"`
}

func distribution(values []time.Duration) Distribution {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	d := Distribution{Count: len(values)}
	if len(values) == 0 {
		return d
	}
	percentile := func(p float64) float64 {
		i := int(float64(len(values)-1) * p)
		return float64(values[i]) / float64(time.Millisecond)
	}
	d.P50Millis = percentile(.5)
	d.P95Millis = percentile(.95)
	d.P99Millis = percentile(.99)
	return d
}

type Workload struct {
	Requests                  int            `json:"requests"`
	Clients                   int            `json:"concurrent_clients"`
	Rounds                    int            `json:"sha256_rounds_per_input"`
	MaxBatch                  int            `json:"configured_max_batch"`
	Successes                 int            `json:"successes"`
	Errors                    map[string]int `json:"errors"`
	ElapsedMillis             float64        `json:"elapsed_ms"`
	ObservedRequestsPerSecond float64        `json:"observed_fixture_requests_per_second"`
	ClientLatency             Distribution   `json:"client_latency_all_attempts"`
	Stats                     gateway.Stats  `json:"gateway"`
	Passed                    bool           `json:"passed"`
}

func HTTPWorkload(requests, clients, rounds, maxBatch int) (Workload, error) {
	if requests < 1 || requests > 4096 || clients < 1 || clients > 64 || rounds < 1 || rounds > 10000 || maxBatch < 1 || maxBatch > 64 {
		return Workload{}, errors.New("workload bounds: requests1..4096 clients1..64 rounds1..10000 batch1..64")
	}
	c := gateway.DefaultConfig()
	c.MaxBatch = maxBatch
	c.MaxRequestTimeout = 5 * time.Second
	c.BackendTimeout = 5 * time.Second
	c.PerTenantOutstanding = 64
	g, err := gateway.New(c, gateway.CPUBackend{Rounds: rounds})
	if err != nil {
		return Workload{}, err
	}
	server := httptest.NewServer(g.Handler())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Close(ctx)
		server.Close()
	}()
	transport := &http.Transport{MaxIdleConns: clients, MaxIdleConnsPerHost: clients, MaxConnsPerHost: clients}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 6 * time.Second}
	type sample struct {
		elapsed time.Duration
		code    string
	}
	jobs := make(chan int, requests)
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	out := make(chan sample, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for id := range jobs {
				body, _ := json.Marshal(map[string]string{"input": fmt.Sprintf("synthetic request %04d", id)})
				req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/infer", bytes.NewReader(body))
				req.Header.Set("X-Tenant-ID", fmt.Sprintf("tenant%d", id%4))
				started := time.Now()
				res, err := client.Do(req)
				code := "ok"
				if err != nil {
					code = "transport_error"
				} else {
					var result gateway.Result
					decodeErr := json.NewDecoder(res.Body).Decode(&result)
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					if res.StatusCode != http.StatusOK {
						code = fmt.Sprintf("http_%d", res.StatusCode)
					} else if decodeErr != nil || len(result.Output) != 64 {
						code = "invalid_fixture_response"
					}
				}
				out <- sample{time.Since(started), code}
			}
		}()
	}
	started := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(started)
	close(out)
	report := Workload{Requests: requests, Clients: clients, Rounds: rounds, MaxBatch: maxBatch, Errors: map[string]int{}, ElapsedMillis: float64(elapsed) / float64(time.Millisecond), ObservedRequestsPerSecond: float64(requests) / elapsed.Seconds()}
	latencies := make([]time.Duration, 0, requests)
	for sample := range out {
		latencies = append(latencies, sample.elapsed)
		if sample.code == "ok" {
			report.Successes++
		} else {
			report.Errors[sample.code]++
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = g.Close(ctx)
	cancel()
	if err != nil {
		return report, err
	}
	report.Stats = g.Stats()
	report.ClientLatency = distribution(latencies)
	report.Passed = report.Successes == requests && report.Stats.Finished == int64(requests) && report.Stats.PeakActiveBatches <= c.BackendConcurrency && report.Stats.Outstanding == 0
	return report, nil
}

type gateBackend struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateBackend) Infer(ctx context.Context, in []gateway.Input) ([]gateway.Output, error) {
	b.once.Do(func() { close(b.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
		out := make([]gateway.Output, len(in))
		for i := range in {
			out[i].Text = "controlled_fixture"
		}
		return out, nil
	}
}
func waitFor(condition func() bool) error {
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			return errors.New("controlled workload did not reach its expected state")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

type Cancellation struct {
	CancelledClients int           `json:"cancelled_clients"`
	CleanupMillis    float64       `json:"queue_cleanup_ms"`
	Stats            gateway.Stats `json:"gateway"`
	Passed           bool          `json:"passed"`
}

func CancellationWorkload() (Cancellation, error) {
	c := gateway.DefaultConfig()
	c.MaxBatch = 1
	c.MaxBatchWait = 0
	c.BackendConcurrency = 1
	c.MaxRequestTimeout = 5 * time.Second
	c.BackendTimeout = 5 * time.Second
	backend := &gateBackend{started: make(chan struct{}), release: make(chan struct{})}
	g, err := gateway.New(c, backend)
	if err != nil {
		return Cancellation{}, err
	}
	server := httptest.NewServer(g.Handler())
	var release sync.Once
	defer func() {
		release.Do(func() { close(backend.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Close(ctx)
		server.Close()
	}()
	call := func(ctx context.Context) <-chan error {
		out := make(chan error, 1)
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/infer", bytes.NewBufferString(`{"input":"controlled"}`))
			req.Header.Set("X-Tenant-ID", "tenant")
			res, err := server.Client().Do(req)
			if res != nil {
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
			}
			out <- err
		}()
		return out
	}
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer warmCancel()
	warm := call(warmCtx)
	select {
	case <-backend.started:
	case <-time.After(2 * time.Second):
		return Cancellation{}, errors.New("backend did not start")
	}
	cancels := []context.CancelFunc{}
	results := []<-chan error{}
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		defer cancel()
		results = append(results, call(ctx))
	}
	if err = waitFor(func() bool { return g.Stats().Queued == 4 }); err != nil {
		return Cancellation{}, err
	}
	started := time.Now()
	for _, cancel := range cancels {
		cancel()
	}
	cancelled := 0
	for _, result := range results {
		if errors.Is(<-result, context.Canceled) {
			cancelled++
		}
	}
	if err = waitFor(func() bool { return g.Stats().Queued == 0 && g.Stats().Cancelled == 4 }); err != nil {
		return Cancellation{}, err
	}
	cleanup := time.Since(started)
	release.Do(func() { close(backend.release) })
	if err = <-warm; err != nil {
		return Cancellation{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = g.Close(ctx)
	cancel()
	if err != nil {
		return Cancellation{}, err
	}
	s := g.Stats()
	return Cancellation{CancelledClients: cancelled, CleanupMillis: float64(cleanup) / float64(time.Millisecond), Stats: s, Passed: cancelled == 4 && s.BackendCalls == 1 && s.Outstanding == 0}, nil
}

type Fairness struct {
	ServiceOrder []string      `json:"backlogged_service_order"`
	Stats        gateway.Stats `json:"gateway"`
	Passed       bool          `json:"passed"`
}

func FairnessWorkload() (Fairness, error) {
	c := gateway.DefaultConfig()
	c.MaxBatch = 1
	c.MaxBatchWait = 0
	c.BackendConcurrency = 1
	backend := &gateBackend{started: make(chan struct{}), release: make(chan struct{})}
	g, err := gateway.New(c, backend)
	if err != nil {
		return Fairness{}, err
	}
	var release sync.Once
	defer func() {
		release.Do(func() { close(backend.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Close(ctx)
	}()
	results := make(chan error, 16)
	go func() { _, err := g.Infer(context.Background(), "warm", "fixture"); results <- err }()
	<-backend.started
	for index, tenant := range []string{"A", "B", "C"} {
		for i := 0; i < 5; i++ {
			go func(tenant string) { _, err := g.Infer(context.Background(), tenant, "fixture"); results <- err }(tenant)
		}
		want := (index + 1) * 5
		if err = waitFor(func() bool { return g.Stats().Queued == want }); err != nil {
			return Fairness{}, err
		}
	}
	release.Do(func() { close(backend.release) })
	for i := 0; i < 16; i++ {
		if err = <-results; err != nil {
			return Fairness{}, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = g.Close(ctx)
	cancel()
	if err != nil {
		return Fairness{}, err
	}
	s := g.Stats()
	report := Fairness{Stats: s, Passed: true}
	for i, entry := range s.Trace {
		if i == 0 {
			continue
		}
		report.ServiceOrder = append(report.ServiceOrder, entry.Tenants[0])
		if entry.Tenants[0] != []string{"A", "B", "C"}[(i-1)%3] {
			report.Passed = false
		}
	}
	if len(report.ServiceOrder) != 15 {
		report.Passed = false
	}
	return report, nil
}

func Evaluate(requests, clients, rounds int) (map[string]any, error) {
	baseline, err := HTTPWorkload(requests, clients, rounds, 1)
	if err != nil {
		return nil, err
	}
	batched, err := HTTPWorkload(requests, clients, rounds, 8)
	if err != nil {
		return nil, err
	}
	cancellation, err := CancellationWorkload()
	if err != nil {
		return nil, err
	}
	fairness, err := FairnessWorkload()
	if err != nil {
		return nil, err
	}
	passed := 0
	for _, ok := range []bool{baseline.Passed, batched.Passed, cancellation.Passed, fairness.Passed} {
		if ok {
			passed++
		}
	}
	return map[string]any{"backend": "deterministic_sha256_cpu_fixture", "model_calls": 0, "transport": "actual httptest loopback HTTP for load/cancellation; direct API for controlled fairness",
		"go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "passed": passed, "total": 4, "unbatched": baseline, "microbatched": batched, "cancellation": cancellation, "fairness": fairness,
		"limitations": []string{"CPU fixture processes inputs serially inside a batch; reduced backend-call count is not evidence of GPU/model speedup.", "Observed timings are one local run including net/http, scheduling and SHA-256; no production throughput or tail-latency guarantee.", "Tenant labels are caller-provided in this lab; identity authentication and external admission are not implemented.", "Cancellation/deadlines require cooperative backend code; a blocked Go function cannot be forcibly killed."}}, nil
}
