package gateway

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var tenantPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Result struct {
	RequestID   uint64 `json:"request_id"`
	BatchID     uint64 `json:"batch_id"`
	Output      string `json:"output"`
	QueueMicros int64  `json:"queue_micros"`
}
type response struct {
	result Result
	err    error
}
type request struct {
	id                uint64
	tenant, text      string
	ctx               context.Context
	arrived, deadline time.Time
	result            chan response
	once              sync.Once
	accepted          bool
	state             int // scheduler-owned: 0 incoming, 1 queued, 2 dispatched, 3 done
	queueMicros       int64
	stopCancel        func() bool
}
type batch struct {
	id    uint64
	items []*request
}
type completion struct {
	batch   batch
	outputs []Output
	err     error
}
type BatchTrace struct {
	BatchID uint64   `json:"batch_id"`
	Tenants []string `json:"tenants"`
	Size    int      `json:"size"`
	Flush   string   `json:"flush"`
}
type Stats struct {
	Accepted             int64            `json:"accepted"`
	Finished             int64            `json:"finished"`
	Cancelled            int64            `json:"cancelled"`
	DeadlineExceeded     int64            `json:"deadline_exceeded"`
	BackendCalls         int64            `json:"backend_calls"`
	Retries              int64            `json:"retries"`
	Batches              int64            `json:"batches"`
	PeakQueued           int              `json:"peak_queued"`
	PeakActiveBatches    int              `json:"peak_active_batches"`
	QueueWaitMicrosTotal int64            `json:"queue_wait_micros_total"`
	QueueWaitMicrosMax   int64            `json:"queue_wait_micros_max"`
	Rejected             map[string]int64 `json:"rejected"`
	BatchSizes           map[int]int64    `json:"batch_sizes"`
	Trace                []BatchTrace     `json:"trace"`
	TraceDropped         int64            `json:"trace_dropped"`
	Queued               int              `json:"queued"`
	ActiveBatches        int              `json:"active_batches"`
	Outstanding          int              `json:"outstanding"`
	Draining             bool             `json:"draining"`
	Breaker              string           `json:"breaker"`
}

type Gateway struct {
	config        Config
	backend       Backend
	breaker       *breaker
	ctx           context.Context
	cancel        context.CancelFunc
	incoming      chan *request
	cancellations chan struct{}
	jobs          chan batch
	completed     chan completion
	slots         chan struct{}
	drain         chan struct{}
	done          chan struct{}
	draining      atomic.Bool
	nextID        atomic.Uint64
	workers       sync.WaitGroup
	admission     sync.RWMutex
	mu            sync.Mutex
	stats         Stats
}

func New(config Config, backend Backend) (*Gateway, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if backend == nil {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{config: config, backend: backend, breaker: newBreaker(config.BreakerFailures, config.BreakerCooldown, time.Now), ctx: ctx, cancel: cancel,
		incoming: make(chan *request, config.MaxOutstanding), cancellations: make(chan struct{}, 1),
		jobs: make(chan batch, config.BackendConcurrency), completed: make(chan completion, config.BackendConcurrency),
		slots: make(chan struct{}, config.MaxOutstanding), drain: make(chan struct{}), done: make(chan struct{}),
		stats: Stats{Rejected: map[string]int64{}, BatchSizes: map[int]int64{}}}
	for i := 0; i < config.BackendConcurrency; i++ {
		g.workers.Add(1)
		go g.worker()
	}
	go g.schedule()
	return g, nil
}

func (g *Gateway) reject(err error) { g.mu.Lock(); g.stats.Rejected[err.Error()]++; g.mu.Unlock() }

func (g *Gateway) Infer(ctx context.Context, tenant, text string) (Result, error) {
	if ctx == nil {
		return Result{}, ErrInvalid
	}
	if !tenantPattern.MatchString(tenant) || len(text) > g.config.MaxInputBytes || !utf8.ValidString(text) || strings.TrimSpace(text) == "" {
		g.reject(ErrInvalid)
		return Result{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	g.admission.RLock()
	if g.draining.Load() {
		g.admission.RUnlock()
		g.reject(ErrDraining)
		return Result{}, ErrDraining
	}
	select {
	case g.slots <- struct{}{}:
	default:
		g.admission.RUnlock()
		g.reject(ErrOverloaded)
		return Result{}, ErrOverloaded
	}
	bounded, cancel := context.WithTimeout(ctx, g.config.MaxRequestTimeout)
	defer cancel()
	deadline, _ := bounded.Deadline()
	r := &request{id: g.nextID.Add(1), tenant: tenant, text: text, ctx: bounded, arrived: time.Now(), deadline: deadline, result: make(chan response, 1)}
	r.stopCancel = context.AfterFunc(bounded, func() {
		select {
		case g.cancellations <- struct{}{}:
		default:
		}
	})
	// A global slot guarantees incoming-channel capacity. Admission is linearized
	// against drain, so no request can arrive after the scheduler exits.
	g.incoming <- r
	g.admission.RUnlock()
	select {
	case result := <-r.result:
		return result.result, result.err
	case <-bounded.Done():
		return Result{}, bounded.Err()
	}
}

// Close rejects new admissions and drains accepted work. On caller timeout it
// cancels cooperative backends and returns promptly; it cannot kill a hung Go call.
func (g *Gateway) Close(ctx context.Context) error {
	g.admission.Lock()
	if g.draining.CompareAndSwap(false, true) {
		close(g.drain)
	}
	g.admission.Unlock()
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		g.cancel()
		return ctx.Err()
	}
}
func (g *Gateway) Done() <-chan struct{} { return g.done }

func (g *Gateway) Stats() Stats {
	g.mu.Lock()
	s := g.stats
	s.Rejected = make(map[string]int64, len(g.stats.Rejected))
	for key, value := range g.stats.Rejected {
		s.Rejected[key] = value
	}
	s.BatchSizes = make(map[int]int64, len(g.stats.BatchSizes))
	for key, value := range g.stats.BatchSizes {
		s.BatchSizes[key] = value
	}
	s.Trace = make([]BatchTrace, len(g.stats.Trace))
	for i, value := range g.stats.Trace {
		s.Trace[i] = value
		s.Trace[i].Tenants = append([]string(nil), value.Tenants...)
	}
	g.mu.Unlock()
	s.Outstanding = len(g.slots)
	s.Draining = g.draining.Load()
	s.Breaker = g.breaker.snapshot()
	return s
}

func (g *Gateway) schedule() {
	queues := map[string][]*request{}
	counts := map[string]int{}
	order := []string{}
	live := map[uint64]*request{}
	queued, active := 0, 0
	var nextBatch uint64
	drainSignal, rootSignal := g.drain, g.ctx.Done()
	forced := false
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	finish := func(r *request, value Result, err error) {
		r.once.Do(func() {
			r.state = 3
			r.stopCancel()
			delete(live, r.id)
			if r.accepted {
				counts[r.tenant]--
				if counts[r.tenant] == 0 {
					delete(counts, r.tenant)
				}
			}
			g.mu.Lock()
			if r.accepted {
				g.stats.Finished++
				if errors.Is(err, context.Canceled) {
					g.stats.Cancelled++
				}
				if errors.Is(err, context.DeadlineExceeded) {
					g.stats.DeadlineExceeded++
				}
			}
			g.mu.Unlock()
			r.result <- response{value, err}
			<-g.slots
		})
	}
	updateQueueStats := func() {
		g.mu.Lock()
		g.stats.Queued = queued
		g.stats.ActiveBatches = active
		g.stats.PeakQueued = max(g.stats.PeakQueued, queued)
		g.stats.PeakActiveBatches = max(g.stats.PeakActiveBatches, active)
		g.mu.Unlock()
	}
	readiness := func() (bool, time.Time, string) {
		var oldest, earliest time.Time
		for _, q := range queues {
			for _, r := range q {
				if oldest.IsZero() || r.arrived.Before(oldest) {
					oldest = r.arrived
				}
				if earliest.IsZero() || r.deadline.Before(earliest) {
					earliest = r.deadline
				}
			}
		}
		if queued == 0 {
			return false, time.Time{}, ""
		}
		if g.draining.Load() {
			return true, time.Time{}, "drain"
		}
		if queued >= g.config.MaxBatch {
			return true, time.Time{}, "full"
		}
		due := oldest.Add(g.config.MaxBatchWait)
		reason := "max_wait"
		if d := earliest.Add(-g.config.DispatchSlack); d.Before(due) {
			due = d
			reason = "deadline"
		}
		return !time.Now().Before(due), due, reason
	}
	for {
		for queued > 0 && active < g.config.BackendConcurrency && !forced {
			ready, _, reason := readiness()
			if !ready {
				break
			}
			items := make([]*request, 0, g.config.MaxBatch)
			for len(items) < g.config.MaxBatch && len(order) > 0 {
				tenant := order[0]
				order = order[1:]
				q := queues[tenant]
				r := q[0]
				q = q[1:]
				queued--
				if len(q) > 0 {
					queues[tenant] = q
					order = append(order, tenant)
				} else {
					delete(queues, tenant)
				}
				if err := r.ctx.Err(); err != nil {
					finish(r, Result{}, err)
					continue
				}
				r.state = 2
				items = append(items, r)
			}
			if len(items) == 0 {
				continue
			}
			nextBatch++
			b := batch{id: nextBatch, items: items}
			active++
			trace := BatchTrace{BatchID: b.id, Size: len(items), Flush: reason}
			g.mu.Lock()
			g.stats.Batches++
			g.stats.BatchSizes[len(items)]++
			for _, r := range items {
				wait := time.Since(r.arrived).Microseconds()
				r.queueMicros = wait
				g.stats.QueueWaitMicrosTotal += wait
				g.stats.QueueWaitMicrosMax = max(g.stats.QueueWaitMicrosMax, wait)
				trace.Tenants = append(trace.Tenants, r.tenant)
			}
			if len(g.stats.Trace) < 512 {
				g.stats.Trace = append(g.stats.Trace, trace)
			} else {
				g.stats.TraceDropped++
			}
			g.mu.Unlock()
			g.jobs <- b
		}
		updateQueueStats()
		if g.draining.Load() && len(g.slots) == 0 && active == 0 {
			close(g.jobs)
			g.workers.Wait()
			g.cancel()
			close(g.done)
			return
		}
		var wake <-chan time.Time
		if queued > 0 && active < g.config.BackendConcurrency && !forced {
			_, due, _ := readiness()
			timer.Reset(max(time.Until(due), 0))
			wake = timer.C
		}
		select {
		case r := <-g.incoming:
			if g.draining.Load() || forced {
				g.reject(ErrDraining)
				finish(r, Result{}, ErrDraining)
				break
			}
			if err := r.ctx.Err(); err != nil {
				finish(r, Result{}, err)
				break
			}
			if counts[r.tenant] >= g.config.PerTenantOutstanding {
				g.reject(ErrTenantFull)
				finish(r, Result{}, ErrTenantFull)
				break
			}
			if counts[r.tenant] == 0 && len(counts) >= g.config.MaxTenants {
				g.reject(ErrTenantLimit)
				finish(r, Result{}, ErrTenantLimit)
				break
			}
			r.accepted = true
			r.state = 1
			live[r.id] = r
			counts[r.tenant]++
			q := queues[r.tenant]
			if len(q) == 0 {
				order = append(order, r.tenant)
			}
			i := sort.Search(len(q), func(i int) bool {
				return q[i].deadline.After(r.deadline) || (q[i].deadline.Equal(r.deadline) && q[i].id > r.id)
			})
			q = append(q, nil)
			copy(q[i+1:], q[i:])
			q[i] = r
			queues[r.tenant] = q
			queued++
			g.mu.Lock()
			g.stats.Accepted++
			g.stats.Queued = queued
			g.stats.PeakQueued = max(g.stats.PeakQueued, queued)
			g.mu.Unlock()
		case <-g.cancellations:
			for tenant, q := range queues {
				kept := q[:0]
				for _, r := range q {
					if err := r.ctx.Err(); err != nil {
						queued--
						finish(r, Result{}, err)
					} else {
						kept = append(kept, r)
					}
				}
				if len(kept) == 0 {
					delete(queues, tenant)
				} else {
					queues[tenant] = kept
				}
			}
			keptTenants := order[:0]
			for _, tenant := range order {
				if len(queues[tenant]) > 0 {
					keptTenants = append(keptTenants, tenant)
				}
			}
			order = keptTenants
		case c := <-g.completed:
			active--
			for i, r := range c.batch.items {
				err := c.err
				if current := r.ctx.Err(); current != nil {
					err = current
				}
				value := Result{RequestID: r.id, BatchID: c.batch.id, QueueMicros: r.queueMicros}
				if err == nil {
					value.Output = c.outputs[i].Text
				}
				finish(r, value, err)
			}
		case <-drainSignal:
			drainSignal = nil
		case <-rootSignal:
			rootSignal = nil
			forced = true
			for _, q := range queues {
				for _, r := range q {
					finish(r, Result{}, ErrDraining)
				}
			}
			queues = map[string][]*request{}
			order = nil
			queued = 0
		case <-wake:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (g *Gateway) worker() {
	defer g.workers.Done()
	for b := range g.jobs {
		outputs, err := g.execute(b)
		g.completed <- completion{batch: b, outputs: outputs, err: err}
	}
}

func (g *Gateway) execute(b batch) ([]Output, error) {
	alive := false
	for _, r := range b.items {
		if r.ctx.Err() == nil {
			alive = true
			break
		}
	}
	if !alive {
		return nil, context.Canceled
	}
	ctx, cancel := context.WithTimeout(g.ctx, g.config.BackendTimeout)
	defer cancel()
	var remaining atomic.Int64
	remaining.Store(int64(len(b.items)))
	stops := make([]func() bool, 0, len(b.items))
	for _, r := range b.items {
		stops = append(stops, context.AfterFunc(r.ctx, func() {
			if remaining.Add(-1) == 0 {
				cancel()
			}
		}))
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	permit, allowed := g.breaker.allow()
	if !allowed {
		return nil, ErrCircuitOpen
	}
	var finalErr error
	defer func() { g.breaker.finish(permit, finalErr == nil, !errors.Is(finalErr, context.Canceled)) }()
	inputs := make([]Input, len(b.items))
	for i, r := range b.items {
		inputs[i] = Input{ID: r.id, Tenant: r.tenant, Text: r.text}
	}
	for attempt := 1; attempt <= g.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			finalErr = err
			return nil, err
		}
		g.mu.Lock()
		g.stats.BackendCalls++
		if attempt > 1 {
			g.stats.Retries++
		}
		g.mu.Unlock()
		out, err := g.invoke(ctx, inputs)
		if budgetError := ctx.Err(); budgetError != nil {
			out, err = nil, budgetError
		}
		if err == nil {
			if len(out) != len(inputs) {
				err = ErrBackendProtocol
			} else {
				for _, item := range out {
					if len(item.Text) > g.config.MaxOutputBytes || !utf8.ValidString(item.Text) {
						err = ErrBackendProtocol
						break
					}
				}
			}
		}
		if err == nil {
			finalErr = nil
			return out, nil
		}
		finalErr = err
		var classified *BackendError
		if !g.config.RetrySafe || !errors.As(err, &classified) || !classified.Retryable || attempt == g.config.MaxAttempts {
			return nil, err
		}
		timer := time.NewTimer(g.config.RetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			finalErr = ctx.Err()
			return nil, finalErr
		case <-timer.C:
		}
	}
	return nil, finalErr
}

func (g *Gateway) invoke(ctx context.Context, inputs []Input) (outputs []Output, err error) {
	defer func() {
		if recover() != nil {
			outputs = nil
			err = &BackendError{Code: "panic", Retryable: false}
		}
	}()
	return g.backend.Infer(ctx, inputs)
}
