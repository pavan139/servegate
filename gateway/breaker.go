package gateway

import (
	"sync"
	"time"
)

type breakerPermit struct {
	generation uint64
	probe      bool
}

// breaker ignores completions from old generations, so stale successful calls
// cannot close a circuit opened by a newer failure. One half-open probe is allowed.
type breaker struct {
	mu         sync.Mutex
	threshold  int
	cooldown   time.Duration
	now        func() time.Time
	state      string
	failures   int
	until      time.Time
	generation uint64
}

func newBreaker(threshold int, cooldown time.Duration, now func() time.Time) *breaker {
	return &breaker{threshold: threshold, cooldown: cooldown, now: now, state: "closed"}
}

func (b *breaker) allow() (breakerPermit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == "closed" {
		return breakerPermit{generation: b.generation}, true
	}
	if b.state == "open" && !b.now().Before(b.until) {
		b.state = "half_open"
		b.generation++
		return breakerPermit{generation: b.generation, probe: true}, true
	}
	return breakerPermit{}, false
}

// completed=false represents caller cancellation, not a backend availability
// failure. An abandoned half-open probe is reopened rather than left stuck.
func (b *breaker) finish(p breakerPermit, success, completed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.generation != b.generation {
		return
	}
	if !completed && !p.probe {
		return
	}
	if success && completed {
		b.failures = 0
		if p.probe {
			b.state = "closed"
			b.generation++
		}
		return
	}
	if completed {
		b.failures++
	}
	if p.probe || b.failures >= b.threshold {
		b.state = "open"
		b.until = b.now().Add(b.cooldown)
		b.generation++
	}
}

func (b *breaker) snapshot() string { b.mu.Lock(); defer b.mu.Unlock(); return b.state }
