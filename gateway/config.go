// Package gateway implements a bounded inference-facing scheduler and HTTP API.
package gateway

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalid         = errors.New("invalid_request")
	ErrOverloaded      = errors.New("global_admission_full")
	ErrTenantFull      = errors.New("tenant_admission_full")
	ErrTenantLimit     = errors.New("active_tenant_limit")
	ErrDraining        = errors.New("gateway_draining")
	ErrCircuitOpen     = errors.New("backend_circuit_open")
	ErrBackendProtocol = errors.New("invalid_backend_response")
)

type Config struct {
	MaxOutstanding       int
	PerTenantOutstanding int
	MaxTenants           int
	MaxBatch             int
	BackendConcurrency   int
	MaxBatchWait         time.Duration
	DispatchSlack        time.Duration
	MaxRequestTimeout    time.Duration
	BackendTimeout       time.Duration
	RetrySafe            bool
	MaxAttempts          int
	RetryBackoff         time.Duration
	BreakerFailures      int
	BreakerCooldown      time.Duration
	MaxInputBytes        int
	MaxOutputBytes       int
}

func DefaultConfig() Config {
	return Config{MaxOutstanding: 128, PerTenantOutstanding: 32, MaxTenants: 16, MaxBatch: 8,
		BackendConcurrency: 2, MaxBatchWait: 2 * time.Millisecond, DispatchSlack: time.Millisecond,
		MaxRequestTimeout: 2 * time.Second, BackendTimeout: time.Second, MaxAttempts: 1,
		RetryBackoff: time.Millisecond, BreakerFailures: 3, BreakerCooldown: 100 * time.Millisecond,
		MaxInputBytes: 4096, MaxOutputBytes: 4096}
}

func (c Config) validate() error {
	if c.MaxOutstanding < 1 || c.MaxOutstanding > 4096 || c.PerTenantOutstanding < 1 || c.PerTenantOutstanding > c.MaxOutstanding || c.MaxTenants < 1 || c.MaxTenants > c.MaxOutstanding || c.MaxBatch < 1 || c.MaxBatch > min(c.MaxOutstanding, 128) || c.BackendConcurrency < 1 || c.BackendConcurrency > 64 {
		return fmt.Errorf("%w: invalid queue, tenant, batch or concurrency bound", ErrInvalid)
	}
	if c.MaxBatchWait < 0 || c.MaxBatchWait > time.Second || c.DispatchSlack < 0 || c.DispatchSlack > 5*time.Second || c.MaxRequestTimeout <= 0 || c.MaxRequestTimeout > 30*time.Second || c.BackendTimeout <= 0 || c.BackendTimeout > 30*time.Second || c.MaxAttempts < 1 || c.MaxAttempts > 3 || c.RetryBackoff < 0 || c.RetryBackoff > time.Second || c.BreakerFailures < 1 || c.BreakerFailures > 100 || c.BreakerCooldown <= 0 || c.BreakerCooldown > time.Minute || c.MaxInputBytes < 1 || c.MaxInputBytes > 65536 || c.MaxOutputBytes < 1 || c.MaxOutputBytes > 65536 {
		return fmt.Errorf("%w: invalid time, retry, breaker or text bound", ErrInvalid)
	}
	return nil
}
