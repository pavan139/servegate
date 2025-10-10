package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

type Input struct {
	ID     uint64
	Tenant string
	Text   string
}
type Output struct{ Text string }
type Backend interface {
	Infer(context.Context, []Input) ([]Output, error)
}

// BackendError is a trusted adapter classification. Request JSON cannot opt
// itself into retries; Config.RetrySafe must also be explicitly enabled.
type BackendError struct {
	Code      string
	Retryable bool
}

func (e *BackendError) Error() string { return "backend_" + e.Code }

// CPUBackend is a deterministic SHA-256 fixture, NOT a model or inference speed
// estimate. It honors context cancellation while doing actual CPU work.
type CPUBackend struct{ Rounds int }

func (b CPUBackend) Infer(ctx context.Context, inputs []Input) ([]Output, error) {
	rounds := b.Rounds
	if rounds < 1 || rounds > 100000 {
		return nil, &BackendError{Code: "invalid_fixture_rounds"}
	}
	out := make([]Output, len(inputs))
	for i, input := range inputs {
		hash := sha256.Sum256([]byte(input.Text))
		for step := 0; step < rounds; step++ {
			if step%32 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			hash = sha256.Sum256(hash[:])
		}
		out[i] = Output{Text: hex.EncodeToString(hash[:])}
	}
	return out, nil
}
