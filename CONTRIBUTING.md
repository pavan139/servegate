# Working on ServeGate

Queue ownership lives in the scheduler. Before adding a goroutine or moving a permit release, trace what happens when a caller disconnects while its batch is still running. `ops/admission-and-shutdown.md` describes those ownership rules; the tests in `gateway/` exercise their observable consequences.

Use the race detector for changes to admission, cancellation, drain or the circuit breaker:

```sh
GOFLAGS=-p=2 GOMAXPROCS=2 go test -race -count=1 ./...
go vet ./...
gofmt -l cmd gateway experiment
```

Prefer a latch-controlled backend when a test needs a particular interleaving. A sleep should not be the only reason a cancellation or fairness test passes. Keep retry safety an operator/adapter decision and test that an untrusted request cannot enable it.

Changes to load generation belong in `experiment/`. Record all attempted clients, errors and workload settings; retain unfavorable timing results. A reduction in backend calls is a batching result, not evidence of faster model computation. Put new recorded runs in `benchmarks/` and explain comparisons in `ops/saturation-runs.md`.
