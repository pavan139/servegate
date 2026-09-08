# Queue and cancellation race checks

This is an implemented concurrent scheduler and HTTP server, not a canned trace. The default backend is labeled a deterministic CPU fixture throughout API examples and measurements.

## Critical invariants exercised

- Global, per-tenant, and active-tenant admission caps; one tenant's backlog cannot consume another tenant's quota.
- Actual full-batch collection, deadline-triggered flush, RR tenant service, and EDF ordering within a tenant.
- Unique input/output routing across a mixed-tenant batch.
- Fixed maximum backend concurrency with real concurrent requests.
- Prompt queued cancellation without a backend call; one cancelled batch member does not poison its peer; all cancelled members cancel a cooperative backend.
- Admission/Close races across bounded concurrent schedules; successful drain and forced cancellation retain no leaked global permits.
- Typed transient errors are retried only under operator-safe policy; unsafe/permanent errors are not retried.
- Breaker generations reject stale successful completions; one half-open probe; abandoned probes do not strand the circuit.
- Wrong backend result shape, panic, and late return cannot become plausible successes. In particular, a backend ignoring cancellation for 25 ms under a 5 ms budget has its late output rejected.
- Actual `httptest.NewServer` requests cover success, malformed/oversized/trailing JSON, invalid tenants, 429 admission, 502 backend failure, 503 circuit-open, 504 deadline, and disconnected-client cleanup.
- Metrics snapshots deep-copy internal maps and tenant slices.

During implementation, explicit race reasoning caught the drain/admission gap: a request could otherwise acquire a slot after an empty scheduler exited. Admission posting is now linearized against Close. Queue high-water counters are sampled at insertion, so immediately dispatched full batches do not disappear from the recorded peak. Cancellation signals are coalesced and queue filtering is linear in the bounded backlog.

## Commands

```sh
go test -race -count=1 ./...
go vet ./...
gofmt -l gateway experiment cmd
go run ./cmd/servegate evaluate --output benchmarks/evaluation.json
python hack/check_gateway.py --require-lint --wheel --go /path/to/real/go
```

The measured report and standalone verification pin the environment and executed tests. No paid/provider calls, pretrained downloads, or external Go modules are required. Remote GitHub Actions remains configured rather than claimed executed.

## Material limits

This fixture does not benchmark actual model inference, GPU batch efficiency, production traffic, or network partitions. Cancellation/deadline detection is cooperative; a hung Go backend cannot be killed by a context. Tenant labels are not authenticated, fairness is request-count based, and retry safety is an operator contract. A deployed service needs its own trusted identity, transport and resource boundaries. Those capabilities are not silently simulated.

## Frozen executed verification

Go 1.25.1 on Linux, compiler/runtime concurrency constrained with `GOFLAGS=-p=2 GOMAXPROCS=2`. All **26 top-level tests** executed under the race detector with zero skips; all **11/11** standalone checks passed, including vet, formatting, actual executable build, and demo/evaluate from both checkout and a separate temporary directory. Source/test/configuration SHA-256: `9f3f2f02606705204ab5543e4b93668fd5a3405423a74735e6e12a32396d45e6`. The independently refreshed verifier includes every Go source directory. See [verification JSON](../reports/verification.json).

Independent second-reader review of cancellation, drain, channel/permit accounting, and breaker generations found no actionable defect in those paths. This is software review evidence, not formal verification.
