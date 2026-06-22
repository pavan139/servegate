# The HTTP request path

```mermaid
sequenceDiagram
    participant H as HTTP handler
    participant A as Admission lock and permits
    participant S as Scheduler owner
    participant W as Bounded workers
    participant B as Backend and breaker
    H->>A: Validate and acquire global slot
    A->>S: Bounded incoming request
    S->>S: Tenant limit and EDF insertion
    S->>S: RR tenant choice and deadline-aware batch timer
    S->>W: Dispatch within worker budget
    W->>B: Generation permit and safe retry boundary
    B-->>W: Ordered outputs or classified failure
    W-->>S: One batch completion
    S->>S: Recheck individual contexts and release slots
    S-->>H: Bound response to original request
```

`gateway/gateway.go` owns scheduling and lifecycle; `breaker.go` serializes breaker generations; `backend.go` defines the adapter and CPU fixture; `http.go` enforces the HTTP contract. `experiment` runs real loopback load/cancellation and controlled backlog fairness. The CLI can run experiments or serve an actual local HTTP endpoint.

No model or provider is hidden behind the fixture. No client field grants retry safety, identity authentication, or additional execution permissions. See [admission-and-shutdown.md](admission-and-shutdown.md) for timing math, ownership, complexity, and failure outcomes, and [saturation-runs.md](saturation-runs.md) for actual observations.
