# Admission, batching and shutdown

## Contract and ownership

The public Go API is `Infer(ctx, tenant, text)`. Inputs are bounded valid UTF-8 text and a 1–64 character tenant label. Contexts without a shorter deadline receive the configured request timeout. Backend adapters implement ordered batch input/output; returning a wrong output count, invalid UTF-8, or an oversized output fails the batch instead of routing mismatched results. The CPU fixture computes deterministic SHA-256 work and periodically checks context cancellation.

One scheduler goroutine owns queues, tenant counts, request state, fair-service order, batch numbering, and completion. Workers receive immutable input fields and contexts, never mutate scheduler state, and return one completion per batch. Metrics are separately mutex-protected and deep-copied. Traces retain at most 512 batch summaries and report dropped trace count; request text is not traced.

Global admission uses a bounded permit channel. Permit acquisition and posting to the bounded incoming channel hold a short admission read lock. Close takes the corresponding write lock before setting drain. Therefore no request can be posted after an empty draining scheduler has exited. Per-tenant and active-tenant checks happen under scheduler ownership. Rejected incoming requests release their permit; accepted running requests retain it until backend completion, even when the caller has already returned cancellation.

## Deadline-aware microbatching

Let request `i` have arrival `a_i` and absolute deadline `d_i`; maximum batch size is `B`, collection window `W`, deadline slack `S`, and worker bound `C`. A nonempty queue becomes dispatchable when:

`queued >= B`, or `now >= min(min_i(a_i)+W, min_i(d_i)-S)`, or draining.

This is an actual timer-driven scheduler. It does not group already completed outputs into fictitious batches. Each tenant queue is sorted by deadline and request ID; batch construction takes one head request from the next nonempty tenant and rotates that tenant to the tail. For continuously backlogged tenants, request-count service rotates across tenants rather than exhausting one tenant first. This is not weighted/token fairness or globally optimal EDF.

The full batch preserves an index-to-request binding. Adapter output position `j` goes only to that batch's request `j`; independent tests use different tenant/input values to detect cross-request routing errors. Expired requests are removed before dispatch, and cancellation wakes are coalesced into one nonblocking signal. The scheduler filters all queues in one bounded scan rather than leaving a goroutine blocked for each cancellation.

Batch backend context is bounded by the backend timeout and engine cancellation, **not the earliest member deadline**. Otherwise one urgent/cancelled caller could poison every other member. Each member has a short cancellation hook; when all members cancel, the batch context cancels. The scheduler independently validates each member's context before publishing its result. A backend that ignores its context cannot publish a success after the backend budget expires: the return boundary checks the context again.

## Backend concurrency, retries, and breaker

`C` workers and scheduler accounting bound outstanding batches. The buffered job channel is also bounded; it cannot create an additional unaccounted worker population. Retries occur serially within one worker and share the original batch budget. They require:

`operator RetrySafe AND typed transient adapter error AND attempts remaining AND live batch context`.

Default retry attempts are one. No request field grants retry safety. A real adapter must establish that repeating the entire batch is acceptable; this fixture has no side effects. Unknown/permanent errors, protocol failures, panic classification, and circuit-open responses are not automatically retried. Backend panic values are not exposed in HTTP errors.

The breaker counts final logical-batch outcomes, not each individual input or retry attempt. It has closed, open, and half-open states. Each transition increments a generation; permits record that generation. A completion with an old generation cannot change the current breaker. Once cooldown expires, exactly one probe acquires permission. An abandoned probe reopens for cooldown rather than stranding the breaker half open. Caller-driven cancellation is not a closed-state availability failure; backend deadline expiry is.

## Request and shutdown state

Requests transition incoming → queued → dispatched → done, or incoming/queued → done on rejection/cancellation. A once-only completion releases the global permit and emits to a one-element response channel, so disconnected clients cannot block accounting. Pending tenant counts include running requests; one tenant cannot evade limits by abandoning callers while its backend work continues.

Graceful Close stops admission, flushes waiting accepted requests without collection delay, waits for batch completions, closes the worker queue, and joins workers. If the Close context expires, it cancels the engine context and returns that deadline error. The scheduler remains responsible for worker completions; an uncooperative hung backend may keep `Done()` open. This limitation is explicit rather than spawning unbounded replacement workers.

| Event | State/output | Budget/accounting outcome |
| --- | --- | --- |
| Global or tenant admission full | HTTP 429 with stable code | No backend call; permit released |
| Request deadline expires in queue | Deadline error/HTTP 504 | Queue removal; no backend execution |
| One member cancels in a running batch | Its result is cancellation | Other members retain their context |
| Every member cancels | Batch context cancels | Cooperative backend exits; worker remains bounded |
| Backend transient error | Retry only under operator-safe policy | Same worker/context, capped attempts |
| Wrong batch shape or backend panic | Stable backend error/HTTP 502 | No plausible output fallback |
| Backend returns after its timeout | Deadline error | Late output discarded; synchronous call was not forcibly killed |
| Breaker open | HTTP 503 | No backend call |
| Old breaker completion | Ignored generation | Cannot undo newer circuit state |
| Close races with admission | New requests rejected | Admission lock prevents post-exit queue leaks |
| Close deadline | Cooperative cancellation; Close returns error | No claim that a hung function terminated |

HTTP client disconnects may prevent the client from receiving a response at all; status 499 is a best-effort server classification, not a guaranteed delivered wire response. The actual `serve` command sets read/header/write/idle bounds. Authentication, TLS, tenant identity, transport connection quotas, remote provider accounting, and deployment access controls are outside this local lab.

## Complexity and bounds

With `M` outstanding requests, `T` active tenants, maximum batch `B`, and `C` workers: per-tenant EDF insertion uses binary search plus slice movement, worst-case `O(M)`; readiness scanning is `O(M)`; a coalesced cancellation scan is `O(M+T)`; fair batch extraction is amortized `O(B)` apart from discarded expired entries. Completion is `O(B)`. These are transparent bounded scans, not a claimed constant-time or lock-free scheduler.

The global permit, incoming/completion/job channels, tenant cardinality, text sizes, retries, and trace history are bounded. Gateway-owned request memory is `O(M * bounded_text + C*B + 512*B)`; worker batches are subsets of admitted requests. An arbitrary in-process backend can allocate outside these limits or ignore contexts and must be supervised separately. No adaptive GPU memory model, token-cost estimator, weighted fair queue, or real model is implemented.

## Measurement boundary

The load experiment issues actual concurrent loopback HTTP requests and records every attempt, including errors, in client latency distributions. It compares identical requests/rounds/client concurrency under batch sizes one and eight. The CPU fixture executes serial hashing within each batch; fewer adapter calls need not improve elapsed time. Queue and cancellation experiments use a named latch backend for reproducible ordering. Counts demonstrate scheduling/cancellation invariants; timings do not establish production throughput or model tail latency.
