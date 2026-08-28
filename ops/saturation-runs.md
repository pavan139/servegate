# Recorded saturation and cancellation runs

Backend: **deterministic SHA-256 CPU fixture**, not a model. Environment: `go1.25.1`, `GOMAXPROCS=2`. Both load variants issue the same 128 inputs, 16 concurrent loopback HTTP clients, four tenant labels, and 256 hashing rounds per input. The operator work/concurrency settings are held fixed except batch size. No warm-up exclusion or error sample is silently dropped.

```sh
go run ./cmd/servegate evaluate --output benchmarks/evaluation.json
go run ./cmd/servegate evaluate --requests 256 --clients 16 --rounds 512 --output artifacts/larger.json
```

## Actual recorded run

| Measurement | Unbatched | Microbatched |
| --- | ---: | ---: |
| Successful HTTP requests | 128/128 | 128/128 |
| Maximum batch | 1 | 8 |
| Actual backend calls | 128 | 16 |
| Elapsed milliseconds | 9.4288 | 13.7142 |
| Observed fixture requests/second | 13575.41 | 9333.41 |
| Client p50 milliseconds | 0.8255 | 1.3223 |
| Client p95 milliseconds | 2.5679 | 2.8035 |
| Client p99 milliseconds | 2.6994 | 3.5357 |
| Peak queued requests | 7 | 8 |
| Peak outstanding backend batches | 2 | 2 |

The backend-call reduction is actual coalescing, not reduced model computation. The CPU fixture hashes each input serially within its batch. Collection overhead and process scheduling may make batching slower or faster in a small run; the table retains the actual elapsed/latency observations. It is not a hardware-independent speedup, serving SLA, GPU-efficiency claim, or production throughput estimate. Reported client quantiles cover **all attempted requests**, with HTTP/transport errors retained in the JSON error map. Quantiles use a sorted nearest-lower observed sample rather than interpolating an invented latency.

## Controlled cancellation and fairness

A named latch backend holds one call while four actual HTTP requests queue. Cancelling their clients removed all four queued requests, with **4/4** clients reporting cancellation, **1** total backend call, and **1.2810 ms** observed cleanup. The held call then completed. The latch controls ordering; it is not represented as a model or throughput benchmark.

With three tenants each backlogged by five requests behind one held call, observed service order was:

`A, B, C, A, B, C, A, B, C, A, B, C, A, B, C`

All fifteen backlog requests completed in RR order. This demonstrates request-count fairness under the stated controlled backlog; it does not prove cost-weighted fairness under arbitrary arrival/compute distributions.

Overall experiment checks: **4/4** (two load variants, queued cancellation, backlog fairness). Full batch traces, queue high-water marks, retry/backend counts and client latency distributions are in [the executed JSON](../benchmarks/evaluation.json). Times vary across machines and runs; compare fixed workload settings and retain failure denominators.

## Software verification

The repository verifier runs `go test -race -count=1 ./...`, `go vet`, `gofmt`, a normal executable build, and demo/evaluate from both checkout and an independent temporary directory. Tests additionally exercise unsafe retry denial, breaker generations, late backend results, per-member cancellation, drain races, EDF, real HTTP status/error paths, and cross-request response binding. See [race-checks.md](race-checks.md) and the generated verification report.
