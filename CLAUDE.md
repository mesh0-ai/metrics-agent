# mesh0 metrics agent

## Intent

This binary exists to do one thing well: accept opaque JSON events over local
UDP and forward them, batched, to the mesh0 ingest API over authenticated
HTTPS. It is a long-running sidecar that absorbs the per-request HTTPS cost
that short-lived processes (PHP, CGI, serverless workers) cannot amortize on
their own.

The agent is deliberately a **pass-through batcher**, not a metrics
aggregator. It does not parse statsd, compute quantiles, dedupe series, or
inspect event field semantics. Every datagram is one candidate JSON object;
the agent validates structure (must be a JSON object, ≤ 32 KB), stamps a
`timestamp` if absent, and ships the original bytes verbatim inside
`{"events":[...]}`. All semantic interpretation lives server-side.

## Design priorities, in order

1. **Never block the caller.** UDP is at-most-once and fire-and-forget. The
   listener uses a non-blocking send onto a bounded queue; saturation drops
   the newest datagram and increments `drops.queue_full` rather than stalling
   the read loop.
2. **Never lose silently.** Every drop has a counter visible at `/stats`:
   `parse_error`, `oversize`, `queue_full`, `flush_failed`. Operators must be
   able to attribute loss to a specific layer without scraping logs.
3. **Be cheap.** Pooled 64 KB read buffers, atomic counters on hot paths, no
   per-event re-marshalling — batches are concatenated from the original
   `json.RawMessage` blobs. Target footprint is the 10m CPU / 32Mi memory
   sidecar request shown in the README.
4. **Fail the right batches.** Retry only on transient gateway responses
   (network errors, 408, 429, 5xx) with exponential backoff + jitter; drop
   client-error 4xx immediately so a bad batch cannot wedge the pipeline.
5. **Drain on shutdown.** SIGTERM closes the UDP socket, the batcher emits
   its final partial batch, and the flusher gets `MESH0_SHUTDOWN_GRACE_MS`
   to finish in-flight POSTs before being cancelled.

## Architecture

```
listener  ──rawCh──▶  eventsBatcher  ──batchCh──▶  eventsFlusher  ──HTTPS──▶  api.mesh0.ai
(listener.go)         (events.go)                  (flusher.go)
```

Three goroutines, two channels, one direction. Each stage is goroutine-affine
(only its own goroutine touches its mutable state); cross-stage state lives
in `selfStats` atomics ([stats.go](stats.go)) or the channels themselves.

## Non-goals

- In-process aggregation, sampling, or rollup.
- Local persistence / disk spool. UDP loss tolerance is the contract.
- Any wire format other than JSON-object-per-datagram.
- TLS termination, mTLS, or auth proxying — the agent is a client of mesh0,
  not a server-facing gateway.

## Working in this repo

- All knobs are environment variables, parsed in [main.go](main.go)
  `loadConfig`. New knobs go there with a validated range and a row in the
  README config table.
- Wire-format and contract changes are user-visible: update
  [README.md](README.md) (wire format + backend contract sections) and
  [CHANGELOG.md](CHANGELOG.md) in the same change.
- Tests are package-internal (`package main`). `go test ./...` must stay
  green; the existing tests cover validation, batching triggers, listener
  dispatch, and flusher retry semantics — extend those rather than mocking.
- Stay allocation-conscious on the listener → batcher path. Anything that
  copies or re-marshals per event needs a benchmark argument.
