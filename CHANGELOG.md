# Changelog

All notable changes to this project are documented here.

## Unreleased

- **Fixed: keys file refused under Kubernetes Secret mounts.** kubelet
  materializes Secret volume files as symlinks (`keys.json →
  ..data/keys.json → ..<timestamp>/keys.json`), so the O_NOFOLLOW open of
  `MESH0_KEYS_FILE` failed at startup with `keys file … is a symlink
  (refusing for safety)` and the sidecar crash-looped. Symlinks are now
  followed when they resolve to a regular file inside the keys file's own
  directory; links resolving anywhere else are still refused.
- **Multi-tenant routing.** A single sidecar can now ship events for many
  mesh0 projects from one host. Callers add an optional top-level
  `_project` field to each datagram; the agent strips it and POSTs to
  the matching project's API key. New env var `MESH0_KEYS_FILE` points
  at a JSON object mapping project → API key, reloaded on `SIGHUP`
  (atomic-rename when writing). `MESH0_API_KEY` is unchanged for
  single-tenant deployments and, when both are set, becomes the
  fallback for datagrams without `_project`. See the README's
  "Multi-tenant routing" section.
- **BREAKING:** `MESH0_QUEUE_SIZE` reverts to **process-wide** with a
  single shared queue feeding a demuxer that fans datagrams out to
  per-project pipelines. Each pipeline now owns only a small 16-slot
  handoff buffer. The shared queue's worst-case in-flight memory is
  `QUEUE_SIZE × MESH0_MAX_EVENT_BYTES`, independent of project count;
  the handoff buffers add `16 × registered_projects ×
  MESH0_MAX_EVENT_BYTES` on top, which is dominated by the shared
  queue at modest project counts but grows linearly with
  `MESH0_MAX_PROJECTS` (at the 4096 cap, lower `MESH0_MAX_EVENT_BYTES`
  to keep the handoff contribution bounded). The same `QUEUE_SIZE`
  setting now matches one fixed shared-queue budget rather than
  scaling with project count. The trade-off: a single project that
  monopolises the shared queue can back-pressure the others — fine for
  related projects under one customer, not fine for hostile-tenant
  isolation (run separate agents for that).
- Per-project `events_dropped.queue_full` (visible in `by_project`)
  now means a specific project's 16-slot handoff buffer overflowed,
  not the shared queue. Process-wide `events_dropped.queue_full`
  continues to count shared-queue exhaustion.
- New per-project counters surface in `GET /stats` under
  `by_project.<project>`: `events_received`, `events_sent`,
  `batches_sent`, `events_dropped.*`, `last_flush_age_ms`. Process-wide
  totals on the existing top-level keys are unchanged.
- New drop counters `events_dropped.unrouted_missing_project` and
  `events_dropped.unrouted_unknown_project` distinguish "datagram had no
  `_project` and no default key registered" from "datagram named a
  project we don't have a key for." Alert on either spiking.
- Project names beginning with `_` are reserved (the agent registers the
  `MESH0_API_KEY` path under sentinel project `_default`). Keys-file
  reload rejects such names at parse time and keeps the previous table.
- `SIGHUP` reloads `MESH0_KEYS_FILE`. The agent diffs the new file
  against the current routing table: added projects get fresh pipelines,
  removed projects are drained with `MESH0_SHUTDOWN_GRACE_MS`, and
  projects whose key rotated are replaced. A parse error keeps the
  previous table — a bad reload does not take the agent down.
- New `/stats` fields `keys_reload_failures` (counter, cumulative) and
  `last_keys_reload_unix` (unix-seconds of the most recent successful
  reload). Alert on `keys_reload_failures > 0` paired with a stale
  `last_keys_reload_unix` to catch operators running on a frozen routing
  table after a bad keys-file push.
- A datagram whose `_project` field is structurally malformed JSON is
  now accounted as `events_dropped.parse_error` at the routing layer
  instead of being forwarded verbatim (which would 4xx the whole batch
  at the gateway).
- A datagram whose `_project` value is well-formed JSON but **not a
  string** (number, null, object, array) is now accounted as
  `events_dropped.unrouted_unknown_project` instead of silently
  defaulting to the `MESH0_API_KEY` fallback. Catches misbehaving
  client SDKs that would otherwise cross-attribute to whichever tenant
  owns the default key.
- New env var `MESH0_MAX_PROJECTS` (default `64`, range `[1, 4096]`)
  caps the number of registered pipelines. Each pipeline costs
  `QueueSize × MaxEventBytes` of worst-case in-flight memory plus two
  goroutines and an `http.Client`; a misconfigured keys file with one
  entry per request-id would otherwise OOM the 32Mi sidecar. `install`
  fails fast over the cap; `SIGHUP` reload over the cap is rejected and
  bumps `keys_reload_failures` (previous table is kept).
- New env var `MESH0_REQUIRE_PROJECT` (default `false`). When set, the
  `MESH0_API_KEY` fallback is disabled for datagrams arriving without a
  `_project` field — they drop as `unrouted_missing_project` instead.
  Recommended for multi-tenant deployments to surface mis-tagged
  callers rather than silently cross-attributing them.
- New drop counter `events_dropped.routing_closed` (per-project and
  process-wide). Distinguishes "a SIGHUP reload retired this pipeline
  between lookup and send" from genuine queue saturation, so a
  `queue_full` alert isn't triggered by reload churn.
- `MESH0_KEYS_FILE` is now opened with `O_NOFOLLOW` and rejected if it
  is a symlink, a non-regular file, world-writable, or larger than
  1 MiB. Operators should keep it mode `0600` (or `0640`); a writable
  keys file is effectively a root credential for every registered
  tenant.

- Per-datagram size cap is now configurable via `MESH0_MAX_EVENT_BYTES`,
  with a new default of **1 MB** (was a hard-coded 32 KB). Range
  `[1 KB, 16 MB]`. Worst-case in-flight memory is `MESH0_QUEUE_SIZE ×
  MESH0_MAX_EVENT_BYTES`; if you raise the cap, consider lowering the
  queue size to keep the memory budget bounded.
- The listener's read-buffer pool is now sized to `max_event_bytes + 1`,
  so an oversized datagram is read at the boundary and rejected by the
  validator with `drops.oversize` accounting rather than silently
  truncated by the kernel.
- **Operator note:** to send datagrams larger than your system default,
  Linux requires `net.core.wmem_max` / `net.core.rmem_max` raised on
  both ends, and the SDK/client must call `setsockopt(SO_SNDBUF, ...)`.
  macOS additionally caps unixgram datagrams at `net.local.dgram.maxdgram`
  (default 2 KB).

## 0.3.0

- **BREAKING:** UDP listening removed. The agent now exclusively listens on
  a Unix-domain SOCK_DGRAM socket. Existing deployments must mount a
  shared socket volume between the app and agent containers and switch
  their SDK/client from UDP to `udg://` (PHP) / `socket(AF_UNIX,
  SOCK_DGRAM)` (other languages).
- **BREAKING:** `MESH0_LISTEN_ADDR` is replaced by `MESH0_LISTEN_PATH`.
  Default is `/run/mesh0/agent.sock`. The value is a filesystem path —
  no `unix://` / `udp://` URL prefixes accepted.
- **BREAKING:** `/stats` field renames: `udp_read_errors` → `read_errors`,
  `udp_buffer_degraded` → `buffer_degraded`. Update any dashboards or
  alerting that scrape these fields.
- The agent removes any stale socket file on startup (idempotent
  restart), `chmod 0666` the bound socket so app processes running as a
  different uid can write to it without uid alignment, and unlinks the
  socket on graceful shutdown. A non-socket file at the bind path is
  rejected rather than removed.
- **BREAKING:** A `chmod 0666` failure on the bound socket now hard-fails
  the listener instead of being logged-and-ignored. A silent perms
  failure would have left the agent reporting zero traffic forever; an
  early fatal exit is the right outcome.
- `MESH0_LISTEN_PATH` is rejected at startup if the path exceeds 103
  bytes (`sun_path` is 104 on macOS / 108 on Linux). This produces a
  clear config error instead of an opaque kernel `EINVAL` at bind time.
- New `/stats` field `listener_fatal` (bool) — set to `true` when the
  listener goroutine exits with a non-nil error before SIGTERM. Alert
  on this; the process will drain and exit, but `events_received` may
  otherwise look healthy.
- Flusher cancellations during shutdown are now accounted as
  `events_dropped.shutdown` rather than `events_dropped.flush_failed`,
  so operators can distinguish a wedged gateway from a hard shutdown.
  `flush_failed` is now reserved for genuine gateway failures
  (retry-exhausted 5xx/429, or non-retryable 4xx).
- Internal: removed a double-increment of `events_received` (the listener
  and the batcher both counted every datagram). The counter now reflects
  every datagram the agent reads off the socket, exactly once, so
  `events_received ≈ events_sent + sum(events_dropped.*)` holds.

## 0.2.0

- **BREAKING:** drops statsd protocol; agent now exclusively accepts JSON events on UDP.
  The first-byte sniff and the statsd line parser have been removed. Every UDP datagram
  is parsed as a JSON object; non-objects increment `events_dropped.parse_error` and are
  discarded.
- Removed `Metric`, `Snapshot`, the per-series counter/gauge/timing aggregator, and
  quantile math. The agent is now a pure pass-through batcher of opaque JSON events.
- Pipeline simplified to listener → eventsBatcher → eventsFlusher (two channels). The
  listener never blocks on a full batch queue — it drops the newest datagram and
  increments `events_dropped.queue_full`.
- Stats endpoint (`GET /stats`) drops the `metrics_received` field and all statsd-only
  counters. Only event counters remain.
- Removed `MESH0_FLUSH_INTERVAL_MS`. The event path is configured via
  `MESH0_BATCH_WINDOW_MS` (default 200) and `MESH0_MAX_BATCH` (default 500).
- Default flush endpoint is `/v1/events`; configurable via `MESH0_EVENTS_PATH`.
- Stats snapshot adds `events_dropped.shutdown` (events abandoned because the
  shutdown grace period expired with the flusher still wedged) and
  `udp_buffer_degraded` (true when the kernel rejected the requested
  `SO_RCVBUF`, signalling that elevated NIC/socket-level loss is plausible).
- Listener now applies a brief exponential backoff (10ms → 1s) on repeated
  non-cancellation read errors so a wedged socket cannot spin the goroutine.
- Shutdown drain re-ordered so the grace timer arms before the batcher's
  final flush. Previously a wedged HTTPS POST plus a full `batchCh` could
  block the batcher's send forever and prevent shutdown from progressing.

## 0.1.0

- Initial release: UDP statsd / DogStatsD aggregator with HTTPS flush to mesh0.ai.
