# Changelog

All notable changes to this project are documented here.

## Unreleased

- **Multi-tenant routing.** A single sidecar can now ship events for many
  mesh0 projects from one host. Callers add an optional top-level
  `_project` field to each datagram; the agent strips it and POSTs to
  the matching project's API key. New env var `MESH0_KEYS_FILE` points
  at a JSON object mapping project → API key, reloaded on `SIGHUP`
  (atomic-rename when writing). `MESH0_API_KEY` is unchanged for
  single-tenant deployments and, when both are set, becomes the
  fallback for datagrams without `_project`. See the README's
  "Multi-tenant routing" section.
- **BREAKING:** `MESH0_QUEUE_SIZE` is now **per-pipeline**, not
  process-wide; its default drops from `10000` to `2000`. Single-tenant
  deployments registering one (`_default`) pipeline can keep the lower
  default or set it explicitly. Multi-tenant deployments cap their
  worst-case in-flight memory at `QUEUE_SIZE × registered projects ×
  MESH0_MAX_EVENT_BYTES`.
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
