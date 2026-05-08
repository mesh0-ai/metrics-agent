# Changelog

All notable changes to this project are documented here.

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
