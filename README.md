# mesh0 metrics agent

A small UDS-DGRAM → HTTPS event forwarder. Customers fire one JSON event
per datagram at the agent's Unix-domain socket
(`/run/mesh0/agent.sock` by default); the agent batches and POSTs to
`<MESH0_BASE_URL>/v1/events`. Authentication, TLS, batching, and retries
happen once on the agent → mesh0 hop, never in the customer's request path.

```
┌──────────────┐  UDS-DGRAM            ┌──────────────────┐  HTTPS batched
│  app process │  /run/mesh0/agent.sock│  mesh0 agent     │ ──────────────► api.mesh0.ai
│  (any lang)  │  one JSON event per   │  (this binary)   │  /v1/events
└──────────────┘  datagram             └──────────────────┘  every ~200ms
```

## Why UDS-DGRAM (and not UDP)

The agent and the app run on the same host — sidecar in Kubernetes, or
co-located in a VM. For that topology UDS-DGRAM beats loopback UDP on
every axis that matters:

- **Bigger payloads.** No 64 KB IP-fragmentation cap; the per-datagram
  ceiling is the kernel's `wmem_max`/`rmem_max`, typically several
  hundred KB and tunable. The agent enforces its own application cap
  (default 1 MB; configurable via `MESH0_MAX_EVENT_BYTES`).
- **Lossless on a healthy host.** No IP stack, no NIC, no checksum.
  The only loss path is the recv buffer filling, which the agent
  surfaces as `buffer_degraded` / `read_errors` in `/stats`.
- **Filesystem-scoped auth.** Access is gated by Unix permissions on
  the socket file (`0666`) and the parent directory, not by who can
  reach a loopback port.
- **No port to coordinate.** Just a path; sidecars share a volume.

The caller cost is the same fire-and-forget µs-scale send. There is no
cross-host transport — if your app and agent live on different hosts,
this binary is the wrong tool.

## Why this exists

The mesh0 backend ingest API (`/v1/events`) is HTTPS — durable,
authenticated. That model breaks down when you want to record millions of
small events from PHP, where every page view is a fresh process and there's
no in-process buffer to amortize HTTP calls against.

This agent is the standard fix: push HTTPS state out of the short-lived
process and into a long-running sidecar. The wire to your app is local
UDS-DGRAM (at-most-once on a saturated host, fire-and-forget); the wire
to mesh0 is HTTPS with retries.

## Wire format (app → agent)

One JSON object per datagram, ≤ `MESH0_MAX_EVENT_BYTES` (default 1 MB,
range `[1 KB, 16 MB]`). Anything that isn't a JSON object is dropped
(`drops.parse_error`++); anything over the configured cap is dropped
(`drops.oversize`++). The agent does not validate field-by-field — it
forwards events as-is and fills `timestamp` with `now()` only if absent.

For multi-tenant deployments, the caller adds a top-level `_project`
field identifying which project the event belongs to; the agent strips
it before forwarding so the backend payload remains exactly
`CustomEventInput`. See [Multi-tenant routing](#multi-tenant-routing).

Recommended fields, all optional:

```
timestamp        ISO-8601 string OR unix epoch number; agent fills if absent
event_id         string
trace_id         string
span_id          string
parent_span_id   string
operation        string
duration_ms      number
status           "success" | "error"
error_type       string
error_message    string
app_id           string
environment      string
user_id          string
session_id       string
tools            string[]
attributes       object (free-form bag)
messages         any
model            { provider, id }
usage            { prompt_tokens, completion_tokens, total_tokens, cost_usd }
finish_reason    string
prompt           { system, messages, prompt }
```

## Backend contract (agent → mesh0)

```
POST <MESH0_BASE_URL>/v1/events
Authorization: Bearer <the datagram's _token>
Content-Type: application/json
Body: {"events":[{...}, ...]}
```

- `200 {accepted: N}` — all good
- `4xx` (other than 429) — log + drop batch, no retry
- `429` / `5xx` — retry with exponential backoff + jitter
  (250ms × 2^attempt, capped at 5s, ±50% jitter), drop after
  `MESH0_MAX_RETRIES` and increment `drops.flush_failed`

Server limits (matched client-side): batch ≤ 5000 events, body ≤ 10 MB.

## Quick start (Docker)

Mount a shared volume for the socket and bind it from both containers:

```bash
docker volume create mesh0-agent
docker run --rm \
  -v mesh0-agent:/run/mesh0 \
  ghcr.io/mesh0-ai/metrics-agent:latest
```

From the app (in a sibling container with the same volume mounted at
`/run/mesh0`):

```bash
echo -n '{"operation":"checkout.charge","duration_ms":42}' \
  | socat - UNIX-SENDTO:/run/mesh0/agent.sock
```

## Kubernetes sidecar

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-php-app
spec:
  template:
    spec:
      volumes:
        - name: mesh0-agent
          emptyDir: { medium: Memory }
      containers:
        - name: app
          image: my/php-app:latest
          env:
            - { name: MESH0_AGENT_SOCKET, value: "/run/mesh0/agent.sock" }
          volumeMounts:
            - { name: mesh0-agent, mountPath: /run/mesh0 }
        - name: mesh0-metrics-agent
          image: ghcr.io/mesh0-ai/metrics-agent:latest
          # No credential env: every datagram carries its own `_token`.
          volumeMounts:
            - { name: mesh0-agent, mountPath: /run/mesh0 }
          ports:
            - { containerPort: 8126, protocol: TCP }
          livenessProbe:
            httpGet: { path: /healthz, port: 8126 }
            periodSeconds: 10
          readinessProbe:
            httpGet: { path: /healthz, port: 8126 }
            periodSeconds: 10
          resources:
            requests: { cpu: "10m",  memory: "32Mi" }
            limits:   { cpu: "500m", memory: "128Mi" }
```

## PHP example

PHP exposes Unix datagram sockets as `udg://`:

```php
$sock = stream_socket_client('udg:///run/mesh0/agent.sock');
fwrite($sock, json_encode([
    "operation"   => "checkout.charge",
    "duration_ms" => 42,
    "status"      => "success",
    "user_id"     => "u_42",
]));
```

The official mesh0 PHP SDK reads `MESH0_AGENT_SOCKET=/run/mesh0/agent.sock`
and writes via `udg://` automatically.

## Configuration

All knobs are environment variables.

| Variable                  | Default                | Notes                                      |
|---------------------------|------------------------|--------------------------------------------|
| `MESH0_BASE_URL`          | `https://api.mesh0.ai` | Override for self-hosted / staging.        |
| `MESH0_EVENTS_PATH`       | `/v1/events`           | Path appended to base URL.                 |
| `MESH0_LISTEN_PATH`       | `/run/mesh0/agent.sock`| UDS-DGRAM bind path (≤103 bytes — `sun_path` cap). Parent dir is `MkdirAll`'d; stale socket files are unlinked on startup; bind fails if `chmod 0666` is rejected. |
| `MESH0_HEALTH_ADDR`       | `:8126`                | HTTP health/stats bind. Empty disables.    |
| `MESH0_BATCH_WINDOW_MS`   | `200`                  | Max age of an event before its batch flushes. |
| `MESH0_MAX_BATCH`         | `500`                  | Max events per batch (≤ 5000 server cap).  |
| `MESH0_MAX_EVENT_BYTES`   | `1048576` (1 MB)       | Per-datagram size ceiling. Range `[1024, 16777216]`. Datagrams larger than this are dropped (`drops.oversize`). The listener allocates a single pooled read buffer of this size + 1 byte; raising it enlarges the agent's resident memory floor. Senders (and on Linux, `net.core.{wmem,rmem}_max`) must also permit datagrams of this size. |
| `MESH0_QUEUE_SIZE`        | `2000`                 | **Process-wide** bounded queue depth between the listener and the per-project demuxer. Worst-case in-flight memory in the shared queue is `QUEUE_SIZE × MESH0_MAX_EVENT_BYTES`, *independent of the number of registered projects*. Each project additionally owns a 16-slot handoff buffer, so total worst-case in-flight memory is `(QUEUE_SIZE + 16 × registered_projects) × MESH0_MAX_EVENT_BYTES`. The handoff term is dominated by `QUEUE_SIZE` at modest project counts but grows linearly with the number of registered projects — at 4096 projects with 1 MB events the handoff buffers alone account for ~64 GB worst-case, so deployments running that many projects should also lower `MESH0_MAX_EVENT_BYTES` and size the sidecar's memory limit accordingly. Size `QUEUE_SIZE` to match your sidecar's memory limit. |
| `MESH0_MAX_RETRIES`       | `4`                    | Retry budget per batch on 429/5xx/network. |
| `MESH0_SHUTDOWN_GRACE_MS` | `15000`                | Max wait for in-flight flushes on exit.    |
| `MESH0_MAX_PROJECTS`      | `0` (unlimited)        | Cap on registered pipelines. Range `[0, 4096]`; **`0` disables the cap** and is the default. Each pipeline costs two goroutines, an `http.Client`, and a 16-slot handoff buffer (`16 × MAX_EVENT_BYTES` worst case). Since every registration requires a valid `_token`, reaching a sane cap means an emitter is minting a distinct `_project` per request. Set a positive value only if you want that guard. |
| `MESH0_INLINE_IDLE_MS`    | `900000` (15m)         | Retire a pipeline after this long without traffic. Range `[0, 86400000]`; `0` disables expiry. This is the **only** thing that removes a pipeline, so it is what bounds the population for deployments whose projects turn over — watch `inline_projects_registered` / `inline_projects_expired` / `inline_projects_live` in `/stats`. |
| `MESH0_LOG_LEVEL`         | `info`                 | `debug` \| `info` \| `warn` \| `error`     |

## Multi-tenant routing

One sidecar per host ships events for many mesh0 projects. Each datagram
names its project in `_project` **and carries its own credential in
`_token`**:

```jsonc
{
  "_project": "workspace-42",
  "_token": "eyJhbGciOiJFZERTQSJ9…",
  "operation": "checkout.charge",
  "duration_ms": 42
}
```

`_token` is the **only** credential the agent understands. There is no keys
file, no `MESH0_API_KEY`, and no default project — the agent is configured
with no credentials at all, and learns every project from the traffic it
receives. The first datagram naming a project registers a pipeline for it on
demand; the token becomes the `Authorization: Bearer` header for the batch
those events land in.

> **Why there is no keys file.** Earlier versions rebuilt the entire routing
> table from a keys file on every poll and drained any pipeline missing from
> the rebuild. Projects registered from a `_token` are not in that file by
> construction, so an agent pointed at an empty keys file tore down every
> live pipeline twice a minute and dropped its buffered events as
> `shutdown`. On one production cluster that cost 11.8M of 28.4M events.
> Registration is now purely additive: nothing but idle expiry removes a
> pipeline.

mesh0's admission layer accepts a non-`m0_` bearer as an open-mode instance
token, so the agent forwards the token as-is and the gateway resolves the
tenant from it.

**Tenant identity comes from the `Authorization` header**, never from
anything in the event body. The agent strips every top-level `_project` and
`_token` before forwarding, so the gateway sees the same `CustomEventInput`
shape it always has.

### Routing rules

| `_project` | `_token`            | Behavior |
|------------|---------------------|----------|
| absent     | any                 | drop, `drops.unrouted_missing_project`++ |
| known      | present             | route; the token authorizes the batch |
| known      | absent              | drop, `drops.missing_token`++ |
| unknown    | present             | **register the project on demand**, then route |
| unknown    | absent              | drop, `drops.unrouted_unknown_project`++ |
| any        | present, non-string | drop, `drops.bad_token`++ |
| non-string | any                 | drop, `drops.unrouted_unknown_project`++ |

Three properties are load-bearing:

- **`_token` is always stripped from the body, even when it is unusable.** A
  credential left in the payload is written into stored telemetry and
  readable by anyone who can later read the event back. This is not
  configurable.
- **A pipeline holds no credential of its own.** Every batch is authorized by
  the token that arrived with its events, so a rotation gap can never
  authenticate one project's events under another's credential. A datagram
  routed to a live pipeline **without** a token therefore drops as
  `drops.missing_token` — reachable whenever an emitter that normally
  attaches one fails to mint it. The alternative would be to POST under an
  empty bearer, spending a full batch to earn a 401 that reads like a gateway
  problem rather than an emitter one.
- **A non-string `_token` is its own drop category.** It is not silently
  downgraded: a caller that meant to authenticate as one project must not be
  quietly authorized as another, and `drops.bad_token` distinguishes a client
  bug from an unknown project.

Idle pipelines are retired after `MESH0_INLINE_IDLE_MS` without traffic —
the only thing that removes one. Registrations count against
`MESH0_MAX_PROJECTS` when it is set.

Project names live only on the UDS wire between caller and sidecar.

### Retired environment variables

`MESH0_API_KEY`, `MESH0_KEYS_FILE`, `MESH0_KEYS_POLL_MS`,
`MESH0_INLINE_TOKENS` and `MESH0_REQUIRE_PROJECT` are no longer read. They
are **ignored with a warning** rather than being fatal, so a chart that still
sets one keeps booting through the rollout that removes it. `SIGHUP` no
longer does anything.

## Health & observability

The agent exposes a small HTTP server on `MESH0_HEALTH_ADDR` (default `:8126`):

- `GET /healthz` — returns `200 ok` once the agent is up.
- `GET /stats` — JSON snapshot:

  ```json
  {
    "events_received":   123456,
    "events_dropped":    {"parse_error": 12, "queue_full": 3, "oversize": 0, "flush_failed": 0, "shutdown": 0, "routing_closed": 0, "unrouted_missing_project": 0, "unrouted_unknown_project": 0, "bad_token": 0, "missing_token": 0},
    "batches_sent":      247,
    "events_sent":       123087,
    "last_flush_age_ms": 180,
    "read_errors":       0,
    "buffer_degraded":   false,
    "listener_fatal":    false,
    "uptime_s":          3600,
    "by_project": {
      "workspace-42": {
        "events_received": 89000, "events_sent": 88950, "batches_sent": 178,
        "events_dropped":  {"parse_error": 0, "queue_full": 0, "oversize": 0, "flush_failed": 50, "shutdown": 0, "routing_closed": 0},
        "last_flush_age_ms": 180
      }
    }
  }
  ```

  - `events_dropped.shutdown` counts events abandoned when the shutdown
    grace timer fires (or the flusher's in-flight POST is cancelled) before
    the pipeline finishes draining. `flush_failed` is reserved for true
    gateway failures (retry-exhausted 5xx/429, or non-retryable 4xx) so
    operators can distinguish "gateway broken" from "we shut down."
  - `events_dropped.routing_closed` counts datagrams that landed on a
    pipeline retired by idle expiry between routing-lookup and queue
    send. Distinct from `queue_full` so expiry doesn't masquerade as
    back-pressure.
  - `buffer_degraded` is `true` if the kernel rejected the agent's
    `SO_RCVBUF=8MB` request — investigate elevated socket-level loss
    via `/proc/net/unix` queue depth if so.
  - `listener_fatal` is `true` if the listener goroutine returned a
    non-nil error before SIGTERM. The process drains and exits, but
    `events_received` may otherwise look healthy; alert on this flag.

## Loss model

UDS-DGRAM is lossless on a healthy host, but the kernel will drop
datagrams if the agent's recv buffer fills before the listener drains
it. Four loss points, all observable in `/stats`:

1. **Kernel recv buffer** — mitigated by `SO_RCVBUF=8MB` set on
   startup. Drops at this layer are invisible to the agent — watch
   `/proc/net/unix` queue depth if you suspect them, and check
   `buffer_degraded` in `/stats` to confirm the kernel accepted the
   buffer request.
2. **Agent queue full** (`drops.queue_full`) — process-wide shared
   queue between the listener and the demuxer is bounded by
   `MESH0_QUEUE_SIZE`. The listener never blocks; if the demuxer is
   behind, the newest datagram is dropped. Per-project `queue_full`
   (only visible in `by_project`, not the top-level counter) indicates
   one project's 16-slot handoff buffer is full while the shared queue
   still has capacity — a single project's batcher/flusher is wedged.
3. **Flush failures** (`drops.flush_failed`) — gateway 4xx (non-429),
   or 429/5xx after `MESH0_MAX_RETRIES`.
4. **Unrouted** (`drops.unrouted_missing_project`,
   `drops.unrouted_unknown_project`) — datagrams with no `_project`, or
   with a `_project` that matches no registered pipeline and carried no
   `_token` to register one, are dropped here.
5. **Shutdown grace exhausted** (`drops.shutdown`) — events still in
   flight (or queued behind a wedged POST) when
   `MESH0_SHUTDOWN_GRACE_MS` elapses are abandoned and counted.

## Build from source

```bash
go build -ldflags="-X main.Version=$(git describe --tags --always)" .
go test ./...
```

## License

[Apache 2.0](./LICENSE).
