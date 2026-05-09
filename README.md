# mesh0 metrics agent

A small UDP/UDS → HTTPS event forwarder. Customers fire one JSON event per
datagram at the agent (loopback UDP `127.0.0.1:8125` by default, or a Unix
domain socket if you'd prefer); the agent batches and POSTs to
`<MESH0_BASE_URL>/v1/events`. Authentication, TLS, batching, and retries
happen once on the agent → mesh0 hop, never in the customer's request path.

```
┌──────────────┐  UDP :8125 or          ┌──────────────────┐  HTTPS batched
│  app process │  UDS-DGRAM /run/...  ► │  mesh0 agent     │ ──────────────► api.mesh0.ai
│  (any lang)  │  one JSON event per    │  (this binary)   │  /v1/events
└──────────────┘  datagram              └──────────────────┘  every ~200ms
```

## Picking the wire: UDP vs UDS-DGRAM

Both are fire-and-forget local transports — no connection, no ack, the
caller never blocks. UDS-DGRAM is the better default whenever the app and
the agent share a host (Kubernetes sidecar, the same VM):

|                           | UDP             | UDS-DGRAM        |
|---------------------------|-----------------|------------------|
| Caller cost (per event)   | ~µs             | ~µs              |
| Max single payload        | ~64 KB (IP frag) | configurable, often 200 KB+ |
| Goes through IP stack     | yes             | no               |
| Auth / scoping            | listen on lo    | filesystem perms |
| Cross-host transport      | yes             | no — same host only |

Stick with UDP if your app and the agent might be on different hosts (rare
for sidecar deployments). Otherwise prefer UDS — bigger payloads, lossless
on a healthy host, no port to coordinate.

## Why this exists

The mesh0 backend ingest API (`/v1/events`) is HTTPS — durable,
authenticated. That model breaks down when you want to record millions of
small events from PHP, where every page view is a fresh process and there's
no in-process buffer to amortize HTTP calls against.

This agent is the standard fix: push HTTPS state out of the short-lived
process and into a long-running sidecar. The wire to your app is local UDP
(at-most-once, fire-and-forget); the wire to mesh0 is HTTPS with retries.

## Wire format (UDP / UDS-DGRAM → agent)

One JSON object per UDP datagram, ≤ 32 KB. Anything that isn't a JSON
object is dropped (`drops.parse_error`++); anything over 32 KB is dropped
(`drops.oversize`++). The agent does not validate field-by-field — it
forwards events as-is and fills `timestamp` with `now()` only if absent.

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
Authorization: Bearer <MESH0_API_KEY>
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

```bash
docker run --rm \
  -e MESH0_API_KEY=m0_... \
  -p 8125:8125/udp \
  ghcr.io/mesh0-ai/metrics-agent:latest
```

From the app:

```bash
echo -n '{"operation":"checkout.charge","duration_ms":42}' \
  | nc -u -w0 127.0.0.1 8125
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
      containers:
        - name: app
          image: my/php-app:latest
          env:
            - { name: MESH0_AGENT_HOST, value: "127.0.0.1" }
            - { name: MESH0_AGENT_PORT, value: "8125" }
        - name: mesh0-metrics-agent
          image: ghcr.io/mesh0-ai/metrics-agent:latest
          env:
            - { name: MESH0_API_KEY, valueFrom: { secretKeyRef: { name: mesh0, key: api-key } } }
          ports:
            - { containerPort: 8125, protocol: UDP }
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

UDP:

```php
$sock = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
$evt  = json_encode([
    "operation"   => "checkout.charge",
    "duration_ms" => 42,
    "status"      => "success",
    "user_id"     => "u_42",
]);
socket_sendto($sock, $evt, strlen($evt), 0, '127.0.0.1', 8125);
```

UDS-DGRAM (PHP exposes Unix datagram sockets as `udg://`):

```php
$sock = stream_socket_client('udg:///run/mesh0/agent.sock');
fwrite($sock, json_encode([
    "operation"   => "checkout.charge",
    "duration_ms" => 42,
]));
```

The official mesh0 PHP SDK handles both transports — set
`MESH0_AGENT_SOCKET=/run/mesh0/agent.sock` to switch the SDK's sinks
from UDP to UDS-DGRAM with no code change.

## Configuration

All knobs are environment variables.

| Variable                  | Default                | Notes                                      |
|---------------------------|------------------------|--------------------------------------------|
| `MESH0_API_KEY`           | (required)             | Per-project API key (`m0_…`).              |
| `MESH0_BASE_URL`          | `https://api.mesh0.ai` | Override for self-hosted / staging.        |
| `MESH0_EVENTS_PATH`       | `/v1/events`           | Path appended to base URL.                 |
| `MESH0_LISTEN_ADDR`       | `:8125`                | Bind target. `host:port` (UDP) or `unix:///path` / `unixgram:///path` (UDS-DGRAM). |
| `MESH0_HEALTH_ADDR`       | `:8126`                | HTTP health/stats bind. Empty disables.    |
| `MESH0_BATCH_WINDOW_MS`   | `200`                  | Max age of an event before its batch flushes. |
| `MESH0_MAX_BATCH`         | `500`                  | Max events per batch (≤ 5000 server cap).  |
| `MESH0_QUEUE_SIZE`        | `10000`                | UDP-side bounded queue depth.              |
| `MESH0_MAX_RETRIES`       | `4`                    | Retry budget per batch on 429/5xx/network. |
| `MESH0_SHUTDOWN_GRACE_MS` | `15000`                | Max wait for in-flight flushes on exit.    |
| `MESH0_LOG_LEVEL`         | `info`                 | `debug` \| `info` \| `warn` \| `error`     |

## Health & observability

The agent exposes a small HTTP server on `MESH0_HEALTH_ADDR` (default `:8126`):

- `GET /healthz` — returns `200 ok` once the agent is up.
- `GET /stats` — JSON snapshot:

  ```json
  {
    "events_received":     123456,
    "events_dropped":      {"parse_error": 12, "queue_full": 3, "oversize": 0, "flush_failed": 0, "shutdown": 0},
    "batches_sent":        247,
    "events_sent":         123087,
    "last_flush_age_ms":   180,
    "udp_read_errors":     0,
    "udp_buffer_degraded": false,
    "uptime_s":            3600
  }
  ```

  - `events_dropped.shutdown` counts events abandoned when the shutdown
    grace timer fires before the flusher finishes draining.
  - `udp_buffer_degraded` is `true` if the kernel rejected the agent's
    `SO_RCVBUF=8MB` request — investigate elevated NIC/socket-level loss
    via `/proc/net/udp` if so.

## Loss model

The local wire is at-most-once on UDP and best-effort on UDS-DGRAM
(lossless on a healthy host but the kernel will still drop datagrams if
the agent's recv buffer fills). Four loss points, all observable in
`/stats`:

1. **Kernel recv buffer** — mitigated by `SO_RCVBUF=8MB` set on
   startup (Linux applies it to both UDP and UDS-DGRAM). Drops at this
   layer are invisible to the agent — watch `/proc/net/udp` (UDP) or
   `/proc/net/unix` queue depth (UDS) if you suspect them, and check
   `udp_buffer_degraded` in `/stats` to confirm the kernel accepted the
   buffer request.
2. **Agent queue full** (`drops.queue_full`) — internal `rawCh` is
   bounded by `MESH0_QUEUE_SIZE`. The listener never blocks; if the
   batcher is behind, the newest datagram is dropped.
3. **Flush failures** (`drops.flush_failed`) — gateway 4xx (non-429),
   or 429/5xx after `MESH0_MAX_RETRIES`.
4. **Shutdown grace exhausted** (`drops.shutdown`) — events still in
   flight (or queued behind a wedged POST) when
   `MESH0_SHUTDOWN_GRACE_MS` elapses are abandoned and counted.

## Build from source

```bash
go build -ldflags="-X main.Version=$(git describe --tags --always)" .
go test ./...
```

## License

[Apache 2.0](./LICENSE).
