# mesh0 metrics agent

A small UDP/statsd sidecar that lets request-scoped runtimes (PHP, short-lived
serverless functions, anything that can't keep a background flusher alive)
ship high-frequency metrics to [mesh0](https://mesh0.ai) without blocking the
hot request path.

```
┌──────────────┐  UDP localhost:8125    ┌──────────────────┐  HTTPS batched
│  app process │ ─────────────────────► │  mesh0 agent     │ ──────────────► gateway.mesh0.ai
│  (any lang)  │   ~5µs per metric      │  (this binary)   │  flush every 10s
└──────────────┘                        └──────────────────┘
```

The app fires UDP datagrams in statsd / DogStatsD format to `127.0.0.1:8125`
and returns immediately. The agent aggregates counters, gauges, and timings
in memory and POSTs a compact JSON batch to the mesh0 gateway on a fixed
interval. Authentication, TLS, and retries happen once on the agent → gateway
hop, never in the customer's request path.

## Why this exists

The mesh0 backend ingest API (`/v1/traces`, `/v1/events`) is HTTPS — durable,
authenticated, with `acks=all` to Redpanda before returning 200. That model
breaks down when you want to record millions of small counters/timings from
PHP, where every page view is a fresh process and there's no in-process
buffer to amortize HTTP calls against.

This agent is the standard fix: push aggregation state out of the
short-lived process and into a long-running sidecar. Same pattern as
statsd / DogStatsD — but the wire to mesh0 is HTTPS, not UDP.

## Quick start (Docker)

```bash
docker run --rm \
  -e MESH0_API_KEY=m0_... \
  -e MESH0_PROJECT_ID=$(uuidgen) \
  -p 8125:8125/udp \
  ghcr.io/mesh0-ai/metrics-agent:latest
```

From the app:

```bash
echo -n "checkout.charge:1|c|#tier:pro" | nc -u -w0 127.0.0.1 8125
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
            - { name: MESH0_API_KEY,    valueFrom: { secretKeyRef: { name: mesh0, key: api-key } } }
            - { name: MESH0_PROJECT_ID, valueFrom: { secretKeyRef: { name: mesh0, key: project-id } } }
          ports:
            - { containerPort: 8125, protocol: UDP }
          resources:
            requests: { cpu: "10m",  memory: "16Mi" }
            limits:   { cpu: "200m", memory: "64Mi" }
```

The pod's other containers reach the agent over loopback — UDP never leaves
the pod, so packet loss is a non-issue and there's no auth to wire up
between the app and the agent.

## PHP example

```php
$sock = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
$line = "checkout.charge:1|c|#tier:pro,region:us-east-1";
socket_sendto($sock, $line, strlen($line), 0, '127.0.0.1', 8125);
```

A thin composer package (`mesh0/metrics-php`) wraps this with timers,
histograms, and tag handling — but the wire format is open and any
existing statsd/DogStatsD client will work.

## Wire format

Standard statsd, with DogStatsD tag and sample-rate extensions:

```
metric.name:value|type[|@sample_rate][|#tag1:v1,tag2:v2]
```

| Type | Meaning                                      |
|------|----------------------------------------------|
| `c`  | counter — summed per series per flush window |
| `g`  | gauge — last-write-wins per series           |
| `ms` | timing in milliseconds                       |
| `h`  | histogram (alias for `ms`)                   |
| `d`  | distribution (alias for `ms`)                |

Multiple metrics may share one packet, separated by `\n`.

## Configuration

All knobs are environment variables:

| Variable                  | Default                      | Notes                                  |
|---------------------------|------------------------------|----------------------------------------|
| `MESH0_API_KEY`           | (required)                   | Per-project API key (`m0_…`).          |
| `MESH0_PROJECT_ID`        | (required)                   | Project UUID metrics are billed to.    |
| `MESH0_GATEWAY_URL`       | `https://gateway.mesh0.ai`   | Override for self-hosted / staging.    |
| `MESH0_FLUSH_PATH`        | `/v1/metrics`                | Path appended to gateway URL.          |
| `MESH0_LISTEN_ADDR`       | `0.0.0.0:8125`               | Bind address.                          |
| `MESH0_FLUSH_INTERVAL_MS` | `10000`                      | Min `1000`. Lower = more HTTP traffic. |
| `MESH0_LOG_LEVEL`         | `info`                       | `debug` \| `info` \| `warn` \| `error` |

## Operational notes

- **Aggregation is in-process and unreplicated.** If the agent crashes
  between flushes, the current window is lost. Counters/gauges/timings are
  not durable telemetry — use `/v1/events` for anything you need exactly
  once.
- **Cap on timing samples per series** is 10,000 per flush window. Beyond
  that, percentiles are computed off the first 10K samples (count/sum/min/max
  remain exact). Plenty for percentile signal at sub-minute flush windows.
- **Resource floor** is ~10 MB RSS. CPU scales with metric line rate; on
  a single core the parser does ~3M lines/sec.
- **Drops on overload.** The internal channel between UDP reader and
  aggregator is 100K deep. If the aggregator falls behind that far, the
  reader blocks and the kernel buffer absorbs the next burst. Beyond the
  kernel buffer, the OS drops UDP packets — a deliberate fire-and-forget
  trade-off, not a bug.

## Build from source

```bash
go build -ldflags="-X main.Version=$(git describe --tags --always)" .
go test ./...
```

## License

[Apache 2.0](./LICENSE).
