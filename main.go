package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Version is overridden at build time via -ldflags "-X main.Version=...".
var Version = "dev"

// Config is the agent's runtime knobs. All come from the environment so the
// container ships with no flags to remember.
type Config struct {
	GatewayURL    string
	EventsPath    string
	ListenPath    string
	HealthAddr    string
	BatchWindow   time.Duration
	MaxBatch      int
	MaxEventBytes int
	QueueSize     int
	MaxRetries    int
	ShutdownGrace time.Duration
	LogLevel      slog.Level
	// MaxProjects bounds the number of registered pipelines. Each pipeline
	// owns a per-project queue, two goroutines, and an http.Client —
	// worst-case in-flight memory is MaxProjects * QueueSize * MaxEventBytes.
	//
	// 0 means UNLIMITED and is the default. The cap guards against a caller
	// minting a distinct `_project` per request (e.g. one per request_id)
	// and spawning unbounded goroutines. Since every registration requires a
	// valid `_token`, reaching a sane cap means an emitter is misbehaving.
	MaxProjects int
	// InlineIdle retires a registered pipeline after this long without
	// traffic. Every pipeline is registered on demand from a `_token`, so
	// this is what bounds the population for deployments whose projects turn
	// over. 0 disables expiry.
	InlineIdle time.Duration
}

func loadConfig() (Config, error) {
	c := Config{
		GatewayURL:    envOr("MESH0_BASE_URL", "https://api.mesh0.ai"),
		EventsPath:    envOr("MESH0_EVENTS_PATH", "/v1/events"),
		ListenPath:    envOr("MESH0_LISTEN_PATH", "/run/mesh0/agent.sock"),
		HealthAddr:    envOr("MESH0_HEALTH_ADDR", ":8126"),
		BatchWindow:   200 * time.Millisecond,
		MaxBatch:      500,
		MaxEventBytes: DefaultMaxEventBytes,
		// Per-pipeline default. Multi-tenant deployments register one
		// pipeline per project, so the process-wide ceiling is
		// (QueueSize * registered projects).
		QueueSize:     2_000,
		MaxRetries:    4,
		ShutdownGrace: 15 * time.Second,
		LogLevel:      slog.LevelInfo,
		MaxProjects:   0,
		InlineIdle:    15 * time.Minute,
	}
	if v := os.Getenv("MESH0_BATCH_WINDOW_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1 || ms > 60_000 {
			return c, fmt.Errorf("MESH0_BATCH_WINDOW_MS must be an integer in [1, 60000]")
		}
		c.BatchWindow = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("MESH0_MAX_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > MaxEventsPerBatch {
			return c, fmt.Errorf("MESH0_MAX_BATCH must be an integer in [1, %d]", MaxEventsPerBatch)
		}
		c.MaxBatch = n
	}
	if v := os.Getenv("MESH0_MAX_EVENT_BYTES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < MinMaxEventBytes || n > MaxMaxEventBytes {
			return c, fmt.Errorf("MESH0_MAX_EVENT_BYTES must be an integer in [%d, %d]", MinMaxEventBytes, MaxMaxEventBytes)
		}
		c.MaxEventBytes = n
	}
	if v := os.Getenv("MESH0_QUEUE_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return c, fmt.Errorf("MESH0_QUEUE_SIZE must be a positive integer")
		}
		c.QueueSize = n
	}
	if v := os.Getenv("MESH0_MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 16 {
			return c, fmt.Errorf("MESH0_MAX_RETRIES must be an integer in [0, 16]")
		}
		c.MaxRetries = n
	}
	if v := os.Getenv("MESH0_SHUTDOWN_GRACE_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 0 {
			return c, fmt.Errorf("MESH0_SHUTDOWN_GRACE_MS must be a non-negative integer")
		}
		c.ShutdownGrace = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("MESH0_MAX_PROJECTS"); v != "" {
		n, err := strconv.Atoi(v)
		// 0 is accepted and means unlimited, matching the default and the
		// `MaxProjects > 0` guards in routing.go. A NEGATIVE value is
		// rejected rather than read as unlimited, so "-1" is not a synonym.
		if err != nil || n < 0 || n > 4096 {
			return c, fmt.Errorf("MESH0_MAX_PROJECTS must be an integer in [0, 4096] (0 disables the cap)")
		}
		c.MaxProjects = n
	}
	if v := os.Getenv("MESH0_INLINE_IDLE_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		// 0 disables expiry; the upper bound is a day, past which "idle" has
		// stopped meaning anything a sidecar's lifetime can observe.
		if err != nil || ms < 0 || ms > 86_400_000 {
			return c, fmt.Errorf("MESH0_INLINE_IDLE_MS must be an integer in [0, 86400000]")
		}
		c.InlineIdle = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("MESH0_LOG_LEVEL"); v != "" {
		switch v {
		case "debug":
			c.LogLevel = slog.LevelDebug
		case "info":
			c.LogLevel = slog.LevelInfo
		case "warn":
			c.LogLevel = slog.LevelWarn
		case "error":
			c.LogLevel = slog.LevelError
		default:
			return c, fmt.Errorf("MESH0_LOG_LEVEL must be debug|info|warn|error")
		}
	}
	if c.ListenPath == "" {
		return c, errors.New("MESH0_LISTEN_PATH is required")
	}
	// sun_path is 104 bytes on macOS and 108 on Linux; use the smaller cap so
	// the same config is portable. The kernel returns EINVAL otherwise, which
	// surfaces as an opaque "bind: invalid argument".
	if len(c.ListenPath) > 103 {
		return c, fmt.Errorf("MESH0_LISTEN_PATH must be <= 103 bytes (got %d)", len(c.ListenPath))
	}
	return c, nil
}

// retiredEnv names the credential knobs that existed before routing became
// `_token`-only. They are no longer read at all.
//
// They are WARNED about rather than silently ignored, and rather than made
// fatal. Silence is wrong because a manifest still setting MESH0_KEYS_FILE
// looks configured to whoever reads it while doing nothing — the exact class
// of confusion that hid the reload bug this removal fixes. Fatal is wrong
// because it would turn a harmless leftover in a chart into a crash loop
// during the rollout that removes it.
var retiredEnv = []string{
	"MESH0_API_KEY",
	"MESH0_KEYS_FILE",
	"MESH0_KEYS_POLL_MS",
	"MESH0_INLINE_TOKENS",
	"MESH0_REQUIRE_PROJECT",
}

// warnRetiredEnv logs any retired knob still present in the environment.
func warnRetiredEnv(log *slog.Logger) {
	for _, k := range retiredEnv {
		if os.Getenv(k) != "" {
			log.Warn("ignoring retired environment variable; routing is inline-token only",
				"env", k)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	warnRetiredEnv(log)
	log.Info("starting mesh0 metrics agent",
		"version", Version,
		"listen", cfg.ListenPath,
		"endpoint", cfg.GatewayURL+cfg.EventsPath,
		"batch_window", cfg.BatchWindow,
		"max_batch", cfg.MaxBatch,
		"max_event_bytes", cfg.MaxEventBytes,
		"queue_size", cfg.QueueSize,
		"inline_idle", cfg.InlineIdle,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	stats := newSelfStats()

	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		fmt.Fprintln(os.Stderr, "routing:", err)
		os.Exit(2)
	}

	healthSrv := startHealthServer(cfg.HealthAddr, stats, reg, log)

	// Idle expiry. Every pipeline is registered on demand from a `_token`,
	// so this is the only thing bounding the population.
	if cfg.InlineIdle > 0 {
		expiryStop := make(chan struct{})
		defer close(expiryStop)
		go reg.runInlineExpiry(cfg.InlineIdle, expiryStop)
	}

	listenerErr := make(chan error, 1)
	go func() { listenerErr <- listen(ctx, cfg.ListenPath, cfg.MaxEventBytes+1, reg, log, stats) }()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
		<-listenerErr
	case err := <-listenerErr:
		if err != nil {
			stats.ListenerFatal.Store(true)
			log.Error("listener exited", "err", err)
		}
		cancel()
	}

	reg.shutdown(cfg.ShutdownGrace)

	if healthSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = healthSrv.Shutdown(shutCtx)
		shutCancel()
	}
	log.Info("shutdown complete")
}
