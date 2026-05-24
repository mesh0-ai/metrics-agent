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
	APIKey        string
	KeysFile      string
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
}

func loadConfig() (Config, error) {
	c := Config{
		APIKey:        os.Getenv("MESH0_API_KEY"),
		KeysFile:      os.Getenv("MESH0_KEYS_FILE"),
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
	if c.APIKey == "" && c.KeysFile == "" {
		return c, errors.New("set MESH0_API_KEY (single-tenant) or MESH0_KEYS_FILE (multi-tenant)")
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
	log.Info("starting mesh0 metrics agent",
		"version", Version,
		"listen", cfg.ListenPath,
		"endpoint", cfg.GatewayURL+cfg.EventsPath,
		"batch_window", cfg.BatchWindow,
		"max_batch", cfg.MaxBatch,
		"max_event_bytes", cfg.MaxEventBytes,
		"queue_size", cfg.QueueSize,
		"keys_file", cfg.KeysFile,
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

	// SIGHUP reloads the keys file. Done on a separate signal channel so it
	// doesn't compete with the SIGINT/SIGTERM shutdown context above.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	hupDone := make(chan struct{})
	go func() {
		defer close(hupDone)
		for {
			select {
			case <-hupCh:
				reg.reload()
			case <-ctx.Done():
				return
			}
		}
	}()

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

	<-hupDone

	reg.shutdown(cfg.ShutdownGrace)

	if healthSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = healthSrv.Shutdown(shutCtx)
		shutCancel()
	}
	log.Info("shutdown complete")
}
