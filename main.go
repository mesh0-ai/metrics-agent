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
	GatewayURL    string
	EventsPath    string
	ListenPath    string
	HealthAddr    string
	BatchWindow   time.Duration
	MaxBatch      int
	QueueSize     int
	MaxRetries    int
	ShutdownGrace time.Duration
	LogLevel      slog.Level
}

func loadConfig() (Config, error) {
	c := Config{
		APIKey:        os.Getenv("MESH0_API_KEY"),
		GatewayURL:    envOr("MESH0_BASE_URL", "https://api.mesh0.ai"),
		EventsPath:    envOr("MESH0_EVENTS_PATH", "/v1/events"),
		ListenPath:    envOr("MESH0_LISTEN_PATH", "/run/mesh0/agent.sock"),
		HealthAddr:    envOr("MESH0_HEALTH_ADDR", ":8126"),
		BatchWindow:   200 * time.Millisecond,
		MaxBatch:      500,
		QueueSize:     10_000,
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
	if c.APIKey == "" {
		return c, errors.New("MESH0_API_KEY is required")
	}
	if c.ListenPath == "" {
		return c, errors.New("MESH0_LISTEN_PATH is required")
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
		"queue_size", cfg.QueueSize,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	stats := newSelfStats()
	rawCh := make(chan rawDatagram, cfg.QueueSize)
	batchCh := make(chan EventBatch, 8)

	batcher := newEventsBatcher(rawCh, batchCh, stats, log, cfg.MaxBatch, cfg.BatchWindow)
	flush := newEventsFlusher(batchCh, cfg, log, stats)

	flushCtx, flushCancel := context.WithCancel(context.Background())
	flush.ctx = flushCtx
	batcher.ctx = flushCtx

	healthSrv := startHealthServer(cfg.HealthAddr, stats, log)

	// Shutdown chain:
	//   ctx -> listener returns -> close(rawCh) -> batcher emits final
	//   batch and returns -> close(batchCh) -> flusher drains.
	listenerErr := make(chan error, 1)
	go func() { listenerErr <- listen(ctx, cfg.ListenPath, rawCh, log, stats) }()

	batcherDone := make(chan struct{})
	go func() {
		batcher.run()
		close(batcherDone)
	}()

	flushDone := make(chan struct{})
	go func() {
		flush.run()
		close(flushDone)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
		<-listenerErr
	case err := <-listenerErr:
		if err != nil {
			log.Error("listener exited", "err", err)
		}
		cancel()
	}

	close(rawCh)

	// Arm the grace timer before waiting on the batcher: if the flusher is
	// wedged (slow POST + full batchCh), the batcher's final flush would
	// block forever on send. Firing flushCancel after grace lets both the
	// flusher's in-flight POST and the batcher's final flush abort, the
	// latter accounting its events as drops.shutdown.
	var graceTimer *time.Timer
	if cfg.ShutdownGrace > 0 {
		graceTimer = time.AfterFunc(cfg.ShutdownGrace, func() {
			log.Warn("shutdown grace exceeded, cancelling in-flight flushes",
				"grace", cfg.ShutdownGrace)
			flushCancel()
		})
	} else {
		flushCancel()
	}

	<-batcherDone
	close(batchCh)
	<-flushDone
	if graceTimer != nil {
		graceTimer.Stop()
	}
	flushCancel()

	if healthSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = healthSrv.Shutdown(shutCtx)
		shutCancel()
	}
	log.Info("shutdown complete")
}
