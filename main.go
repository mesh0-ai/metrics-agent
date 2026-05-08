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
	ProjectID     string
	GatewayURL    string
	FlushPath     string
	ListenAddr    string
	HealthAddr    string
	FlushInterval time.Duration
	ShutdownGrace time.Duration
	LogLevel      slog.Level
}

func loadConfig() (Config, error) {
	c := Config{
		APIKey:        os.Getenv("MESH0_API_KEY"),
		ProjectID:     os.Getenv("MESH0_PROJECT_ID"),
		GatewayURL:    envOr("MESH0_GATEWAY_URL", "https://gateway.mesh0.ai"),
		FlushPath:     envOr("MESH0_FLUSH_PATH", "/v1/metrics"),
		ListenAddr:    envOr("MESH0_LISTEN_ADDR", "0.0.0.0:8125"),
		HealthAddr:    envOr("MESH0_HEALTH_ADDR", "0.0.0.0:8126"),
		FlushInterval: 10 * time.Second,
		ShutdownGrace: 15 * time.Second,
		LogLevel:      slog.LevelInfo,
	}
	if v := os.Getenv("MESH0_FLUSH_INTERVAL_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1000 || ms > 600_000 {
			return c, fmt.Errorf("MESH0_FLUSH_INTERVAL_MS must be an integer in [1000, 600000]")
		}
		c.FlushInterval = time.Duration(ms) * time.Millisecond
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
	if c.ProjectID == "" {
		return c, errors.New("MESH0_PROJECT_ID is required")
	}
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// metricsBuffer is the staging chan between the UDP reader and the
// aggregator. Sized to absorb burst traffic during a flush; if it fills,
// the UDP reader will block and the kernel buffer drains next.
const metricsBuffer = 100_000

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	log.Info("starting mesh0 metrics agent",
		"version", Version,
		"listen", cfg.ListenAddr,
		"gateway", cfg.GatewayURL+cfg.FlushPath,
		"flush_interval", cfg.FlushInterval,
		"project_id", cfg.ProjectID,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	stats := newSelfStats()
	metricsCh := make(chan Metric, metricsBuffer)
	snapshotCh := make(chan Snapshot, 4)
	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	agg := newAggregator(metricsCh, ticker.C, snapshotCh, stats)
	flush := newFlusher(snapshotCh, cfg, log, stats)

	healthSrv := startHealthServer(cfg.HealthAddr, stats, log)

	// Shutdown is a chain of channel closes so no goroutine has to
	// guess when its upstream is finished:
	//   ctx -> listener returns -> close(metricsCh) -> aggregator emits
	//   final snapshot and returns -> close(snapshotCh) -> flusher drains.
	listenerErr := make(chan error, 1)
	go func() { listenerErr <- listen(ctx, cfg.ListenAddr, metricsCh, log, stats) }()

	aggDone := make(chan struct{})
	go func() {
		agg.run()
		close(aggDone)
	}()

	flushCtx, flushCancel := context.WithCancel(context.Background())
	flush.ctx = flushCtx
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

	close(metricsCh)
	<-aggDone
	close(snapshotCh)

	// Bound the time we wait for in-flight flushes; cancel the flusher's
	// HTTP context if we exceed grace so we don't outlive k8s SIGKILL.
	if cfg.ShutdownGrace > 0 {
		select {
		case <-flushDone:
		case <-time.After(cfg.ShutdownGrace):
			log.Warn("shutdown grace exceeded, cancelling in-flight flushes",
				"grace", cfg.ShutdownGrace)
			flushCancel()
			<-flushDone
		}
	} else {
		flushCancel()
		<-flushDone
	}
	flushCancel()

	if healthSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = healthSrv.Shutdown(shutCtx)
		shutCancel()
	}
	log.Info("shutdown complete")
}
