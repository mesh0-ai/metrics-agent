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
	// MaxProjects bounds the number of registered pipelines (including the
	// MESH0_API_KEY fallback). Each pipeline owns a per-project queue, two
	// goroutines, and an http.Client — worst-case in-flight memory is
	// MaxProjects * QueueSize * MaxEventBytes.
	//
	// 0 means UNLIMITED and is the default. The cap exists to stop a
	// MISCONFIGURED keys file (e.g. one entry per request_id) spawning
	// unbounded goroutines, but it defends against a shape of bug that has
	// not occurred while reliably breaking a legitimate one that has: a
	// keys file is written wholesale by a control plane, so a real
	// deployment with more projects than the cap has its ENTIRE file
	// rejected and routes nothing at all. Silence for every project is a
	// worse failure than the memory growth the ceiling was guarding, and it
	// is not self-announcing on the caller's side.
	//
	// Deployments that want the guard set MESH0_MAX_PROJECTS to a positive
	// value and size it against their own keys file.
	MaxProjects int
	// KeysPollInterval is how often the agent re-reads MESH0_KEYS_FILE on
	// its own, independent of SIGHUP. On Kubernetes the Secret volume
	// propagates asynchronously (up to ~a minute after the API write), so an
	// external SIGHUP sent right after the write can reload stale contents
	// and never be retried; polling guarantees eventual pickup. 0 disables
	// polling (SIGHUP-only). Ignored when MESH0_KEYS_FILE is unset.
	KeysPollInterval time.Duration
	// RequireProject disables the MESH0_API_KEY fallback for datagrams
	// arriving without a `_project` field. Recommended for multi-tenant
	// deployments to surface mis-tagged callers as `unrouted_missing_project`
	// rather than silently cross-attributing to whatever tenant owns the
	// default key.
	RequireProject bool

	// InlineTokens honors a per-datagram `_token` credential: it authorizes
	// that datagram's batch, and a project seen only via such a datagram is
	// registered on demand without a keys-file entry.
	//
	// DEFAULT ON, and that is a no-op for existing deployments: a datagram
	// that carries no `_token` takes exactly the path it took before, and a
	// project can only be auto-registered by a caller that presented a
	// credential for it. Set MESH0_INLINE_TOKENS=0 to refuse inline
	// credentials outright.
	InlineTokens bool
	// InlineIdle retires an inline-registered pipeline after this long
	// without traffic. Keys-file pipelines are never expired — an operator
	// declared those. 0 disables expiry.
	InlineIdle time.Duration
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
		QueueSize:        2_000,
		MaxRetries:       4,
		ShutdownGrace:    15 * time.Second,
		LogLevel:         slog.LevelInfo,
		MaxProjects:      0,
		KeysPollInterval: 30 * time.Second,
		InlineTokens:     true,
		InlineIdle:       15 * time.Minute,
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
	if v := os.Getenv("MESH0_KEYS_POLL_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 0 || ms > 3_600_000 {
			return c, fmt.Errorf("MESH0_KEYS_POLL_MS must be an integer in [0, 3600000] (0 disables polling)")
		}
		c.KeysPollInterval = time.Duration(ms) * time.Millisecond
	}
	if v := os.Getenv("MESH0_INLINE_TOKENS"); v != "" {
		switch v {
		case "1", "true", "TRUE":
			c.InlineTokens = true
		case "0", "false", "FALSE":
			c.InlineTokens = false
		default:
			return c, fmt.Errorf("MESH0_INLINE_TOKENS must be 0|1|true|false")
		}
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
	if v := os.Getenv("MESH0_REQUIRE_PROJECT"); v != "" {
		switch v {
		case "1", "true", "TRUE":
			c.RequireProject = true
		case "0", "false", "FALSE":
			c.RequireProject = false
		default:
			return c, fmt.Errorf("MESH0_REQUIRE_PROJECT must be 1|0|true|false")
		}
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
	// An inline-token deployment declares nothing up front — the credential
	// arrives on the datagram — so it legitimately has neither knob set. The
	// guard still applies when inline tokens are disabled, where having
	// neither really does mean the agent can authenticate nothing.
	if c.APIKey == "" && c.KeysFile == "" && !c.InlineTokens {
		return c, errors.New("set MESH0_API_KEY (single-tenant), MESH0_KEYS_FILE (multi-tenant), or enable MESH0_INLINE_TOKENS")
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
		"keys_poll", cfg.KeysPollInterval,
		"inline_tokens", cfg.InlineTokens,
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

	// SIGHUP reloads the keys file. Done on a separate signal channel so it
	// doesn't compete with the SIGINT/SIGTERM shutdown context above.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	// The keys file is also re-read on a timer: an external SIGHUP can race
	// the async Secret-volume propagation on Kubernetes (reload the old
	// contents and never fire again), and a sidecar that started before keys
	// were provisioned would otherwise run keyless until restart. reload() is
	// a quiet no-op when the file is unchanged, so the tick is cheap.
	var pollCh <-chan time.Time
	if cfg.KeysFile != "" && cfg.KeysPollInterval > 0 {
		ticker := time.NewTicker(cfg.KeysPollInterval)
		defer ticker.Stop()
		pollCh = ticker.C
	}
	hupDone := make(chan struct{})
	go func() {
		defer close(hupDone)
		for {
			select {
			case <-hupCh:
				reg.reload()
			case <-pollCh:
				reg.reload()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Inline-pipeline idle expiry. Only meaningful when inline registration
	// can happen at all; a keys-file-only deployment never creates one.
	if cfg.InlineTokens && cfg.InlineIdle > 0 {
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

	<-hupDone

	reg.shutdown(cfg.ShutdownGrace)

	if healthSrv != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = healthSrv.Shutdown(shutCtx)
		shutCancel()
	}
	log.Info("shutdown complete")
}
