package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// loadConfig is the single chokepoint for env-driven validation, so a
// regression here can only surface at deploy time. Table-driven coverage
// for each env var's range and the cross-cutting "API key required" check.

func TestLoadConfigDefaults(t *testing.T) {
	clearMesh0Env(t)
	t.Setenv("MESH0_API_KEY", "secret")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.APIKey != "secret" {
		t.Errorf("APIKey: got %q", cfg.APIKey)
	}
	if cfg.GatewayURL != "https://api.mesh0.ai" {
		t.Errorf("GatewayURL default: got %q", cfg.GatewayURL)
	}
	if cfg.EventsPath != "/v1/events" {
		t.Errorf("EventsPath default: got %q", cfg.EventsPath)
	}
	if cfg.ListenPath != "/run/mesh0/agent.sock" {
		t.Errorf("ListenPath default: got %q", cfg.ListenPath)
	}
	if cfg.HealthAddr != ":8126" {
		t.Errorf("HealthAddr default: got %q", cfg.HealthAddr)
	}
	if cfg.BatchWindow != 200*time.Millisecond {
		t.Errorf("BatchWindow default: got %v", cfg.BatchWindow)
	}
	if cfg.MaxBatch != 500 {
		t.Errorf("MaxBatch default: got %d", cfg.MaxBatch)
	}
	if cfg.QueueSize != 2_000 {
		t.Errorf("QueueSize default: got %d", cfg.QueueSize)
	}
	if cfg.MaxRetries != 4 {
		t.Errorf("MaxRetries default: got %d", cfg.MaxRetries)
	}
	if cfg.ShutdownGrace != 15*time.Second {
		t.Errorf("ShutdownGrace default: got %v", cfg.ShutdownGrace)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel default: got %v", cfg.LogLevel)
	}
}

func TestLoadConfigRequiresAPIKey(t *testing.T) {
	clearMesh0Env(t)
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "MESH0_API_KEY") {
		t.Fatalf("expected MESH0_API_KEY required error, got %v", err)
	}
}

func TestLoadConfigAcceptsKeysFileWithoutAPIKey(t *testing.T) {
	clearMesh0Env(t)
	t.Setenv("MESH0_KEYS_FILE", "/some/path.json")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.KeysFile != "/some/path.json" {
		t.Errorf("KeysFile: got %q", cfg.KeysFile)
	}
	if cfg.APIKey != "" {
		t.Errorf("APIKey: got %q", cfg.APIKey)
	}
}

func TestLoadConfigValidationRanges(t *testing.T) {
	cases := []struct {
		name   string
		env    map[string]string
		want   string // substring of expected error; empty = expect success
		assert func(t *testing.T, c Config)
	}{
		{
			name: "batch_window valid",
			env:  map[string]string{"MESH0_BATCH_WINDOW_MS": "500"},
			assert: func(t *testing.T, c Config) {
				if c.BatchWindow != 500*time.Millisecond {
					t.Errorf("BatchWindow: got %v", c.BatchWindow)
				}
			},
		},
		{
			name: "batch_window zero rejected",
			env:  map[string]string{"MESH0_BATCH_WINDOW_MS": "0"},
			want: "MESH0_BATCH_WINDOW_MS",
		},
		{
			name: "batch_window over max rejected",
			env:  map[string]string{"MESH0_BATCH_WINDOW_MS": "60001"},
			want: "MESH0_BATCH_WINDOW_MS",
		},
		{
			name: "batch_window non-numeric rejected",
			env:  map[string]string{"MESH0_BATCH_WINDOW_MS": "abc"},
			want: "MESH0_BATCH_WINDOW_MS",
		},
		{
			name: "max_batch valid",
			env:  map[string]string{"MESH0_MAX_BATCH": "1000"},
			assert: func(t *testing.T, c Config) {
				if c.MaxBatch != 1000 {
					t.Errorf("MaxBatch: got %d", c.MaxBatch)
				}
			},
		},
		{
			name: "max_batch over server cap rejected",
			env:  map[string]string{"MESH0_MAX_BATCH": "5001"},
			want: "MESH0_MAX_BATCH",
		},
		{
			name: "max_batch zero rejected",
			env:  map[string]string{"MESH0_MAX_BATCH": "0"},
			want: "MESH0_MAX_BATCH",
		},
		{
			name: "max_event_bytes default",
			env:  map[string]string{},
			assert: func(t *testing.T, c Config) {
				if c.MaxEventBytes != DefaultMaxEventBytes {
					t.Errorf("MaxEventBytes: got %d want %d", c.MaxEventBytes, DefaultMaxEventBytes)
				}
			},
		},
		{
			name: "max_event_bytes valid",
			env:  map[string]string{"MESH0_MAX_EVENT_BYTES": "65536"},
			assert: func(t *testing.T, c Config) {
				if c.MaxEventBytes != 65536 {
					t.Errorf("MaxEventBytes: got %d", c.MaxEventBytes)
				}
			},
		},
		{
			name: "max_event_bytes below floor rejected",
			env:  map[string]string{"MESH0_MAX_EVENT_BYTES": "512"},
			want: "MESH0_MAX_EVENT_BYTES",
		},
		{
			name: "max_event_bytes above ceiling rejected",
			env:  map[string]string{"MESH0_MAX_EVENT_BYTES": "33554432"},
			want: "MESH0_MAX_EVENT_BYTES",
		},
		{
			name: "max_event_bytes non-numeric rejected",
			env:  map[string]string{"MESH0_MAX_EVENT_BYTES": "1MB"},
			want: "MESH0_MAX_EVENT_BYTES",
		},
		{
			name: "queue_size valid",
			env:  map[string]string{"MESH0_QUEUE_SIZE": "1"},
			assert: func(t *testing.T, c Config) {
				if c.QueueSize != 1 {
					t.Errorf("QueueSize: got %d", c.QueueSize)
				}
			},
		},
		{
			name: "queue_size zero rejected",
			env:  map[string]string{"MESH0_QUEUE_SIZE": "0"},
			want: "MESH0_QUEUE_SIZE",
		},
		{
			name: "max_retries zero allowed",
			env:  map[string]string{"MESH0_MAX_RETRIES": "0"},
			assert: func(t *testing.T, c Config) {
				if c.MaxRetries != 0 {
					t.Errorf("MaxRetries: got %d", c.MaxRetries)
				}
			},
		},
		{
			name: "max_retries over cap rejected",
			env:  map[string]string{"MESH0_MAX_RETRIES": "17"},
			want: "MESH0_MAX_RETRIES",
		},
		{
			name: "max_retries negative rejected",
			env:  map[string]string{"MESH0_MAX_RETRIES": "-1"},
			want: "MESH0_MAX_RETRIES",
		},
		{
			name: "shutdown_grace zero allowed",
			env:  map[string]string{"MESH0_SHUTDOWN_GRACE_MS": "0"},
			assert: func(t *testing.T, c Config) {
				if c.ShutdownGrace != 0 {
					t.Errorf("ShutdownGrace: got %v", c.ShutdownGrace)
				}
			},
		},
		{
			name: "shutdown_grace negative rejected",
			env:  map[string]string{"MESH0_SHUTDOWN_GRACE_MS": "-1"},
			want: "MESH0_SHUTDOWN_GRACE_MS",
		},
		{
			name: "log_level debug",
			env:  map[string]string{"MESH0_LOG_LEVEL": "debug"},
			assert: func(t *testing.T, c Config) {
				if c.LogLevel != slog.LevelDebug {
					t.Errorf("LogLevel: got %v", c.LogLevel)
				}
			},
		},
		{
			name: "log_level invalid rejected",
			env:  map[string]string{"MESH0_LOG_LEVEL": "trace"},
			want: "MESH0_LOG_LEVEL",
		},
		{
			name: "gateway and events path overrides",
			env:  map[string]string{"MESH0_BASE_URL": "https://eu.mesh0.ai", "MESH0_EVENTS_PATH": "/v2/events"},
			assert: func(t *testing.T, c Config) {
				if c.GatewayURL != "https://eu.mesh0.ai" || c.EventsPath != "/v2/events" {
					t.Errorf("override: got %q + %q", c.GatewayURL, c.EventsPath)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearMesh0Env(t)
			t.Setenv("MESH0_API_KEY", "k")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := loadConfig()
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("expected error containing %q, got %v", tc.want, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.assert != nil {
				tc.assert(t, cfg)
			}
		})
	}
}

// clearMesh0Env scrubs MESH0_* so test cases see a clean baseline regardless
// of the developer's shell.
func clearMesh0Env(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"MESH0_API_KEY",
		"MESH0_KEYS_FILE",
		"MESH0_BASE_URL",
		"MESH0_EVENTS_PATH",
		"MESH0_LISTEN_PATH",
		"MESH0_HEALTH_ADDR",
		"MESH0_BATCH_WINDOW_MS",
		"MESH0_MAX_BATCH",
		"MESH0_MAX_EVENT_BYTES",
		"MESH0_QUEUE_SIZE",
		"MESH0_MAX_RETRIES",
		"MESH0_SHUTDOWN_GRACE_MS",
		"MESH0_LOG_LEVEL",
	} {
		t.Setenv(k, "")
	}
}
