package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractAndStripProject_NoField(t *testing.T) {
	in := []byte(`{"a":1,"b":"hi"}`)
	proj, out, removed := extractAndStripProject(in)
	if removed {
		t.Errorf("removed=true, expected false")
	}
	if proj != "" {
		t.Errorf("project: got %q", proj)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

func TestExtractAndStripProject_FirstField(t *testing.T) {
	in := []byte(`{"_project":"ws-42","a":1,"b":"hi"}`)
	proj, out, removed := extractAndStripProject(in)
	if !removed || proj != "ws-42" {
		t.Fatalf("got proj=%q removed=%v", proj, removed)
	}
	if !json.Valid(out) {
		t.Errorf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %+v", m)
	}
	if m["a"] == nil || m["b"] == nil {
		t.Errorf("siblings lost: %+v", m)
	}
}

func TestExtractAndStripProject_MiddleField(t *testing.T) {
	in := []byte(`{"a":1,"_project":"ws-99","b":"hi"}`)
	proj, out, removed := extractAndStripProject(in)
	if !removed || proj != "ws-99" {
		t.Fatalf("got proj=%q removed=%v", proj, removed)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %+v", m)
	}
}

func TestExtractAndStripProject_OnlyField(t *testing.T) {
	in := []byte(`{"_project":"solo"}`)
	proj, out, removed := extractAndStripProject(in)
	if !removed || proj != "solo" {
		t.Fatalf("got proj=%q removed=%v", proj, removed)
	}
	if string(out) != "{}" {
		t.Errorf("output: got %q, want {}", out)
	}
}

func TestExtractAndStripProject_NotObject(t *testing.T) {
	for _, c := range []string{`[]`, `123`, ``, `null`} {
		_, _, removed := extractAndStripProject([]byte(c))
		if removed {
			t.Errorf("removed=true for %q", c)
		}
	}
}

func TestLoadKeysFile_OK(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	contents := `{"ws-42":"m0_aaa","ws-99":"m0_bbb"}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := loadKeysFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if keys["ws-42"] != "m0_aaa" || keys["ws-99"] != "m0_bbb" {
		t.Errorf("got %+v", keys)
	}
}

func TestLoadKeysFile_RejectsReservedPrefix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"_default":"m0_x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadKeysFile(path); err == nil {
		t.Error("expected error for underscore-prefixed project")
	}
}

func TestLoadKeysFile_RejectsEmptyKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":""}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadKeysFile(path); err == nil {
		t.Error("expected error for empty api key")
	}
}

func TestRegistry_DispatchLegacyDefault(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "m0_default"
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	// No _project → routes to _default pipeline.
	dg := rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now()}
	ok, _ := reg.dispatch(dg)
	if !ok {
		t.Error("expected dispatch=true to default pipeline")
	}
	pipe, _ := reg.lookup("")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if pipe.stats.EventsReceived.Load() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("datagram never reached default pipeline (events_received=%d)", pipe.stats.EventsReceived.Load())
}

func TestRegistry_DispatchMissingProjectWhenMultiTenant(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = "" // multi-tenant only: no default fallback
	cfg.KeysFile = path
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	// No _project on the wire → missing_project drop.
	reg.dispatch(rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now()})
	if stats.DropsUnroutedMissing.Load() != 1 {
		t.Errorf("missing: got %d", stats.DropsUnroutedMissing.Load())
	}
	// Unknown project → unknown_project drop.
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-zzz"}`), at: time.Now()})
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unknown: got %d", stats.DropsUnroutedUnknown.Load())
	}
	// Known project → delivered.
	ok, _ := reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-42","a":1}`), at: time.Now()})
	if !ok {
		t.Error("expected dispatch to ws-42")
	}
}

func TestRegistry_ReloadAddsAndRemovesProjects(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	cfg.ShutdownGrace = 0
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	if _, ok := reg.lookup("ws-42"); !ok {
		t.Fatal("ws-42 not registered initially")
	}

	// Add ws-99, remove ws-42 via reload (atomic rename).
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"ws-99":"m0_b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	reg.reload()

	if _, ok := reg.lookup("ws-99"); !ok {
		t.Error("ws-99 not registered after reload")
	}
	if _, ok := reg.lookup("ws-42"); ok {
		t.Error("ws-42 still registered after removal")
	}
}

func TestRegistry_ReloadKeepsPreviousOnParseError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	reg.reload()

	if _, ok := reg.lookup("ws-42"); !ok {
		t.Error("ws-42 lost after bad reload — should keep previous table")
	}
}

// chanSink is a single-channel listenSink used by tests that exercise the
// listener without the routing layer. queueFull mirrors the original
// listener behavior so the existing drop-on-full tests still hit
// drops.queue_full.
type chanSink chan rawDatagram

func (c chanSink) dispatch(dg rawDatagram) (delivered bool, queueFull bool) {
	select {
	case c <- dg:
		return true, false
	default:
		return false, true
	}
}

func testConfig() Config {
	return Config{
		GatewayURL:    "http://localhost:0",
		EventsPath:    "/v1/events",
		BatchWindow:   200 * time.Millisecond,
		MaxBatch:      500,
		MaxEventBytes: DefaultMaxEventBytes,
		QueueSize:     16,
		MaxRetries:    0,
		ShutdownGrace: 0,
	}
}
