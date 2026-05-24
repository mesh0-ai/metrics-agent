package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
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

// TestListenerRoutesToCorrectPipeline is the end-to-end guard: real UDS
// datagrams with `_project` set must reach the matching pipeline's
// EventsReceived counter and nowhere else. Missing/unknown projects must
// land in the process-wide unrouted drop counters.
//
// The flusher is not stubbed — pipelineStats.EventsReceived is incremented
// by the batcher AFTER validateEvent succeeds and BEFORE the flush
// attempt, so the assertion does not require a fake gateway. Flush
// failures during the test are expected and irrelevant to the routing
// contract under test.
func TestListenerRoutesToCorrectPipeline(t *testing.T) {
	dir := t.TempDir()
	keysPath := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(keysPath, []byte(`{"ws-42":"m0_a","ws-99":"m0_b"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.APIKey = "m0_default" // registered under _default, fallback for no-_project datagrams
	cfg.KeysFile = keysPath
	cfg.MaxRetries = 0 // don't burn time retrying against a bogus gateway

	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	sockPath := shortTempSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenErr := make(chan error, 1)
	go func() { listenErr <- listen(ctx, sockPath, DefaultMaxEventBytes+1, reg, log, stats) }()

	if err := waitForSocket(sockPath, 500*time.Millisecond); err != nil {
		t.Fatalf("socket not ready: %v", err)
	}

	cliAddr, err := net.ResolveUnixAddr("unixgram", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := net.DialUnix("unixgram", nil, cliAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	cases := []struct {
		name string
		body string
	}{
		{"ws-42 first", `{"_project":"ws-42","operation":"a"}`},
		{"ws-42 second", `{"_project":"ws-42","operation":"b"}`},
		{"ws-99 once", `{"_project":"ws-99","operation":"c"}`},
		{"no _project goes to default", `{"operation":"d"}`},
		{"unknown project drops", `{"_project":"ws-zzz","operation":"e"}`},
		{"no _project, multi-tenant + default → default", `{"operation":"f"}`},
	}
	for _, c := range cases {
		if _, err := cli.Write([]byte(c.body)); err != nil {
			t.Fatalf("%s: write: %v", c.name, err)
		}
	}

	waitFor := func(get func() uint64, want uint64) bool {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if get() == want {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}

	p42, _ := reg.lookup("ws-42")
	p99, _ := reg.lookup("ws-99")
	pDef, _ := reg.lookup("")

	if !waitFor(p42.stats.EventsReceived.Load, 2) {
		t.Errorf("ws-42 EventsReceived: got %d want 2", p42.stats.EventsReceived.Load())
	}
	if !waitFor(p99.stats.EventsReceived.Load, 1) {
		t.Errorf("ws-99 EventsReceived: got %d want 1", p99.stats.EventsReceived.Load())
	}
	if !waitFor(pDef.stats.EventsReceived.Load, 2) {
		t.Errorf("_default EventsReceived: got %d want 2", pDef.stats.EventsReceived.Load())
	}
	if !waitFor(stats.DropsUnroutedUnknown.Load, 1) {
		t.Errorf("DropsUnroutedUnknown: got %d want 1", stats.DropsUnroutedUnknown.Load())
	}
	// Sanity: cross-tenant leak would show up here.
	if got := p42.stats.EventsReceived.Load(); got != 2 {
		t.Errorf("ws-42 leak check: got %d want exactly 2", got)
	}
	if got := p99.stats.EventsReceived.Load(); got != 1 {
		t.Errorf("ws-99 leak check: got %d want exactly 1", got)
	}

	cancel()
	<-listenErr
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
