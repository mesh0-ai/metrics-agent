package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractAndStripProject_NoField(t *testing.T) {
	in := []byte(`{"a":1,"b":"hi"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed {
		t.Errorf("removed=%v malformed=%v, expected both false", removed, malformed)
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
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-42" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
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
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-99" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
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
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "solo" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if string(out) != "{}" {
		t.Errorf("output: got %q, want {}", out)
	}
}

func TestExtractAndStripProject_DuplicateKeysStripAllLastWins(t *testing.T) {
	// Duplicate top-level keys are non-canonical JSON but legal under
	// RFC 8259. Strip every occurrence (the gateway uses
	// DisallowUnknownFields and would 400 on any leak) and adopt the
	// last value for routing, matching json.Unmarshal semantics.
	in := []byte(`{"_project":"first","a":1,"_project":"middle","b":2,"_project":"last"}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "last" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if !json.Valid(out) {
		t.Fatalf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not fully stripped: %s", out)
	}
	if m["a"] == nil || m["b"] == nil {
		t.Errorf("siblings lost: %s", out)
	}
}

func TestExtractAndStripProject_NotObject(t *testing.T) {
	// Non-object JSON (array / scalar / empty / null) is not the routing
	// layer's job to drop — the validator will reject it as parse_error.
	// We just confirm we don't claim to have stripped anything.
	for _, c := range []string{`[]`, `123`, `null`} {
		_, _, removed, _, _ := extractAndStripProject([]byte(c))
		if removed {
			t.Errorf("removed=true for %q", c)
		}
	}
}

// TestExtractAndStripProject_LiteralInsideString guards against a regression
// where a substring matcher (instead of a real JSON decoder) treats the
// `_project` literal embedded in a STRING VALUE as if it were a top-level
// key. The streaming-decoder implementation handles this correctly; the
// test pins it so a future "fast path" rewrite can't silently break.
func TestExtractAndStripProject_LiteralInsideString(t *testing.T) {
	in := []byte(`{"msg":"this string contains \"_project\" verbatim","real":1}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed {
		t.Fatalf("got proj=%q removed=%v malformed=%v; expected pass-through", proj, removed, malformed)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

// TestExtractAndStripProject_LiteralInsideNestedKey: `_project` appears as
// a NESTED object key, not a top-level one. Must not be stripped or treated
// as a routing hint.
func TestExtractAndStripProject_LiteralInsideNestedKey(t *testing.T) {
	in := []byte(`{"meta":{"_project":"nested"},"a":1}`)
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if removed || malformed || proj != "" {
		t.Fatalf("got proj=%q removed=%v malformed=%v; expected pass-through", proj, removed, malformed)
	}
	if string(out) != string(in) {
		t.Errorf("output mutated: %q", out)
	}
}

// TestExtractAndStripProject_NonStringValue: `_project` with a non-string
// value (number, null, object) is well-formed JSON but unusable for routing.
// The key is still stripped (so the gateway doesn't 400) and badProject=true
// so dispatch counts it as unrouted_unknown instead of silently falling
// through to the DefaultProject pipeline.
func TestExtractAndStripProject_NonStringValue(t *testing.T) {
	for _, body := range []string{
		`{"_project":42,"a":1}`,
		`{"_project":null,"a":1}`,
		`{"_project":{"nested":"obj"},"a":1}`,
	} {
		proj, out, removed, malformed, badProject := extractAndStripProject([]byte(body))
		if malformed {
			t.Errorf("%s: malformed=true, expected false (well-formed JSON)", body)
		}
		if !badProject {
			t.Errorf("%s: badProject=false, expected true (non-string _project)", body)
		}
		if !removed {
			t.Errorf("%s: removed=false, expected true (key still stripped)", body)
		}
		if proj != "" {
			t.Errorf("%s: project=%q, expected empty (non-string value)", body, proj)
		}
		if !json.Valid(out) {
			t.Errorf("%s: output not valid JSON: %s", body, out)
		}
	}
}

// TestExtractAndStripProject_NonStringThenStringLastWins: mixed-type
// duplicates — non-string occurrence earlier, string occurrence last. The
// last (string) wins per json.Unmarshal semantics; badProject must clear.
func TestExtractAndStripProject_NonStringThenStringLastWins(t *testing.T) {
	in := []byte(`{"_project":42,"a":1,"_project":"good"}`)
	proj, _, removed, malformed, badProject := extractAndStripProject(in)
	if malformed || !removed || badProject || proj != "good" {
		t.Fatalf("got proj=%q removed=%v malformed=%v badProject=%v", proj, removed, malformed, badProject)
	}
}

// TestExtractAndStripProject_StringThenNonStringLastWins: string first,
// non-string last — last-wins means badProject must be set and project
// cleared, so the dispatcher accounts the drop rather than misrouting to
// the earlier string value.
func TestExtractAndStripProject_StringThenNonStringLastWins(t *testing.T) {
	in := []byte(`{"_project":"good","a":1,"_project":42}`)
	proj, _, removed, malformed, badProject := extractAndStripProject(in)
	if malformed || !removed || !badProject || proj != "" {
		t.Fatalf("got proj=%q removed=%v malformed=%v badProject=%v", proj, removed, malformed, badProject)
	}
}

// TestExtractAndStripProject_Whitespace: pretty-printed input with
// inter-token whitespace must still produce a well-formed output.
func TestExtractAndStripProject_Whitespace(t *testing.T) {
	in := []byte("{ \"a\":1, \"_project\" : \"ws-42\" , \"b\":2 }")
	proj, out, removed, malformed, _ := extractAndStripProject(in)
	if !removed || malformed || proj != "ws-42" {
		t.Fatalf("got proj=%q removed=%v malformed=%v", proj, removed, malformed)
	}
	if !json.Valid(out) {
		t.Errorf("output not valid JSON: %s", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, found := m["_project"]; found {
		t.Errorf("_project not stripped: %s", out)
	}
}

// TestExtractAndStripProject_Malformed: structurally broken JSON (non-string
// key, unterminated value) must report malformed=true so dispatch counts it
// as parse_error rather than forwarding poison bytes to the gateway.
func TestExtractAndStripProject_Malformed(t *testing.T) {
	for _, body := range []string{
		`{"_project":"x",`,       // truncated
		`{"_project":"x", trail`, // garbage after value
	} {
		_, _, _, malformed, _ := extractAndStripProject([]byte(body))
		if !malformed {
			t.Errorf("%q: malformed=false, expected true", body)
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

// TestRegistry_InstallEmptyKeysFile ensures a readable-but-empty keys file
// starts an empty routing table instead of exiting. On Kubernetes the mesh0
// Secret is templated as `{}` at deploy time and a reconciler fills it in
// after the pod starts — failing install turns that provisioning window into
// a crash loop. A later reload must then install the first routes.
func TestRegistry_InstallEmptyKeysFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatalf("install with empty keys file must not fail: %v", err)
	}
	defer reg.shutdown(0)

	// Nothing routable yet: datagrams drop as unrouted, not crash.
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-42","a":1}`), at: time.Now()})
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unknown drops: got %d", stats.DropsUnroutedUnknown.Load())
	}

	// Keys arrive (reconciler wrote the Secret): reload installs the route.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	reg.reload()
	if _, ok := reg.lookup("ws-42"); !ok {
		t.Error("ws-42 not registered after reload")
	}
}

// TestRegistry_ReloadUnchangedKeepsPipelines ensures the periodic keys-file
// poll is a quiet no-op when the contents haven't changed: live pipelines
// must be reused (no drain/replace churn on every tick) while the freshness
// timestamp still advances.
func TestRegistry_ReloadUnchangedKeepsPipelines(t *testing.T) {
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

	before, ok := reg.lookup("ws-42")
	if !ok {
		t.Fatal("ws-42 not registered")
	}
	reg.reload()
	after, ok := reg.lookup("ws-42")
	if !ok {
		t.Fatal("ws-42 gone after no-change reload")
	}
	if before != after {
		t.Error("pipeline replaced on no-change reload")
	}
	if reg.LastKeysReloadUnix.Load() == 0 {
		t.Error("freshness timestamp not stamped on no-change reload")
	}
}

// TestRegistry_DispatchMalformedBumpsParseError ensures a malformed datagram
// is counted as parse_error at the routing layer rather than silently
// forwarded to a pipeline (which would poison the batch with a 4xx).
func TestRegistry_DispatchMalformedBumpsParseError(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "m0_default"
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	// Trips the prefilter (contains `"_project"`) AND is structurally
	// broken — without the malformed return, this would be forwarded.
	bad := []byte(`{"_project":"x", oh no`)
	delivered, _ := reg.dispatch(rawDatagram{bytes: bad, at: time.Now()})
	if delivered {
		t.Error("expected delivered=false for malformed input")
	}
	if got := stats.DropsParseError.Load(); got != 1 {
		t.Errorf("DropsParseError: got %d want 1", got)
	}
}

// TestRegistry_ReloadReplacesOnKeyChange: a project whose api key changes
// across a reload must get a fresh pipeline (the old one drained). Without
// this, a rotated key would continue shipping under the stale credential.
func TestRegistry_ReloadReplacesOnKeyChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_old"}`), 0o600); err != nil {
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

	before, _ := reg.lookup("ws-42")
	if before == nil || before.apiKey != "m0_old" {
		t.Fatalf("initial pipeline: %+v", before)
	}

	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_new"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reg.reload()

	after, _ := reg.lookup("ws-42")
	if after == nil {
		t.Fatal("ws-42 missing after key rotation")
	}
	if after == before {
		t.Error("expected new pipeline instance after key change; got same pointer")
	}
	if after.apiKey != "m0_new" {
		t.Errorf("new apiKey: got %q want m0_new", after.apiKey)
	}
}

// TestRegistry_ReloadFailureBumpsCounter: a malformed keys file on SIGHUP
// must bump KeysReloadFailures so operators can detect a stale table from
// /stats without scraping logs.
func TestRegistry_ReloadFailureBumpsCounter(t *testing.T) {
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
	if got := reg.KeysReloadFailures.Load(); got != 1 {
		t.Errorf("KeysReloadFailures: got %d want 1", got)
	}
	if reg.LastKeysReloadUnix.Load() != 0 {
		t.Error("LastKeysReloadUnix should remain 0 after failed reload")
	}

	// Recover: a good file must zero the failure path's effect on
	// LastKeysReloadUnix (we don't reset the failures counter — it's
	// cumulative).
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reg.reload()
	if reg.LastKeysReloadUnix.Load() == 0 {
		t.Error("LastKeysReloadUnix not stamped after successful reload")
	}
}

// TestRegistry_MultiPipelineDrain: shutdown must drain all pipelines (N>1)
// with pending batches. Validates the fan-out drain in registry.shutdown.
func TestRegistry_MultiPipelineDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a","ws-99":"m0_b","ws-7":"m0_c"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	cfg.MaxRetries = 0
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{"ws-42", "ws-99", "ws-7"} {
		body := []byte(`{"_project":"` + p + `","a":1}`)
		reg.dispatch(rawDatagram{bytes: body, at: time.Now()})
	}

	done := make(chan struct{})
	go func() {
		reg.shutdown(500 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return within 2s — drain stuck")
	}
}

// TestRegistry_ConcurrentReloadAndDispatch is a race-detector guard: a
// reload that replaces pipelines must not race with concurrent dispatch.
// The atomic.Pointer swap + start-before-publish ordering should keep this
// safe; this test pins it under `go test -race`.
func TestRegistry_ConcurrentReloadAndDispatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	cfg.ShutdownGrace = 0
	cfg.MaxRetries = 0
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	stop := make(chan struct{})
	// Dispatch loop.
	go func() {
		body := []byte(`{"_project":"ws-42","op":"x"}`)
		for {
			select {
			case <-stop:
				return
			default:
				reg.dispatch(rawDatagram{bytes: body, at: time.Now()})
			}
		}
	}()
	// Reload loop: alternate api key so each iteration replaces the
	// pipeline (drain old, start new).
	go func() {
		alt := true
		for i := 0; i < 50; i++ {
			key := "m0_a"
			if alt {
				key = "m0_b"
			}
			alt = !alt
			contents := `{"ws-42":"` + key + `"}`
			tmp := path + ".tmp"
			if err := os.WriteFile(tmp, []byte(contents), 0o600); err != nil {
				return
			}
			_ = os.Rename(tmp, path)
			reg.reload()
		}
		close(stop)
	}()
	<-stop
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

// BenchmarkExtractAndStripProject measures the hot-path cost of routing's
// _project extraction. The no-field case is the dominant single-tenant
// workload; the prefilter should keep it allocation-free.
func BenchmarkExtractAndStripProject_NoField(b *testing.B) {
	in := []byte(`{"operation":"db.query","duration_ms":42,"status":200,"meta":{"k":"v"}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = extractAndStripProject(in)
	}
}

func BenchmarkExtractAndStripProject_FirstField(b *testing.B) {
	in := []byte(`{"_project":"ws-42","operation":"db.query","duration_ms":42}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = extractAndStripProject(in)
	}
}

// FuzzExtractAndStripProject backstops the hand-rolled JSON scanner against
// encoding/json. For every input the corpus produces, the scanner's result
// must agree with what a decoder-based oracle would have returned. The
// scanner is allowed to over-reject (return malformed=true on edge cases
// the stdlib accepts) because the downstream validator (events.go) re-runs
// json.Valid before the batch ships — but it must NEVER under-reject (let
// poison bytes through) and must NEVER misroute (produce a different
// project name for the same input).
func FuzzExtractAndStripProject(f *testing.F) {
	for _, seed := range []string{
		`{"_project":"x"}`,
		`{"a":1,"_project":"x","b":2}`,
		`{}`,
		`{"a":1}`,
		`[1,2,3]`,
		`{"_project":42}`,
		`{"_project":null}`,
		`{"_project":"a","_project":"b"}`,
		`{"_project":42,"_project":"b"}`,
		`{"msg":"contains \"_project\" literally"}`,
		`{"nested":{"_project":"inner"},"_project":"outer"}`,
		`{"_project":"with\"escape"}`,
		`{"_project":"é"}`,
		`{ "_project" : "x" , "a" : 1 }`,
		`{"a":1.5e10,"_project":"x"}`,
		`{"a":-0.1,"_project":"x"}`,
		`{"a":[1,{"_project":"shadowed"}],"_project":"real"}`,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		// Skip inputs that are too large or contain a NUL byte (uncommon in
		// JSON and trivially malformed; not worth burning cycles on).
		if len(b) > 16*1024 {
			return
		}

		scProj, scOut, scRemoved, scMal, scBad := extractAndStripProject(b)

		// Build the oracle from encoding/json. We only trust the oracle
		// when the input is a syntactically valid JSON object — that's the
		// domain the scanner is contracted for.
		if !json.Valid(b) {
			// Scanner may report malformed or pass through; either is fine
			// because the downstream json.Valid in validateEvent will drop
			// the event regardless. Just check the scanner did not panic
			// and did not invent a project name from garbage.
			if scProj != "" && !scMal {
				// Got a project name out of invalid JSON — must at least
				// have signaled removed=true (we touched bytes) OR
				// badProject=true. A non-empty project with
				// removed=false and malformed=false would be a contract
				// violation: dispatch would route somewhere real off
				// invalid input.
				if !scRemoved && !scBad {
					t.Fatalf("invented project %q from invalid JSON: %q", scProj, b)
				}
			}
			return
		}

		// Oracle: top-level walk via json.Decoder.
		dec := json.NewDecoder(bytes.NewReader(b))
		tok, err := dec.Token()
		if err != nil {
			return
		}
		d, isObj := tok.(json.Delim)
		if !isObj || d != '{' {
			// Not an object — scanner contract returns malformed=false (the
			// downstream validator drops it). Scanner must NOT have stripped
			// or claimed a project.
			if scRemoved || scProj != "" {
				t.Fatalf("non-object input %q: scanner returned removed=%v project=%q", b, scRemoved, scProj)
			}
			return
		}

		var oracleProj string
		var oracleBad bool
		var oracleHits int
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return
			}
			ks := k.(string)
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return
			}
			if ks == projectKey {
				oracleHits++
				oracleProj = ""
				oracleBad = true
				trim := bytes.TrimSpace(raw)
				if len(trim) > 0 && trim[0] == '"' {
					var pv string
					if json.Unmarshal(trim, &pv) == nil {
						oracleProj = pv
						oracleBad = false
					}
				}
			}
		}

		// Scanner may have flagged malformed for edge cases stdlib accepts
		// (e.g. unusual but legal whitespace inside numbers — there aren't
		// any, but harden against future divergence). If so, the validator
		// will drop the event. That's fine; just don't compare further.
		if scMal {
			return
		}

		if oracleHits > 0 != scRemoved {
			t.Fatalf("removed mismatch on %q: scanner=%v oracle=%v (hits=%d)", b, scRemoved, oracleHits > 0, oracleHits)
		}
		if scProj != oracleProj {
			t.Fatalf("project mismatch on %q: scanner=%q oracle=%q", b, scProj, oracleProj)
		}
		if scBad != oracleBad {
			t.Fatalf("badProject mismatch on %q: scanner=%v oracle=%v", b, scBad, oracleBad)
		}

		// If we claimed a strip, the output must still be valid JSON and
		// must not contain a top-level _project.
		if scRemoved {
			if !json.Valid(scOut) {
				t.Fatalf("strip produced invalid JSON on %q -> %q", b, scOut)
			}
			var m map[string]json.RawMessage
			if err := json.Unmarshal(scOut, &m); err == nil {
				if _, leak := m[projectKey]; leak {
					t.Fatalf("strip left top-level _project on %q -> %q", b, scOut)
				}
			}
		}
	})
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

func TestLoadKeysFile_RejectsWorldWritable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Chmod explicitly; os.WriteFile honors umask so 0o666 → 0o644 on most
	// developer machines and we'd miss the rejection branch.
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := loadKeysFile(path); err == nil {
		t.Fatal("expected error for world-writable keys file")
	}
}

func TestLoadKeysFile_AllowsSameDirSymlink(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.json")
	link := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(real, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	keys, err := loadKeysFile(link)
	if err != nil {
		t.Fatal(err)
	}
	if keys["ws-42"] != "m0_a" {
		t.Errorf("got %+v", keys)
	}
}

// TestLoadKeysFile_KubernetesSecretLayout mirrors how kubelet materializes a
// Secret volume: file → ..data/file, ..data → ..<timestamp> directory.
func TestLoadKeysFile_KubernetesSecretLayout(t *testing.T) {
	dir := t.TempDir()
	tsDir := filepath.Join(dir, "..2026_07_13_17_00_00.123")
	if err := os.Mkdir(tsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tsDir, "keys.json"), []byte(`{"ws-42":"m0_a"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..2026_07_13_17_00_00.123", filepath.Join(dir, "..data")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.Symlink(filepath.Join("..data", "keys.json"), filepath.Join(dir, "keys.json")); err != nil {
		t.Fatal(err)
	}
	keys, err := loadKeysFile(filepath.Join(dir, "keys.json"))
	if err != nil {
		t.Fatal(err)
	}
	if keys["ws-42"] != "m0_a" {
		t.Errorf("got %+v", keys)
	}
}

func TestLoadKeysFile_RejectsSymlinkEscape(t *testing.T) {
	outside := t.TempDir()
	dir := t.TempDir()
	real := filepath.Join(outside, "real.json")
	link := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(real, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if _, err := loadKeysFile(link); err == nil {
		t.Fatal("expected error for symlink escaping the keys directory")
	}
}

func TestInstall_RejectsTooManyProjects(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	keys := map[string]string{"a": "k1", "b": "k2", "c": "k3"}
	body, _ := json.Marshal(keys)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = ""
	cfg.KeysFile = path
	cfg.MaxProjects = 2
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), newSelfStats())
	if err := reg.install(); err == nil {
		t.Fatal("expected install to reject 3 projects with MaxProjects=2")
	}
}

func TestRegistry_RequireProjectDisablesDefaultFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = "m0_default" // would normally absorb unlabeled datagrams
	cfg.KeysFile = path
	cfg.RequireProject = true
	stats := newSelfStats()
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	// No _project on the wire — with RequireProject set this must NOT route
	// to the _default pipeline; it must drop as unrouted_missing.
	reg.dispatch(rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now()})
	if stats.DropsUnroutedMissing.Load() != 1 {
		t.Errorf("missing: got %d", stats.DropsUnroutedMissing.Load())
	}
}

func TestRegistry_DispatchBadProjectType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"ws-42":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.APIKey = "m0_default" // default exists; non-string _project must
	// still NOT fall through to it (silent cross-tenant attribution).
	cfg.KeysFile = path
	stats := newSelfStats()
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":42,"a":1}`), at: time.Now()})
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unknown: got %d want 1", stats.DropsUnroutedUnknown.Load())
	}
	// Default pipeline must NOT have received it.
	defPipe, _ := reg.lookup("")
	if defPipe != nil && defPipe.stats.EventsReceived.Load() != 0 {
		t.Errorf("non-string _project leaked into default pipeline: %d", defPipe.stats.EventsReceived.Load())
	}
}

func TestRegistry_DispatchClosedPipelineAccountsRoutingClosed(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "m0_default"
	stats := newSelfStats()
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}

	// Pre-drain the default pipeline, then dispatch. trySend must report
	// closed; dispatch must account it as routing_closed (not queue_full).
	p, _ := reg.lookup("")
	p.drain(0)

	_, queueFull := reg.dispatch(rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now()})
	if queueFull {
		t.Error("dispatch reported queueFull on a closed pipeline; should be routing_closed")
	}
	if stats.DropsRoutingClosed.Load() != 1 {
		t.Errorf("DropsRoutingClosed: got %d want 1", stats.DropsRoutingClosed.Load())
	}
	if stats.DropsQueueFull.Load() != 0 {
		t.Errorf("DropsQueueFull: got %d want 0 (closed pipeline must not bump queue_full)", stats.DropsQueueFull.Load())
	}
	if p.stats.DropsRoutingClosed.Load() != 1 {
		t.Errorf("per-pipeline routing_closed: got %d want 1", p.stats.DropsRoutingClosed.Load())
	}
}

// TestRegistry_PerPipelineHandoffFullBumpsOnlyPerProjectQueueFull pins the
// semantic the PR's CHANGELOG flags as breaking: when a single project's
// 16-slot handoff buffer overflows while the shared queue still has
// capacity, the per-project `queue_full` counter must bump and the
// process-wide `queue_full` counter must NOT. Operators rely on this
// distinction to tell "the whole agent is back-pressured" from "one
// project's batcher/flusher is wedged."
func TestRegistry_PerPipelineHandoffFullBumpsOnlyPerProjectQueueFull(t *testing.T) {
	// Wedge the gateway so the flusher never makes forward progress. With
	// MaxBatch=1 every dispatched event becomes its own batch, so after
	// (batchCh cap=8 + 1 in-flight POST) events the batcher blocks trying
	// to hand off; subsequent events fill the 16-slot rawCh; the next
	// demux trySend then returns full.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	// LIFO defer order: shutdown(0) cancels in-flight POSTs → close(release)
	// unblocks any handler still waiting → srv.Close() can then complete
	// without timing out on a stuck client connection.
	defer srv.Close()
	defer func() { close(release) }()

	cfg := testConfig()
	cfg.APIKey = "m0_default"
	cfg.GatewayURL = srv.URL
	cfg.MaxBatch = 1
	cfg.BatchWindow = 1 * time.Millisecond
	// Generous shared queue so it never fills during the test.
	cfg.QueueSize = 1024
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	p, _ := reg.lookup("")

	// Drip-feed datagrams one at a time with a small pause so the demuxer
	// always drains shared before the next dispatch lands. After the
	// pipeline wedges, additional dispatches will hit the per-pipeline
	// trySend default branch in the demuxer.
	//
	// Bound: batchCh(8) + flusher in-flight(1) + rawCh(16) = 25 events
	// before further sends start overflowing the handoff. Send 100 to
	// guarantee plenty of overflow signals without depending on tight
	// scheduler timing.
	body := []byte(`{"a":1}`)
	for i := 0; i < 100; i++ {
		ok, qfull := reg.dispatch(rawDatagram{bytes: body, at: time.Now()})
		if qfull {
			t.Fatalf("dispatch %d reported shared-queue full; QueueSize=%d is too small for this test", i, cfg.QueueSize)
		}
		_ = ok // dispatch returns false-with-qfull=false is impossible here (dg is routable)
		time.Sleep(200 * time.Microsecond)
	}

	// Poll for per-project queue_full > 0. The exact count depends on
	// scheduler interleavings between dispatch and demux, so we only
	// assert "at least one overflow was observed."
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.stats.DropsQueueFull.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := p.stats.DropsQueueFull.Load(); got == 0 {
		t.Fatal("per-project DropsQueueFull never bumped — handoff buffer overflow not exercised")
	}
	if got := stats.DropsQueueFull.Load(); got != 0 {
		t.Errorf("process-wide DropsQueueFull bumped (%d) but shared queue had capacity — semantic regression: per-pipeline overflow must not count toward process-wide queue_full", got)
	}
}

// TestRegistry_DemuxAccountsRoutingClosedForUnknownProject pins the
// dispatch→reload→demux race: a datagram that was successfully routed at
// dispatch time can find its destination pipeline gone by the time the
// demuxer pops it (a SIGHUP reload removed the project in between). The
// demuxer's lookup-miss branch must account this as routing_closed
// rather than silently swallowing it. The test models the race by
// injecting a datagram whose `project` field names a project not in the
// current routing table directly into the shared queue — bypassing
// dispatch (which would reject the unknown project as
// unrouted_unknown).
func TestRegistry_DemuxAccountsRoutingClosedForUnknownProject(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "m0_default"
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	// Inject a datagram pre-stamped with a project name that the current
	// routing table does not contain. This is exactly the state the
	// demuxer would observe if dispatch had stamped `project="ws-gone"`
	// against an older routing table snapshot and reload subsequently
	// retired the project.
	reg.sharedRawCh <- rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now(), project: "ws-gone"}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stats.DropsRoutingClosed.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := stats.DropsRoutingClosed.Load(); got != 1 {
		t.Fatalf("process-wide DropsRoutingClosed: got %d want 1 (demuxer missed the lookup-miss branch)", got)
	}
	// Default pipeline must not be charged — the datagram never named it.
	p, _ := reg.lookup("")
	if got := p.stats.DropsRoutingClosed.Load(); got != 0 {
		t.Errorf("default pipeline DropsRoutingClosed: got %d want 0 (lookup miss must not charge an unrelated pipeline)", got)
	}
	// And no other drop category should fire for a lookup miss.
	if got := stats.DropsQueueFull.Load(); got != 0 {
		t.Errorf("DropsQueueFull: got %d want 0", got)
	}
	if got := stats.DropsUnroutedUnknown.Load(); got != 0 {
		t.Errorf("DropsUnroutedUnknown: got %d want 0 (demuxer lookup miss is routing_closed, not unrouted)", got)
	}
}
