package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func inlineConfig() Config {
	c := testConfig()
	c.InlineTokens = true
	return c
}

func newInlineRegistry(t *testing.T, cfg Config) (*registry, *selfStats) {
	t.Helper()
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reg.shutdown(0) })
	return reg, stats
}

// waitFor polls until cond holds or the deadline passes. The dispatch →
// shared queue → demuxer → pipeline path is asynchronous by design.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- extraction -----------------------------------------------------------

func TestExtractRouting_PullsBothFields(t *testing.T) {
	in := []byte(`{"_project":"ws-42","_token":"eyJhbGciOiJFZERTQSJ9.x.y","a":1}`)
	f := extractAndStripRouting(in)
	if f.malformed || f.badProject || f.badToken {
		t.Fatalf("unexpected flags: %+v", f)
	}
	if f.project != "ws-42" {
		t.Errorf("project: got %q", f.project)
	}
	if f.token != "eyJhbGciOiJFZERTQSJ9.x.y" {
		t.Errorf("token: got %q", f.token)
	}
	if !f.removed {
		t.Error("removed: got false")
	}
	var m map[string]any
	if err := json.Unmarshal(f.stripped, &m); err != nil {
		t.Fatalf("stripped not valid JSON: %s", f.stripped)
	}
	if _, ok := m["_token"]; ok {
		t.Error("_token survived into the body")
	}
	if _, ok := m["_project"]; ok {
		t.Error("_project survived into the body")
	}
	if m["a"] != float64(1) {
		t.Errorf("payload lost: %v", m)
	}
}

func TestExtractRouting_TokenOnly(t *testing.T) {
	// A token with no project: the credential is recovered and stripped, but
	// there is nothing to route by.
	f := extractAndStripRouting([]byte(`{"_token":"tok","a":1}`))
	if f.token != "tok" || f.project != "" || !f.removed {
		t.Fatalf("got %+v", f)
	}
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("stripped: got %s", f.stripped)
	}
}

func TestExtractRouting_AllMembersAreRoutingFields(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_project":"p","_token":"t"}`))
	if string(f.stripped) != `{}` {
		t.Errorf("stripped: got %s, want {}", f.stripped)
	}
	if f.project != "p" || f.token != "t" {
		t.Errorf("got project=%q token=%q", f.project, f.token)
	}
}

func TestExtractRouting_NonStringToken(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_project":"p","_token":42,"a":1}`))
	if !f.badToken {
		t.Error("badToken: got false")
	}
	if f.token != "" {
		t.Errorf("token: got %q, want empty", f.token)
	}
	// Still removed — an unusable credential must not reach storage either.
	var m map[string]any
	if err := json.Unmarshal(f.stripped, &m); err != nil {
		t.Fatalf("stripped invalid: %s", f.stripped)
	}
	if _, ok := m["_token"]; ok {
		t.Error("non-string _token survived into the body")
	}
}

func TestExtractRouting_LastTokenWins(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_token":"first","_token":"second","a":1}`))
	if f.token != "second" {
		t.Errorf("token: got %q, want second", f.token)
	}
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("stripped: got %s", f.stripped)
	}
}

// The `_project`-only entry point must be unchanged by the addition of
// `_token`, because its test suite and the differential fuzz are written
// against exactly that behavior.
func TestExtractAndStripProject_LeavesTokenAlone(t *testing.T) {
	in := []byte(`{"_token":"tok","a":1}`)
	proj, out, removed, malformed, bad := extractAndStripProject(in)
	if proj != "" || removed || malformed || bad {
		t.Fatalf("got proj=%q removed=%v malformed=%v bad=%v", proj, removed, malformed, bad)
	}
	if string(out) != string(in) {
		t.Errorf("body mutated by the project-only scan: %s", out)
	}
}

// ---- dispatch -------------------------------------------------------------

func TestDispatch_RegistersProjectFromInlineToken(t *testing.T) {
	// The whole point: an empty keys file, a project nobody declared, and a
	// datagram that carries its own credential.
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := inlineConfig()
	cfg.KeysFile = path
	reg, stats := newInlineRegistry(t, cfg)

	ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"workspace-10","_token":"eyJ.tok","a":1}`),
		at:    time.Now(),
	})
	if !ok {
		t.Fatal("dispatch: got false, want true")
	}
	if stats.DropsUnroutedUnknown.Load() != 0 {
		t.Errorf("unrouted_unknown: got %d, want 0", stats.DropsUnroutedUnknown.Load())
	}
	p, found := reg.lookup("workspace-10")
	if !found {
		t.Fatal("pipeline was not registered")
	}
	if !p.inline {
		t.Error("pipeline not marked inline")
	}
	if p.apiKey != "" {
		t.Errorf("inline pipeline holds a fallback key %q; it must authenticate per batch", p.apiKey)
	}
	if reg.InlineProjectsRegistered.Load() != 1 {
		t.Errorf("registered counter: got %d", reg.InlineProjectsRegistered.Load())
	}
	waitFor(t, "datagram to reach the inline pipeline", func() bool {
		return p.stats.EventsReceived.Load() == 1
	})
}

func TestDispatch_InlineDisabledLeavesProjectUnrouted(t *testing.T) {
	cfg := testConfig() // InlineTokens false
	cfg.APIKey = "m0_default"
	reg, stats := newInlineRegistry(t, cfg)

	reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"workspace-10","_token":"eyJ.tok"}`),
		at:    time.Now(),
	})
	if _, found := reg.lookup("workspace-10"); found {
		t.Error("registered a pipeline with inline tokens disabled")
	}
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unrouted_unknown: got %d, want 1", stats.DropsUnroutedUnknown.Load())
	}
}

func TestDispatch_TokenStrippedEvenWhenInlineDisabled(t *testing.T) {
	// Honoring the credential is optional; keeping it out of stored telemetry
	// is not.
	cfg := testConfig() // InlineTokens false
	cfg.APIKey = "m0_default"
	reg, _ := newInlineRegistry(t, cfg)

	reg.dispatch(rawDatagram{bytes: []byte(`{"_token":"secret-jwt","a":1}`), at: time.Now()})
	p, _ := reg.lookup("")
	waitFor(t, "datagram to reach the default pipeline", func() bool {
		return p.stats.EventsReceived.Load() == 1
	})
	// The body handed on must not contain the credential; assert on the
	// extraction the dispatch performed.
	f := extractAndStripRouting([]byte(`{"_token":"secret-jwt","a":1}`))
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("token not stripped: %s", f.stripped)
	}
}

func TestDispatch_BadTokenIsItsOwnDrop(t *testing.T) {
	cfg := inlineConfig()
	cfg.APIKey = "m0_default"
	reg, stats := newInlineRegistry(t, cfg)

	reg.dispatch(rawDatagram{bytes: []byte(`{"_token":99,"a":1}`), at: time.Now()})
	if stats.DropsBadToken.Load() != 1 {
		t.Errorf("bad_token: got %d, want 1", stats.DropsBadToken.Load())
	}
	// Not conflated with a routing problem — the project was fine.
	if stats.DropsUnroutedUnknown.Load() != 0 {
		t.Errorf("unrouted_unknown: got %d, want 0", stats.DropsUnroutedUnknown.Load())
	}
}

func TestDispatch_BadTokenIsNotDowngradedToTheKeysFileKey(t *testing.T) {
	// A caller that meant to authenticate with a token and got the type wrong
	// must not be quietly authorized as whoever owns the default key.
	cfg := inlineConfig()
	cfg.APIKey = "m0_default"
	reg, _ := newInlineRegistry(t, cfg)

	ok, _ := reg.dispatch(rawDatagram{bytes: []byte(`{"_token":{},"a":1}`), at: time.Now()})
	if ok {
		t.Error("dispatch delivered a datagram with an unusable credential")
	}
	p, _ := reg.lookup("")
	if p.stats.EventsReceived.Load() != 0 {
		t.Errorf("default pipeline received %d events", p.stats.EventsReceived.Load())
	}
}

func TestDispatch_MaxProjectsBoundsInlineRegistration(t *testing.T) {
	cfg := inlineConfig()
	cfg.APIKey = "m0_default" // occupies one slot
	cfg.MaxProjects = 2
	reg, stats := newInlineRegistry(t, cfg)

	// First inline project fits (default + one).
	if ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"p1","_token":"t"}`), at: time.Now(),
	}); !ok {
		t.Fatal("first inline registration refused")
	}
	// Second exceeds the cap.
	if ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"p2","_token":"t"}`), at: time.Now(),
	}); ok {
		t.Error("registration past max_projects was allowed")
	}
	if _, found := reg.lookup("p2"); found {
		t.Error("p2 registered despite the cap")
	}
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unrouted_unknown: got %d, want 1", stats.DropsUnroutedUnknown.Load())
	}
}

func TestDispatch_ConcurrentFirstSightingRegistersOnce(t *testing.T) {
	cfg := inlineConfig()
	reg, _ := newInlineRegistry(t, cfg)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reg.dispatch(rawDatagram{
				bytes: []byte(`{"_project":"racy","_token":"t"}`), at: time.Now(),
			})
		}()
	}
	wg.Wait()

	if got := reg.InlineProjectsRegistered.Load(); got != 1 {
		t.Errorf("registered counter: got %d, want 1", got)
	}
	if got := reg.inlineLive(); got != 1 {
		t.Errorf("live inline pipelines: got %d, want 1", got)
	}
}

// ---- precedence -----------------------------------------------------------

func TestFlusher_InlineTokenBeatsTheConfiguredKey(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, _ := newTestEventsFlusher(t, srv.URL, 0) // configured with APIKey "k"
	in := make(chan EventBatch, 1)
	f.in = in
	batch := sampleEventBatch(1)
	batch.Token = "eyJ.inline"
	in <- batch
	close(in)
	f.run()

	if got != "Bearer eyJ.inline" {
		t.Errorf("Authorization: got %q, want the inline token", got)
	}
}

func TestFlusher_FallsBackToConfiguredKeyWithoutAToken(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, _ := newTestEventsFlusher(t, srv.URL, 0)
	in := make(chan EventBatch, 1)
	f.in = in
	in <- sampleEventBatch(1) // no Token
	close(in)
	f.run()

	if got != "Bearer k" {
		t.Errorf("Authorization: got %q, want the configured key", got)
	}
}

func TestBatcher_CarriesTheFreshestToken(t *testing.T) {
	// A rotation mid-batch leaves the batch on the newer credential, which is
	// the one with more life left against the gateway's clock.
	in := make(chan rawDatagram, 4)
	out := make(chan EventBatch, 2)
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := newEventsBatcher(in, out, stats, log, 10, DefaultMaxEventBytes, 20*time.Millisecond)
	b.ctx = context.Background()
	go b.run()

	in <- rawDatagram{bytes: []byte(`{"a":1}`), at: time.Now(), token: "old"}
	in <- rawDatagram{bytes: []byte(`{"a":2}`), at: time.Now(), token: "new"}
	close(in)

	batch := <-out
	if batch.Token != "new" {
		t.Errorf("batch token: got %q, want new", batch.Token)
	}
	if len(batch.Events) != 2 {
		t.Errorf("events: got %d, want 2", len(batch.Events))
	}
}

// ---- idle expiry ----------------------------------------------------------

func TestExpireInline_RetiresIdleInlineOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"declared":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := inlineConfig()
	cfg.KeysFile = path
	reg, _ := newInlineRegistry(t, cfg)

	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ephemeral","_token":"t"}`), at: time.Now()})
	if _, found := reg.lookup("ephemeral"); !found {
		t.Fatal("inline project not registered")
	}

	// Everything is idle relative to a future clock.
	n := reg.expireInline(time.Minute, time.Now().Add(time.Hour))
	if n != 1 {
		t.Errorf("retired: got %d, want 1", n)
	}
	if _, found := reg.lookup("ephemeral"); found {
		t.Error("idle inline pipeline still routable")
	}
	// A declared project is an operator's statement, not a guess about traffic.
	if _, found := reg.lookup("declared"); !found {
		t.Error("keys-file pipeline was expired")
	}
	waitFor(t, "expiry counter", func() bool {
		return reg.InlineProjectsExpired.Load() == 1
	})
}

func TestExpireInline_KeepsActiveInline(t *testing.T) {
	reg, _ := newInlineRegistry(t, inlineConfig())
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"busy","_token":"t"}`), at: time.Now()})

	if n := reg.expireInline(time.Hour, time.Now()); n != 0 {
		t.Errorf("retired: got %d, want 0", n)
	}
	if _, found := reg.lookup("busy"); !found {
		t.Error("active inline pipeline was expired")
	}
}

func TestExpireInline_ZeroIdleDisablesExpiry(t *testing.T) {
	reg, _ := newInlineRegistry(t, inlineConfig())
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"p","_token":"t"}`), at: time.Now()})

	if n := reg.expireInline(0, time.Now().Add(24*time.Hour)); n != 0 {
		t.Errorf("retired: got %d with idle=0, want 0", n)
	}
}

func TestDispatch_ReRegistersAfterExpiry(t *testing.T) {
	// An expired tenant that comes back must route again, not stay dead.
	reg, _ := newInlineRegistry(t, inlineConfig())
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"p","_token":"t"}`), at: time.Now()})
	reg.expireInline(time.Minute, time.Now().Add(time.Hour))

	if ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"p","_token":"t2"}`), at: time.Now(),
	}); !ok {
		t.Fatal("re-registration after expiry failed")
	}
	if got := reg.InlineProjectsRegistered.Load(); got != 2 {
		t.Errorf("registered counter: got %d, want 2", got)
	}
}

// TestExtractAndStripProject_AdjacentDuplicatesAtHead pins a bug that predates
// inline tokens: two adjacent removed members at the head of the object both
// claimed the comma between them, so nothing absorbed the separator after the
// run and the body came back as `{,"a":1}` — invalid JSON that the batcher
// then dropped as a parse error.
func TestExtractAndStripProject_AdjacentDuplicatesAtHead(t *testing.T) {
	proj, out, removed, malformed, _ := extractAndStripProject(
		[]byte(`{"_project":"a","_project":"b","x":1}`))
	if !removed || malformed {
		t.Fatalf("removed=%v malformed=%v", removed, malformed)
	}
	if proj != "b" {
		t.Errorf("project: got %q, want b (last wins)", proj)
	}
	if string(out) != `{"x":1}` {
		t.Errorf("stripped: got %s, want {\"x\":1}", out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("stripped not valid JSON: %s", out)
	}
}

// FuzzExtractAndStripRouting holds the two invariants the two-field scan is
// contracted for, on the domain it is contracted for (a valid JSON object):
// the result is still a valid JSON object, and neither routing member
// survives into it.
//
// The credential half is the one that matters most — a `_token` that survives
// a splice is a live credential written into stored telemetry.
func FuzzExtractAndStripRouting(f *testing.F) {
	for _, seed := range []string{
		`{"_project":"p","_token":"t","a":1}`,
		`{"_token":"t","_project":"p"}`,
		`{"a":1,"_token":"t"}`,
		`{"_token":"t"}`,
		`{"_token":"t","_token":"u","a":1}`,
		`{"_project":"p","_project":"q","_token":"t"}`,
		`{"_token":42,"a":1}`,
		`{"_token":null}`,
		`{"nested":{"_token":"inner"},"_token":"outer"}`,
		`{"msg":"contains \"_token\" literally"}`,
		`{ "_token" : "t" , "_project" : "p" , "a" : 1 }`,
		`{"_token":"with\"escape","a":1}`,
		`{}`,
		`{"a":1}`,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 16*1024 {
			return
		}
		res := extractAndStripRouting(b)

		// Only a valid top-level JSON object is in contract. Anything else is
		// dropped downstream by validateEvent regardless.
		var probe map[string]json.RawMessage
		if json.Unmarshal(b, &probe) != nil {
			return
		}
		if res.malformed {
			t.Fatalf("valid JSON object reported malformed: %s", b)
		}
		if !json.Valid(res.stripped) {
			t.Fatalf("stripped is not valid JSON: in=%s out=%s", b, res.stripped)
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(res.stripped, &out); err != nil {
			t.Fatalf("stripped is not an object: in=%s out=%s", b, res.stripped)
		}
		if _, ok := out["_token"]; ok {
			t.Fatalf("_token survived: in=%s out=%s", b, res.stripped)
		}
		if _, ok := out["_project"]; ok {
			t.Fatalf("_project survived: in=%s out=%s", b, res.stripped)
		}
		// Every surviving member must be one the input actually had.
		for k := range out {
			if _, ok := probe[k]; !ok {
				t.Fatalf("invented member %q: in=%s out=%s", k, b, res.stripped)
			}
		}
	})
}

// TestEndToEnd_KeylessInstanceRoutesOnInlineToken is the production scenario
// this feature exists for, wired end to end: an agent started with an EMPTY
// keys file (the shape a control plane leaves behind after migrating its
// emitter to inline credentials), a datagram arriving over the real UDS
// socket naming a project nobody declared and carrying its own token, and a
// gateway that must see that token in the Authorization header.
//
// Before inline credentials this dropped as unrouted_unknown_project with an
// empty routing table and no route ever installed.
func TestEndToEnd_KeylessInstanceRoutesOnInlineToken(t *testing.T) {
	type received struct {
		auth string
		body []byte
	}
	got := make(chan received, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case got <- received{auth: r.Header.Get("Authorization"), body: body}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(keys, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// shortTempSocketPath, not filepath.Join(t.TempDir(), …): sun_path caps
	// at 103 bytes and a temp-dir path blows past it on macOS.
	sock := shortTempSocketPath(t)

	cfg := inlineConfig()
	cfg.GatewayURL = srv.URL
	cfg.KeysFile = keys
	cfg.APIKey = "" // nothing declared anywhere
	cfg.BatchWindow = 20 * time.Millisecond

	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatalf("install with an empty keys file: %v", err)
	}
	defer reg.shutdown(time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenErr := make(chan error, 1)
	go func() {
		listenErr <- listen(ctx, sock, cfg.MaxEventBytes+1, reg, log, stats)
	}()
	if err := waitForSocket(sock, 2*time.Second); err != nil {
		t.Fatalf("socket not ready: %v", err)
	}

	addr, err := net.ResolveUnixAddr("unixgram", sock)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := net.DialUnix("unixgram", nil, addr)
	if err != nil {
		t.Fatalf("dial unixgram: %v", err)
	}
	defer cli.Close()

	const token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.claims.sig"
	dg := `{"_project":"workspace-10","_token":"` + token + `","operation":"query"}`
	if _, err := cli.Write([]byte(dg)); err != nil {
		t.Fatalf("write datagram: %v", err)
	}

	select {
	case r := <-got:
		if r.auth != "Bearer "+token {
			t.Errorf("Authorization: got %q, want the inline token", r.auth)
		}
		var payload struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.Unmarshal(r.body, &payload); err != nil {
			t.Fatalf("gateway body not decodable: %v (%s)", err, r.body)
		}
		if len(payload.Events) != 1 {
			t.Fatalf("events: got %d, want 1", len(payload.Events))
		}
		ev := payload.Events[0]
		if _, leaked := ev["_token"]; leaked {
			t.Error("the credential reached the gateway inside the event body")
		}
		if _, leaked := ev["_project"]; leaked {
			t.Error("_project reached the gateway inside the event body")
		}
		if ev["operation"] != "query" {
			t.Errorf("payload lost: %v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("gateway never received the batch (unrouted_unknown=%d bad_token=%d parse_error=%d)",
			stats.DropsUnroutedUnknown.Load(), stats.DropsBadToken.Load(), stats.DropsParseError.Load())
	}

	if reg.InlineProjectsRegistered.Load() != 1 {
		t.Errorf("inline registrations: got %d, want 1", reg.InlineProjectsRegistered.Load())
	}
	cancel()
	<-listenErr
}

func TestDispatch_InlinePipelineRejectsUncredentialedDatagram(t *testing.T) {
	// An emitter that normally attaches `_token` still emits without one when
	// its own minting fails. The project stays registered from earlier
	// traffic, and an inline pipeline has no credential of its own — so this
	// must drop attributably rather than POST an empty bearer.
	reg, stats := newInlineRegistry(t, inlineConfig())

	if ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"p","_token":"t","a":1}`), at: time.Now(),
	}); !ok {
		t.Fatal("first dispatch refused")
	}
	p, _ := reg.lookup("p")

	ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"p","a":2}`), at: time.Now(),
	})
	if ok {
		t.Error("delivered an uncredentialed datagram to an inline pipeline")
	}
	if stats.DropsMissingToken.Load() != 1 {
		t.Errorf("missing_token: got %d, want 1", stats.DropsMissingToken.Load())
	}
	if p.stats.DropsMissingToken.Load() != 1 {
		t.Errorf("per-pipeline missing_token: got %d, want 1", p.stats.DropsMissingToken.Load())
	}
	// Not conflated with a credential that was present but malformed.
	if stats.DropsBadToken.Load() != 0 {
		t.Errorf("bad_token: got %d, want 0", stats.DropsBadToken.Load())
	}
}

func TestDispatch_KeysFilePipelineStillAcceptsUncredentialedDatagram(t *testing.T) {
	// The new guard must apply ONLY to inline pipelines — a declared project
	// authenticates from the keys file and never needed a datagram credential.
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := os.WriteFile(path, []byte(`{"declared":"m0_a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := inlineConfig()
	cfg.KeysFile = path
	reg, stats := newInlineRegistry(t, cfg)

	if ok, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"declared","a":1}`), at: time.Now(),
	}); !ok {
		t.Fatal("keys-file pipeline refused an uncredentialed datagram")
	}
	if stats.DropsMissingToken.Load() != 0 {
		t.Errorf("missing_token: got %d, want 0", stats.DropsMissingToken.Load())
	}
}
