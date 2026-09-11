package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// inlineConfig is now just testConfig -- inline tokens are the only mode.
// Kept as a named seam so the inline-specific tests still read clearly.
func inlineConfig() Config {
	return testConfig()
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
	// The whole point: nothing declared anywhere, and a datagram that carries
	// its own credential.
	reg, stats := newInlineRegistry(t, inlineConfig())

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
	if reg.InlineProjectsRegistered.Load() != 1 {
		t.Errorf("registered counter: got %d", reg.InlineProjectsRegistered.Load())
	}
	waitFor(t, "datagram to reach the inline pipeline", func() bool {
		return p.stats.EventsReceived.Load() == 1
	})
}

// Keeping the credential out of stored telemetry is not optional, whatever
// else happens to the datagram.
func TestDispatch_TokenIsStrippedFromTheBody(t *testing.T) {
	f := extractAndStripRouting([]byte(`{"_project":"ws","_token":"secret-jwt","a":1}`))
	if string(f.stripped) != `{"a":1}` {
		t.Errorf("token not stripped: %s", f.stripped)
	}
	if f.token != "secret-jwt" {
		t.Errorf("token: got %q", f.token)
	}
}

func TestDispatch_BadTokenIsItsOwnDrop(t *testing.T) {
	reg, stats := newInlineRegistry(t, inlineConfig())

	reg.dispatch(rawDatagram{bytes: []byte(`{"_token":99,"a":1}`), at: time.Now()})
	if stats.DropsBadToken.Load() != 1 {
		t.Errorf("bad_token: got %d, want 1", stats.DropsBadToken.Load())
	}
	// Not conflated with a routing problem — the project was fine.
	if stats.DropsUnroutedUnknown.Load() != 0 {
		t.Errorf("unrouted_unknown: got %d, want 0", stats.DropsUnroutedUnknown.Load())
	}
}

func TestDispatch_MaxProjectsBoundsInlineRegistration(t *testing.T) {
	cfg := inlineConfig()
	cfg.MaxProjects = 1
	reg, stats := newInlineRegistry(t, cfg)

	// The table starts empty, so the first project fits.
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

// ---- credential ------------------------------------------------------------

func TestFlusher_AuthorizesTheBatchWithItsOwnToken(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, _ := newTestEventsFlusher(t, srv.URL, 0)
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

// Idle expiry is now the ONLY thing that removes a pipeline, so it is the
// only thing bounding the population.
func TestExpireInline_RetiresIdlePipelines(t *testing.T) {
	reg, _ := newInlineRegistry(t, inlineConfig())

	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ephemeral","_token":"t"}`), at: time.Now()})
	if _, found := reg.lookup("ephemeral"); !found {
		t.Fatal("project not registered")
	}

	// Everything is idle relative to a future clock.
	n := reg.expireInline(time.Minute, time.Now().Add(time.Hour))
	if n != 1 {
		t.Errorf("retired: got %d, want 1", n)
	}
	if _, found := reg.lookup("ephemeral"); found {
		t.Error("idle pipeline still routable")
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
// this exists for, wired end to end: an agent started with NO credentials at
// all, a datagram arriving over the real UDS socket naming a project nobody
// declared and carrying its own token, and a gateway that must see that
// token in the Authorization header.
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

	// shortTempSocketPath, not filepath.Join(t.TempDir(), …): sun_path caps
	// at 103 bytes and a temp-dir path blows past it on macOS.
	sock := shortTempSocketPath(t)

	cfg := inlineConfig() // nothing declared anywhere
	cfg.GatewayURL = srv.URL
	cfg.BatchWindow = 20 * time.Millisecond

	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatalf("install with nothing declared: %v", err)
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

func TestDispatch_PipelineRejectsUncredentialedDatagram(t *testing.T) {
	// An emitter that normally attaches `_token` still emits without one when
	// its own minting fails. The project stays registered from earlier
	// traffic, and no pipeline holds a credential of its own — so this must
	// drop attributably rather than POST an empty bearer.
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
		t.Error("delivered an uncredentialed datagram to a pipeline")
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

// ---- the incident this removal fixes ---------------------------------------

// REGRESSION. The keys-file model rebuilt the whole routing table on every
// poll (default 30s) from the file's contents, then drained every pipeline
// that was not in the rebuilt table. Inline `_token` pipelines were never in
// it -- they are not declared anywhere -- so an agent configured with
// MESH0_KEYS_FILE pointed at an empty `{}` tore down every live pipeline
// twice a minute and dropped its buffered batches as `shutdown`.
//
// On xano-deriv that was 11.8M dropped events against 28.4M received.
//
// The reload path is gone, so this pins the invariant that replaced it:
// registration is PURELY ADDITIVE. Nothing but idle expiry removes a
// pipeline, and registering a new project must never disturb an existing one.
func TestRegistry_RegistrationIsAdditiveAndNeverRetiresLivePipelines(t *testing.T) {
	reg, stats := newInlineRegistry(t, inlineConfig())

	first, _ := reg.dispatch(rawDatagram{
		bytes: []byte(`{"_project":"ws-1","_token":"t1","a":1}`), at: time.Now(),
	})
	if !first {
		t.Fatal("first registration refused")
	}
	p1, _ := reg.lookup("ws-1")
	waitFor(t, "ws-1 to receive its datagram", func() bool {
		return p1.stats.EventsReceived.Load() == 1
	})

	// Register many more projects. Under the old model any table rebuild
	// between these would have retired ws-1.
	for i := 2; i <= 25; i++ {
		n := fmt.Sprintf("ws-%d", i)
		if ok, _ := reg.dispatch(rawDatagram{
			bytes: []byte(`{"_project":"` + n + `","_token":"t","a":1}`), at: time.Now(),
		}); !ok {
			t.Fatalf("registration of %s refused", n)
		}
	}

	// ws-1 is still the SAME pipeline object, still routable, never drained.
	again, found := reg.lookup("ws-1")
	if !found {
		t.Fatal("ws-1 was retired by later registrations")
	}
	if again != p1 {
		t.Error("ws-1 was replaced by a new pipeline; registration must not rebuild the table")
	}
	again.sendMu.RLock()
	closed := again.closed
	again.sendMu.RUnlock()
	if closed {
		t.Error("ws-1 was drained while still live")
	}

	// And nothing was dropped as shutdown along the way -- the counter that
	// reached 11.8M in production.
	if got := stats.DropsShutdown.Load(); got != 0 {
		t.Errorf("shutdown drops during steady-state registration: got %d, want 0", got)
	}
	if got := len(reg.cur.Load().pipelines); got != 25 {
		t.Errorf("pipelines: got %d, want 25", got)
	}
}
