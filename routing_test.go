package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
// so dispatch counts it as unrouted_unknown instead of being treated as a
// project name.
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

func TestRegistry_DispatchMissingProjectWhenMultiTenant(t *testing.T) {
	cfg := testConfig()
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)
	registerProjects(t, reg, "ws-42")

	// No _project on the wire → missing_project drop. There is no default
	// pipeline to fall through to, credential or not.
	reg.dispatch(rawDatagram{bytes: []byte(`{"_token":"t","a":1}`), at: time.Now()})
	if stats.DropsUnroutedMissing.Load() != 1 {
		t.Errorf("missing: got %d", stats.DropsUnroutedMissing.Load())
	}
	// Unknown project with NO credential → unknown_project drop (a token is
	// what would have registered it).
	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-zzz"}`), at: time.Now()})
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unknown: got %d", stats.DropsUnroutedUnknown.Load())
	}
	// Known project → delivered.
	ok, _ := reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-42","_token":"t","a":1}`), at: time.Now()})
	if !ok {
		t.Error("expected dispatch to ws-42")
	}
}

func TestRegistry_DispatchMalformedBumpsParseError(t *testing.T) {
	cfg := testConfig()
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

func TestRegistry_MultiPipelineDrain(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 0
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	registerProjects(t, reg, "ws-42", "ws-99", "ws-7")

	for _, p := range []string{"ws-42", "ws-99", "ws-7"} {
		body := []byte(`{"_project":"` + p + `","_token":"t","a":1}`)
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

func TestListenerRoutesToCorrectPipeline(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 0 // don't burn time retrying against a bogus gateway

	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)
	registerProjects(t, reg, "ws-42", "ws-99")

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
		{"ws-42 first", `{"_project":"ws-42","_token":"t","operation":"a"}`},
		{"ws-42 second", `{"_project":"ws-42","_token":"t","operation":"b"}`},
		{"ws-99 once", `{"_project":"ws-99","_token":"t","operation":"c"}`},
		{"no _project drops", `{"_token":"t","operation":"d"}`},
		{"unknown project without a token drops", `{"_project":"ws-zzz","operation":"e"}`},
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

	// registerProjects already delivered one datagram to each.
	if !waitFor(p42.stats.EventsReceived.Load, 3) {
		t.Errorf("ws-42 EventsReceived: got %d want 3", p42.stats.EventsReceived.Load())
	}
	if !waitFor(p99.stats.EventsReceived.Load, 2) {
		t.Errorf("ws-99 EventsReceived: got %d want 2", p99.stats.EventsReceived.Load())
	}
	if !waitFor(stats.DropsUnroutedMissing.Load, 1) {
		t.Errorf("DropsUnroutedMissing: got %d want 1", stats.DropsUnroutedMissing.Load())
	}
	if !waitFor(stats.DropsUnroutedUnknown.Load, 1) {
		t.Errorf("DropsUnroutedUnknown: got %d want 1", stats.DropsUnroutedUnknown.Load())
	}
	// Sanity: cross-tenant leak would show up here. Counts include the one
	// datagram registerProjects sent to bring each project into the table.
	if got := p42.stats.EventsReceived.Load(); got != 3 {
		t.Errorf("ws-42 leak check: got %d want exactly 3", got)
	}
	if got := p99.stats.EventsReceived.Load(); got != 2 {
		t.Errorf("ws-99 leak check: got %d want exactly 2", got)
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

// waitForCount polls an atomic counter until it reaches want. The
// dispatch → shared queue → demuxer → batcher path is asynchronous, so a
// counter read immediately after dispatch is a race.
func waitForCount(t *testing.T, what string, get func() uint64, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if get() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (got %d, want %d)", what, get(), want)
}

// registerProjects brings projects into the routing table the only way there
// is: a datagram carrying `_project` plus its own `_token`.
func registerProjects(t *testing.T, reg *registry, names ...string) {
	t.Helper()
	for _, n := range names {
		body := []byte(`{"_project":"` + n + `","_token":"t-` + n + `"}`)
		if ok, _ := reg.dispatch(rawDatagram{bytes: body, at: time.Now()}); !ok {
			t.Fatalf("could not register %q", n)
		}
		if _, found := reg.lookup(n); !found {
			t.Fatalf("%q not routable after registration", n)
		}
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

// 0 means unlimited, and it is the default. A deployment with many live
// workspaces must register them all rather than start refusing partway
// through, which would be silent on the caller's side.
func TestRegistry_MaxProjectsZeroMeansUnlimited(t *testing.T) {
	cfg := testConfig()
	cfg.MaxProjects = 0
	cfg.QueueSize = 4096
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), newSelfStats())
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	for i := 0; i < 200; i++ {
		n := fmt.Sprintf("ws-%d", i)
		body := []byte(`{"_project":"` + n + `","_token":"t"}`)
		if ok, _ := reg.dispatch(rawDatagram{bytes: body, at: time.Now()}); !ok {
			t.Fatalf("registration of %s refused with MaxProjects=0", n)
		}
	}
	if got := len(reg.cur.Load().pipelines); got != 200 {
		t.Fatalf("registered pipelines: got %d, want 200", got)
	}
}

func TestRegistry_DispatchBadProjectType(t *testing.T) {
	cfg := testConfig()
	stats := newSelfStats()
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)
	registerProjects(t, reg, "ws-42")

	// The registration datagram reaches the pipeline asynchronously; let it
	// land so the leak assertion below has a stable baseline.
	p, _ := reg.lookup("ws-42")
	waitForCount(t, "registration datagram to land", p.stats.EventsReceived.Load, 1)

	reg.dispatch(rawDatagram{bytes: []byte(`{"_project":42,"_token":"t","a":1}`), at: time.Now()})
	if stats.DropsUnroutedUnknown.Load() != 1 {
		t.Errorf("unknown: got %d want 1", stats.DropsUnroutedUnknown.Load())
	}
	// It must not have leaked into the one real pipeline either.
	if got := p.stats.EventsReceived.Load(); got != 1 {
		t.Errorf("non-string _project leaked into ws-42: %d", got)
	}
}

func TestRegistry_DispatchClosedPipelineAccountsRoutingClosed(t *testing.T) {
	cfg := testConfig()
	stats := newSelfStats()
	reg := newRegistry(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	registerProjects(t, reg, "ws-42")

	// Let the registration datagram land BEFORE draining: a datagram still
	// in the shared queue when the pipeline closes is itself charged
	// routing_closed by the demuxer, which would double the count below.
	p, _ := reg.lookup("ws-42")
	waitForCount(t, "registration datagram to land", p.stats.EventsReceived.Load, 1)

	// Pre-drain the pipeline, then dispatch. trySend must report closed;
	// dispatch must account it as routing_closed (not queue_full).
	p.drain(0)

	_, queueFull := reg.dispatch(rawDatagram{bytes: []byte(`{"_project":"ws-42","_token":"t","a":1}`), at: time.Now()})
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
	registerProjects(t, reg, "ws-42")

	p, _ := reg.lookup("ws-42")

	// Drip-feed datagrams one at a time with a small pause so the demuxer
	// always drains shared before the next dispatch lands. After the
	// pipeline wedges, additional dispatches will hit the per-pipeline
	// trySend default branch in the demuxer.
	//
	// Bound: batchCh(8) + flusher in-flight(1) + rawCh(16) = 25 events
	// before further sends start overflowing the handoff. Send 100 to
	// guarantee plenty of overflow signals without depending on tight
	// scheduler timing.
	body := []byte(`{"_project":"ws-42","_token":"t","a":1}`)
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
// dispatch→expiry→demux race: a datagram that was successfully routed at
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
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)
	registerProjects(t, reg, "ws-42")

	// Inject a datagram pre-stamped with a project name that the current
	// routing table does not contain. This is exactly the state the
	// demuxer would observe if dispatch had stamped `project="ws-gone"`
	// against an older routing table snapshot and idle expiry subsequently
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
	// The live pipeline must not be charged — the datagram never named it.
	p, _ := reg.lookup("ws-42")
	if got := p.stats.DropsRoutingClosed.Load(); got != 0 {
		t.Errorf("ws-42 DropsRoutingClosed: got %d want 0 (lookup miss must not charge an unrelated pipeline)", got)
	}
	// And no other drop category should fire for a lookup miss.
	if got := stats.DropsQueueFull.Load(); got != 0 {
		t.Errorf("DropsQueueFull: got %d want 0", got)
	}
	if got := stats.DropsUnroutedUnknown.Load(); got != 0 {
		t.Errorf("DropsUnroutedUnknown: got %d want 0 (demuxer lookup miss is routing_closed, not unrouted)", got)
	}
}
