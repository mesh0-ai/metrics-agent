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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Multi-event datagrams ("m0batch/1"): one datagram carries one routing
// header (`_project`, `_token`) and N events. An emitter that sends many
// small events per request (Xano's exact flow evidence: ~200 for a request
// making 100 function calls) otherwise pays one datagram, one ~340-byte
// `_token` and one listener/demuxer/batcher hop per event. These pin the
// wire format, the accounting (every event counted once, every drop counted
// per event) and the capability file the agent advertises it with.

func batchDatagramBytes(header string, events ...string) []byte {
	return []byte("M0BATCH/1\n{" + header + `"events":[` + strings.Join(events, ",") + "]}")
}

// recordingGateway accepts every POST and keeps each event with the bearer
// it was sent under.
type recordedEvent struct {
	auth  string
	event map[string]any
}

type recordingGateway struct {
	mu     sync.Mutex
	events []recordedEvent
	srv    *httptest.Server
}

func newRecordingGateway(t *testing.T) *recordingGateway {
	t.Helper()
	g := &recordingGateway{}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("gateway decode: %v body=%s", err, body)
		}
		g.mu.Lock()
		for _, ev := range p.Events {
			g.events = append(g.events, recordedEvent{auth: r.Header.Get("Authorization"), event: ev})
		}
		g.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *recordingGateway) received() []recordedEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]recordedEvent(nil), g.events...)
}

func batchRegistry(t *testing.T, gatewayURL string, mutate func(*Config)) (*registry, *selfStats) {
	t.Helper()
	cfg := inlineConfig()
	cfg.GatewayURL = gatewayURL
	cfg.BatchWindow = 20 * time.Millisecond
	cfg.BatchDatagrams = true
	if mutate != nil {
		mutate(&cfg)
	}
	return newInlineRegistry(t, cfg)
}

// ---- format ---------------------------------------------------------------

func TestIsBatchDatagram(t *testing.T) {
	if !isBatchDatagram(batchDatagramBytes(``, `{"a":1}`)) {
		t.Error("a batch datagram was not recognized")
	}
	for _, b := range []string{`{"a":1}`, `{"M0BATCH/":1}`, ` M0BATCH/1\n{}`, ``} {
		if isBatchDatagram([]byte(b)) {
			t.Errorf("%q recognized as a batch", b)
		}
	}
}

func TestParseBatchDatagram_HeaderAndEvents(t *testing.T) {
	bd, res := parseBatchDatagram(batchDatagramBytes(`"_project":"workspace-7","_token":"tok-a",`, `{"a":1}`, `{"b":{"c":[1,2]}}`))
	if res != batchParsed {
		t.Fatalf("result: got %v, want batchParsed", res)
	}
	if bd.project != "workspace-7" || bd.token != "tok-a" || bd.badProject || bd.badToken {
		t.Errorf("header: %+v", bd)
	}
	if len(bd.events) != 2 || string(bd.events[0]) != `{"a":1}` || string(bd.events[1]) != `{"b":{"c":[1,2]}}` {
		t.Errorf("events: %q", bd.events)
	}
}

func TestParseBatchDatagram_HeaderFieldsAreOptional(t *testing.T) {
	bd, res := parseBatchDatagram(batchDatagramBytes(``, `{"a":1}`))
	if res != batchParsed || bd.project != "" || bd.token != "" || len(bd.events) != 1 {
		t.Fatalf("got %+v / %v", bd, res)
	}
}

func TestParseBatchDatagram_Refusals(t *testing.T) {
	many := make([]string, MaxEventsPerBatch+1)
	for i := range many {
		many[i] = `{"a":1}`
	}
	cases := map[string]struct {
		in   []byte
		want batchParse
	}{
		"unknown version":       {[]byte("M0BATCH/2\n{\"events\":[{\"a\":1}]}"), batchUnsupported},
		"no header line":        {[]byte(`M0BATCH/1{"events":[{"a":1}]}`), batchMalformed},
		"body is an array":      {[]byte("M0BATCH/1\n[{\"a\":1}]"), batchMalformed},
		"body is not json":      {[]byte("M0BATCH/1\n{\"events\":[{\"a\":1}"), batchMalformed},
		"no events key":         {[]byte("M0BATCH/1\n{\"_project\":\"p\"}"), batchMalformed},
		"events null":           {[]byte("M0BATCH/1\n{\"events\":null}"), batchMalformed},
		"events empty":          {[]byte("M0BATCH/1\n{\"events\":[]}"), batchMalformed},
		"events not an array":   {[]byte("M0BATCH/1\n{\"events\":{\"a\":1}}"), batchMalformed},
		"unknown header member": {[]byte("M0BATCH/1\n{\"_project\":\"p\",\"extra\":1,\"events\":[{\"a\":1}]}"), batchMalformed},
		"trailing data":         {[]byte("M0BATCH/1\n{\"events\":[{\"a\":1}]} x"), batchMalformed},
		"trailing brace":        {[]byte("M0BATCH/1\n{\"events\":[{\"a\":1}]}}"), batchMalformed},
		"too many events":       {batchDatagramBytes(``, many...), batchMalformed},
	}
	for name, c := range cases {
		if _, res := parseBatchDatagram(c.in); res != c.want {
			t.Errorf("%s: got %v, want %v", name, res, c.want)
		}
	}
}

// ---- dispatch -------------------------------------------------------------

func TestDispatchBatch_DeliversEveryEventUnderTheHeaderToken(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, nil)

	ok, qfull := reg.dispatch(rawDatagram{
		bytes: batchDatagramBytes(`"_project":"workspace-7","_token":"tok-a",`, `{"n":1}`, `{"n":2}`, `{"n":3}`),
		at:    time.Now(),
	})
	if !ok || qfull {
		t.Fatalf("dispatch: ok=%v qfull=%v", ok, qfull)
	}
	waitFor(t, "three events at the gateway", func() bool { return len(g.received()) == 3 })

	for i, rec := range g.received() {
		if rec.auth != "Bearer tok-a" {
			t.Errorf("event %d sent under %q, want the header token", i, rec.auth)
		}
		if _, has := rec.event["_token"]; has {
			t.Errorf("event %d carried _token into the gateway body", i)
		}
		if _, has := rec.event["_project"]; has {
			t.Errorf("event %d carried _project into the gateway body", i)
		}
		if _, has := rec.event["timestamp"]; !has {
			t.Errorf("event %d was not stamped with a timestamp", i)
		}
		if rec.event["n"] != float64(i+1) {
			t.Errorf("event %d: n=%v, want %d (order kept)", i, rec.event["n"], i+1)
		}
	}
	p, _ := reg.lookup("workspace-7")
	if got := p.stats.EventsReceived.Load(); got != 3 {
		t.Errorf("pipeline events_received: got %d, want 3", got)
	}
	// The listener counts the datagram as one received event; dispatch adds
	// the other n-1, so a batch of 3 is 3 received events end to end.
	if got := stats.EventsReceived.Load(); got != 2 {
		t.Errorf("process events_received added by dispatch: got %d, want 2", got)
	}
	if got := stats.BatchDatagramsReceived.Load(); got != 1 {
		t.Errorf("batch_datagrams_received: got %d, want 1", got)
	}
}

func TestDispatchBatch_MemberRoutingFieldsAreStrippedOrRefused(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, nil)

	reg.dispatch(rawDatagram{
		bytes: batchDatagramBytes(`"_project":"workspace-7","_token":"tok-a",`,
			`{"n":1,"_token":"member-token"}`,
			`{"n":2,"_project":"workspace-8"}`,
			`{"n":3,"_project":"workspace-7"}`,
			`{"n":4,"_project":5}`,
		),
		at: time.Now(),
	})
	waitFor(t, "two events at the gateway", func() bool { return len(g.received()) == 2 })
	time.Sleep(60 * time.Millisecond)

	got := g.received()
	if len(got) != 2 {
		t.Fatalf("delivered %d events, want 2", len(got))
	}
	for _, rec := range got {
		if rec.auth != "Bearer tok-a" {
			t.Errorf("sent under %q; a member's own _token must never authorize it", rec.auth)
		}
		if _, has := rec.event["_token"]; has {
			t.Error("a member _token reached the gateway body")
		}
		if _, has := rec.event["_project"]; has {
			t.Error("a member _project reached the gateway body")
		}
	}
	if got[0].event["n"] != float64(1) || got[1].event["n"] != float64(3) {
		t.Errorf("delivered %v and %v, want n=1 and n=3", got[0].event["n"], got[1].event["n"])
	}
	// A member naming another project, or a non-string project, is a
	// malformed member: dropped and counted, never re-routed.
	if got := stats.DropsParseError.Load(); got != 2 {
		t.Errorf("parse_error: got %d, want 2", got)
	}
}

func TestDispatchBatch_DisabledIsAParseError(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, func(c *Config) { c.BatchDatagrams = false })

	ok, qfull := reg.dispatch(rawDatagram{
		bytes: batchDatagramBytes(`"_project":"workspace-7","_token":"tok-a",`, `{"n":1}`, `{"n":2}`),
		at:    time.Now(),
	})
	if ok || qfull {
		t.Errorf("dispatch: ok=%v qfull=%v, want a drop", ok, qfull)
	}
	if got := stats.DropsParseError.Load(); got != 1 {
		t.Errorf("parse_error: got %d, want 1", got)
	}
	time.Sleep(60 * time.Millisecond)
	if n := len(g.received()); n != 0 {
		t.Errorf("a refused batch delivered %d events", n)
	}
}

func TestDispatchBatch_MalformedIsOneParseError(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, nil)

	reg.dispatch(rawDatagram{bytes: []byte("M0BATCH/9\n{\"events\":[{\"n\":1}]}"), at: time.Now()})
	reg.dispatch(rawDatagram{bytes: []byte("M0BATCH/1\n{\"events\":[]}"), at: time.Now()})
	if got := stats.DropsParseError.Load(); got != 2 {
		t.Errorf("parse_error: got %d, want 2", got)
	}
	if got := stats.EventsReceived.Load(); got != 0 {
		t.Errorf("events_received added for a refused batch: %d", got)
	}
}

func TestDispatchBatch_RoutingDropsCountEveryEvent(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, func(c *Config) { c.InlineTokens = false })

	reg.dispatch(rawDatagram{
		bytes: batchDatagramBytes(`"_project":"workspace-unknown",`, `{"n":1}`, `{"n":2}`, `{"n":3}`),
		at:    time.Now(),
	})
	if got := stats.DropsUnroutedUnknown.Load(); got != 3 {
		t.Errorf("unrouted_unknown_project: got %d, want 3 (one per event)", got)
	}
}

func TestDispatchBatch_BadHeaderTokenCountsEveryEvent(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, nil)

	reg.dispatch(rawDatagram{
		bytes: batchDatagramBytes(`"_project":"workspace-7","_token":7,`, `{"n":1}`, `{"n":2}`),
		at:    time.Now(),
	})
	if got := stats.DropsBadToken.Load(); got != 2 {
		t.Errorf("bad_token: got %d, want 2", got)
	}
}

func TestDispatchBatch_SharedQueueFullCountsEveryEvent(t *testing.T) {
	cfg := testConfig()
	cfg.APIKey = "m0_default"
	cfg.QueueSize = 1
	cfg.BatchDatagrams = true
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	// A routing table without install(): no demuxer, so the shared queue
	// stays exactly as full as this test makes it.
	p := newPipeline(DefaultProject, "m0_default", cfg, log, stats)
	reg.cur.Store(&routingTable{pipelines: map[string]*pipeline{DefaultProject: p}, hasDefault: true})
	reg.sharedRawCh <- rawDatagram{bytes: []byte(`{"filler":1}`), at: time.Now()}

	ok, qfull := reg.dispatch(rawDatagram{bytes: batchDatagramBytes(``, `{"n":1}`, `{"n":2}`, `{"n":3}`), at: time.Now()})
	if ok {
		t.Error("dispatch delivered into a full shared queue")
	}
	if qfull {
		t.Error("a batch reported queueFull to the listener; it must account its own events")
	}
	if got := stats.DropsQueueFull.Load(); got != 3 {
		t.Errorf("queue_full: got %d, want 3 (one per event)", got)
	}
}

func TestDemux_PipelineOverflowOfABatchCountsEveryEvent(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer func() { close(release) }()

	cfg := testConfig()
	cfg.APIKey = "m0_default"
	cfg.GatewayURL = srv.URL
	cfg.MaxBatch = 1
	cfg.BatchWindow = time.Millisecond
	cfg.QueueSize = 1024
	cfg.BatchDatagrams = true
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)
	p, _ := reg.lookup("")

	for i := 0; i < 60; i++ {
		reg.dispatch(rawDatagram{bytes: batchDatagramBytes(``, `{"n":1}`, `{"n":2}`, `{"n":3}`), at: time.Now()})
		time.Sleep(200 * time.Microsecond)
	}
	waitFor(t, "a per-project overflow", func() bool { return p.stats.DropsQueueFull.Load() > 0 })
	waitFor(t, "top-level pipeline_queue_full to match", func() bool {
		return stats.DropsPipelineQueueFull.Load() == p.stats.DropsQueueFull.Load()
	})
	if got := p.stats.DropsQueueFull.Load(); got%3 != 0 {
		t.Errorf("per-project queue_full = %d, not a multiple of the batch size 3", got)
	}
}

// ---- batcher --------------------------------------------------------------

func TestBatcher_SplitsABatchAcrossMaxBatch(t *testing.T) {
	in, out, _, _, done := newTestBatcher(t, 2, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()

	in <- rawDatagram{events: [][]byte{
		[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`), []byte(`{"n":4}`), []byte(`{"n":5}`),
	}, at: time.Now(), token: "tok"}

	var sizes []int
	deadline := time.After(2 * time.Second)
	for total := 0; total < 5; {
		select {
		case b := <-out:
			sizes = append(sizes, len(b.Events))
			total += len(b.Events)
			if b.Token != "tok" {
				t.Errorf("batch token %q, want the datagram's", b.Token)
			}
		case <-deadline:
			t.Fatalf("timed out; batches so far %v", sizes)
		}
	}
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Errorf("batch sizes %v, want [2 2 1]", sizes)
	}
}

func TestBatcher_BatchEventsAreValidatedOneByOne(t *testing.T) {
	in, out, b, stats, done := newTestBatcherWithCap(t, 500, 1024, 30*time.Millisecond)
	b.pipelineStats = newPipelineStats()
	defer func() {
		close(in)
		<-done
	}()

	big := `{"pad":"` + strings.Repeat("x", 1100) + `"}`
	in <- rawDatagram{events: [][]byte{
		[]byte(`{"n":1}`), []byte(big), []byte(`null`), []byte(`{"n":2,"timestamp":"keep"}`),
	}, at: time.Now()}

	select {
	case batch := <-out:
		if len(batch.Events) != 2 {
			t.Fatalf("got %d events, want 2", len(batch.Events))
		}
		var first, second map[string]any
		_ = json.Unmarshal(batch.Events[0], &first)
		_ = json.Unmarshal(batch.Events[1], &second)
		if _, has := first["timestamp"]; !has {
			t.Error("a batch event without a timestamp was not stamped")
		}
		if second["timestamp"] != "keep" {
			t.Errorf("an existing timestamp was replaced: %v", second["timestamp"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no batch")
	}
	if stats.DropsOversize.Load() != 1 || stats.DropsParseError.Load() != 1 {
		t.Errorf("oversize=%d parse_error=%d, want 1 and 1", stats.DropsOversize.Load(), stats.DropsParseError.Load())
	}
	if got := b.pipelineStats.EventsReceived.Load(); got != 2 {
		t.Errorf("pipeline events_received: got %d, want 2", got)
	}
}

// ---- advertisement --------------------------------------------------------

func TestListen_AdvertisesBatchSupport(t *testing.T) {
	sockPath := shortTempSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- listenWithOptions(ctx, sockPath, DefaultMaxEventBytes+1, chanSink(make(chan rawDatagram, 4)), log, stats, listenOptions{advertiseBatch: true})
	}()
	if err := waitForSocket(sockPath, time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the capability file", func() bool { _, err := os.Stat(capsPath(sockPath)); return err == nil })

	raw, err := os.ReadFile(capsPath(sockPath))
	if err != nil {
		t.Fatal(err)
	}
	var caps agentCaps
	if err := json.Unmarshal(raw, &caps); err != nil {
		t.Fatalf("caps: %v (%s)", err, raw)
	}
	if len(caps.Formats) != 1 || caps.Formats[0] != batchFormatV1 {
		t.Errorf("formats: %v", caps.Formats)
	}
	if caps.MaxDatagramBytes != DefaultMaxEventBytes {
		t.Errorf("max_datagram_bytes: got %d, want %d", caps.MaxDatagramBytes, DefaultMaxEventBytes)
	}
	fi, err := os.Stat(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	if caps.Socket.Ino != uint64(st.Ino) || caps.Socket.Dev != uint64(st.Dev) {
		t.Errorf("caps socket %+v does not name the bound socket (dev %d ino %d)", caps.Socket, st.Dev, st.Ino)
	}
	if info, err := os.Stat(capsPath(sockPath)); err == nil && info.Mode().Perm()&0o044 != 0o044 {
		t.Errorf("caps file mode %o is not readable by an emitter running as another uid", info.Mode().Perm())
	}

	cancel()
	<-done
	if _, err := os.Stat(capsPath(sockPath)); !os.IsNotExist(err) {
		t.Errorf("caps file left behind after shutdown: %v", err)
	}
}

func TestListen_RemovesAStaleAdvertisementWhenNotAdvertising(t *testing.T) {
	sockPath := shortTempSocketPath(t)
	if err := os.WriteFile(capsPath(sockPath), []byte(`{"formats":["m0batch/1"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(capsPath(sockPath)) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- listenWithOptions(ctx, sockPath, DefaultMaxEventBytes+1, chanSink(make(chan rawDatagram, 4)), log, stats, listenOptions{})
	}()
	if err := waitForSocket(sockPath, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(capsPath(sockPath)); !os.IsNotExist(err) {
		t.Errorf("a stale capability file survived an agent that does not advertise: %v", err)
	}
	cancel()
	<-done
}

func TestListen_BatchOverTheWire(t *testing.T) {
	g := newRecordingGateway(t)
	reg, stats := batchRegistry(t, g.srv.URL, nil)

	sockPath := shortTempSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- listenWithOptions(ctx, sockPath, DefaultMaxEventBytes+1, reg, log, stats, listenOptions{advertiseBatch: true})
	}()
	if err := waitForSocket(sockPath, time.Second); err != nil {
		t.Fatal(err)
	}
	addr, err := net.ResolveUnixAddr("unixgram", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := net.DialUnix("unixgram", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write(batchDatagramBytes(`"_project":"workspace-7","_token":"tok-a",`, `{"n":1}`, `{"n":2}`, `{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	_ = cli.Close()

	waitFor(t, "three events at the gateway", func() bool { return len(g.received()) == 3 })
	if got := stats.EventsReceived.Load(); got != 3 {
		t.Errorf("events_received: got %d, want 3 (one per event, not per datagram)", got)
	}
	cancel()
	<-done
}

// ---- config ---------------------------------------------------------------

func TestLoadConfigBatchDatagrams(t *testing.T) {
	clearMesh0Env(t)
	t.Setenv("MESH0_API_KEY", "secret")
	t.Setenv("MESH0_BATCH_DATAGRAMS", "")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.BatchDatagrams {
		t.Error("MESH0_BATCH_DATAGRAMS default: got off, want on")
	}
	for v, want := range map[string]bool{"0": false, "false": false, "1": true, "true": true} {
		t.Setenv("MESH0_BATCH_DATAGRAMS", v)
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("%q: %v", v, err)
		}
		if cfg.BatchDatagrams != want {
			t.Errorf("%q: got %v, want %v", v, cfg.BatchDatagrams, want)
		}
	}
	t.Setenv("MESH0_BATCH_DATAGRAMS", "maybe")
	if _, err := loadConfig(); err == nil {
		t.Error("an invalid MESH0_BATCH_DATAGRAMS was accepted")
	}
}
