package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A per-project handoff overflow used to be counted ONLY under by_project,
// so the top-level events_dropped read zero while one project lost events
// (production-bugs #25: 820 by_project queue_full against 0 top level). It is
// now also reported top level, as pipeline_queue_full, while the top-level
// queue_full keeps meaning the SHARED queue (TestRegistry_PerPipelineHandoff
// FullBumpsOnlyPerProjectQueueFull still pins that).
func TestRegistry_PerPipelineOverflowReachesTheTopLevelCounters(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer func() { close(release) }()

	cfg := testConfig()
	cfg.APIKey = "m0_default"
	cfg.GatewayURL = srv.URL
	cfg.MaxBatch = 1
	cfg.BatchWindow = 1 * time.Millisecond
	cfg.QueueSize = 1024
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := newRegistry(cfg, log, stats)
	if err := reg.install(); err != nil {
		t.Fatal(err)
	}
	defer reg.shutdown(0)

	p, _ := reg.lookup("")

	body := []byte(`{"a":1}`)
	for i := 0; i < 100; i++ {
		if _, qfull := reg.dispatch(rawDatagram{bytes: body, at: time.Now()}); qfull {
			t.Fatalf("dispatch %d reported the shared queue full; QueueSize is too small for this test", i)
		}
		time.Sleep(200 * time.Microsecond)
	}

	waitFor(t, "a per-project overflow", func() bool { return p.stats.DropsQueueFull.Load() > 0 })
	waitFor(t, "the top-level pipeline_queue_full to match by_project", func() bool {
		return stats.DropsPipelineQueueFull.Load() == p.stats.DropsQueueFull.Load()
	})

	snap := stats.snapshot()
	if snap.EventsDropped.PipelineQueueFull == 0 {
		t.Error("snapshot events_dropped.pipeline_queue_full is 0 after a per-project overflow")
	}
	if snap.EventsDropped.QueueFull != 0 {
		t.Errorf("top-level queue_full = %d; it counts the shared queue only, which had capacity", snap.EventsDropped.QueueFull)
	}

	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		EventsDropped map[string]uint64 `json:"events_dropped"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EventsDropped["pipeline_queue_full"] != snap.EventsDropped.PipelineQueueFull {
		t.Errorf("/stats JSON pipeline_queue_full = %d, want %d", decoded.EventsDropped["pipeline_queue_full"], snap.EventsDropped.PipelineQueueFull)
	}
}
