package main

import (
	"testing"
	"time"
)

func TestAggregatorCounterGaugeTiming(t *testing.T) {
	in := make(chan Metric, 16)
	flush := make(chan time.Time, 1)
	out := make(chan Snapshot, 1)
	a := newAggregator(in, flush, out)

	done := make(chan struct{})
	go func() { a.run(); close(done) }()

	send := func(line string) {
		t.Helper()
		m, err := parseLine(line)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		in <- m
	}

	send("hits:1|c|#tier:pro")
	send("hits:1|c|#tier:pro")
	send("hits:1|c|#tier:pro|@0.5") // sampled, contributes 2
	send("queue:7|g")
	send("queue:9|g") // last-write-wins
	send("lat:10|ms")
	send("lat:20|ms")
	send("lat:30|ms")

	// Closing `in` drives the aggregator's shutdown emit deterministically;
	// using the flush ticker would race the in-channel drain in the select.
	close(in)
	var snap Snapshot
	select {
	case snap = <-out:
	case <-time.After(time.Second):
		t.Fatal("flush timed out")
	}
	<-done
	_ = flush

	byKey := map[string]FlushedMetric{}
	for _, m := range snap.Metrics {
		byKey[m.Name+"|"+m.Type] = m
	}

	hits := byKey["hits|counter"]
	if hits.Value != 4 {
		t.Errorf("hits counter: got %v want 4", hits.Value)
	}
	if hits.Tags["tier"] != "pro" {
		t.Errorf("hits tags: %+v", hits.Tags)
	}

	gauge := byKey["queue|gauge"]
	if gauge.Value != 9 {
		t.Errorf("queue gauge: got %v want 9", gauge.Value)
	}

	timing := byKey["lat|timing"]
	if timing.Count != 3 || timing.Sum != 60 || timing.Min != 10 || timing.Max != 30 {
		t.Errorf("lat timing: %+v", timing)
	}
	if timing.P50 == 0 || timing.P95 == 0 || timing.P99 == 0 {
		t.Errorf("lat percentiles unset: %+v", timing)
	}
}
