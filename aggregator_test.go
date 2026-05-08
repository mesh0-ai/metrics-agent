package main

import (
	"testing"
	"time"
)

func TestAggregatorCounterGaugeTiming(t *testing.T) {
	in := make(chan Metric, 16)
	flush := make(chan time.Time, 1)
	out := make(chan Snapshot, 1)
	a := newAggregator(in, flush, out, newSelfStats())

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
	if hits.Value == nil || *hits.Value != 4 {
		t.Errorf("hits counter: got %v want 4", hits.Value)
	}
	if hits.Tags["tier"] != "pro" {
		t.Errorf("hits tags: %+v", hits.Tags)
	}

	gauge := byKey["queue|gauge"]
	if gauge.Value == nil || *gauge.Value != 9 {
		t.Errorf("queue gauge: got %v want 9", gauge.Value)
	}

	timing := byKey["lat|timing"]
	if timing.Count == nil || *timing.Count != 3 ||
		timing.Sum == nil || *timing.Sum != 60 ||
		timing.Min == nil || *timing.Min != 10 ||
		timing.Max == nil || *timing.Max != 30 {
		t.Errorf("lat timing: %+v", timing)
	}
	if timing.P50 == nil || timing.P95 == nil || timing.P99 == nil {
		t.Errorf("lat percentiles unset: %+v", timing)
	}
	// 3 samples [10,20,30]: ceil(0.5*3)-1=1 → 20; ceil(0.95*3)-1=2 → 30.
	if *timing.P50 != 20 {
		t.Errorf("p50: got %v want 20", *timing.P50)
	}
	if *timing.P95 != 30 || *timing.P99 != 30 {
		t.Errorf("p95/p99: got %v/%v want 30/30", *timing.P95, *timing.P99)
	}
}

func TestAggregatorPreservesZero(t *testing.T) {
	in := make(chan Metric, 4)
	flush := make(chan time.Time, 1)
	out := make(chan Snapshot, 1)
	a := newAggregator(in, flush, out, newSelfStats())

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
	send("queue:0|g")
	send("zero.lat:0|ms")
	close(in)

	snap := <-out
	<-done

	for _, m := range snap.Metrics {
		switch m.Name {
		case "queue":
			if m.Value == nil || *m.Value != 0 {
				t.Errorf("zero gauge dropped: %+v", m)
			}
		case "zero.lat":
			if m.Min == nil || *m.Min != 0 {
				t.Errorf("zero timing min dropped: %+v", m)
			}
		}
	}
}
