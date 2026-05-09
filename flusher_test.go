package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func newTestEventsFlusher(t *testing.T, url string, maxRetries int) (*eventsFlusher, *selfStats) {
	t.Helper()
	stats := newSelfStats()
	cfg := Config{GatewayURL: url, EventsPath: "/v1/events", APIKey: "k", MaxRetries: maxRetries}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	in := make(chan EventBatch, 1)
	f := newEventsFlusher(in, cfg, log, stats)
	f.ctx = context.Background()
	return f, stats
}

func sampleEventBatch(n int) EventBatch {
	evs := make([]json.RawMessage, n)
	for i := 0; i < n; i++ {
		evs[i] = json.RawMessage(`{"status":"success"}`)
	}
	return EventBatch{Events: evs, StartedAt: time.Unix(1700000000, 0)}
}

func TestEventsFlusherSuccess(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.URL.Path != "/v1/events" {
			t.Errorf("path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("auth: %s", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		var p struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("decode: %v body=%s", err, body)
		}
		if len(p.Events) != 3 {
			t.Errorf("events: got %d want 3", len(p.Events))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, stats := newTestEventsFlusher(t, srv.URL, 0)
	f.send(sampleEventBatch(3))
	if hits != 1 {
		t.Errorf("expected 1 request, got %d", hits)
	}
	if stats.BatchesSent.Load() != 1 || stats.EventsSent.Load() != 3 {
		t.Errorf("stats: batches=%d events=%d", stats.BatchesSent.Load(), stats.EventsSent.Load())
	}
}

func TestEventsFlusherRetriesOn429(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, stats := newTestEventsFlusher(t, srv.URL, 4)
	// Use a deterministic backoff source by shrinking to near-zero via
	// the rng mean of 1.0; we just need the test to not take forever.
	f.send(sampleEventBatch(1))
	if hits != 2 {
		t.Errorf("expected 2 attempts, got %d", hits)
	}
	if stats.BatchesSent.Load() != 1 || stats.DropsFlushFailed.Load() != 0 {
		t.Errorf("stats: batches=%d failed=%d", stats.BatchesSent.Load(), stats.DropsFlushFailed.Load())
	}
}

func TestEventsFlusherDropsAfterMaxRetries(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	// 1 + maxRetries = 3 attempts total.
	f, stats := newTestEventsFlusher(t, srv.URL, 2)
	f.send(sampleEventBatch(5))
	if hits != 3 {
		t.Errorf("expected 3 attempts (1+2 retries), got %d", hits)
	}
	if stats.DropsFlushFailed.Load() != 5 {
		t.Errorf("expected drops_flush_failed=5, got %d", stats.DropsFlushFailed.Load())
	}
	if stats.BatchesSent.Load() != 0 {
		t.Errorf("expected batches=0, got %d", stats.BatchesSent.Load())
	}
}

func TestEventsFlusherDoesNotRetryOn4xx(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	f, stats := newTestEventsFlusher(t, srv.URL, 4)
	f.send(sampleEventBatch(1))
	if hits != 1 {
		t.Errorf("expected 1 attempt (no retry on 401), got %d", hits)
	}
	if stats.DropsFlushFailed.Load() != 1 {
		t.Errorf("expected drops_flush_failed=1, got %d", stats.DropsFlushFailed.Load())
	}
}

func TestEncodeEventBatchShape(t *testing.T) {
	b := EventBatch{Events: []json.RawMessage{
		json.RawMessage(`{"a":1}`),
		json.RawMessage(`{"b":2}`),
	}}
	out := encodeEventBatch(b)
	want := []byte(`{"events":[{"a":1},{"b":2}]}`)
	if !bytes.Equal(out, want) {
		t.Errorf("encode: got %s want %s", out, want)
	}
	// Round-trip
	var p struct {
		Events []map[string]int `json:"events"`
	}
	if err := json.Unmarshal(out, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(p.Events) != 2 || p.Events[0]["a"] != 1 || p.Events[1]["b"] != 2 {
		t.Errorf("round-trip: %+v", p)
	}
}

func TestMain(m *testing.M) { os.Exit(m.Run()) }
