package main

import (
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

func newTestFlusher(t *testing.T, url string) (*flusher, *selfStats) {
	t.Helper()
	stats := newSelfStats()
	cfg := Config{GatewayURL: url, FlushPath: "", APIKey: "k", ProjectID: "p"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := newFlusher(make(chan Snapshot), cfg, log, stats)
	f.ctx = context.Background()
	return f, stats
}

func sampleSnapshot() Snapshot {
	v := 1.0
	return Snapshot{
		Since:   time.Unix(1700000000, 0),
		Until:   time.Unix(1700000010, 0),
		Metrics: []FlushedMetric{{Name: "x", Type: "counter", Value: &v}},
	}
}

func TestFlusherSuccess(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("missing auth: %s", r.Header.Get("Authorization"))
		}
		var p flushPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if p.ProjectID != "p" || len(p.Metrics) != 1 {
			t.Errorf("payload: %+v", p)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, stats := newTestFlusher(t, srv.URL)
	f.send(sampleSnapshot())
	if hits != 1 {
		t.Errorf("expected 1 request, got %d", hits)
	}
	if stats.FlushesOK.Load() != 1 || stats.FlushesFailed.Load() != 0 {
		t.Errorf("stats: ok=%d fail=%d", stats.FlushesOK.Load(), stats.FlushesFailed.Load())
	}
}

func TestFlusherRetriesOn5xx(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f, stats := newTestFlusher(t, srv.URL)
	f.send(sampleSnapshot())
	if hits != 2 {
		t.Errorf("expected 2 attempts, got %d", hits)
	}
	if stats.FlushesOK.Load() != 1 {
		t.Errorf("expected ok=1, got %d", stats.FlushesOK.Load())
	}
}

func TestFlusherDoesNotRetryOn4xx(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	f, stats := newTestFlusher(t, srv.URL)
	f.send(sampleSnapshot())
	if hits != 1 {
		t.Errorf("expected 1 attempt (no retry on 401), got %d", hits)
	}
	if stats.FlushesFailed.Load() != 1 {
		t.Errorf("expected failed=1, got %d", stats.FlushesFailed.Load())
	}
}

func TestFlusherCancelStopsRetry(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	f, _ := newTestFlusher(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	f.ctx = ctx
	cancel()
	f.send(sampleSnapshot())
	if hits > 1 {
		t.Errorf("cancel should prevent retry, got %d hits", hits)
	}
}

func TestMain(m *testing.M) { os.Exit(m.Run()) }
