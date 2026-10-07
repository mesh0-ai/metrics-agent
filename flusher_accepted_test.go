package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// mesh0's open-mode admission answers `202 {"status":"provisioning",
// "accepted":0}` while a project is still being provisioned, and that body
// means "dropped, keep sending". These pin that the agent reads the body
// instead of counting every 2xx as delivered.

func acceptedServer(t *testing.T, answer func(hit int32) (int, string)) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		status, body := answer(n)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestEventsFlusherRetriesAcceptedZero(t *testing.T) {
	srv, hits := acceptedServer(t, func(hit int32) (int, string) {
		if hit <= 2 {
			return http.StatusAccepted, `{"status":"provisioning","accepted":0}`
		}
		return http.StatusOK, `{"accepted":3}`
	})

	f, stats := newTestEventsFlusher(t, srv.URL, 4)
	f.notAcceptedBackoff = time.Millisecond
	f.send(sampleEventBatch(3))

	if got := atomic.LoadInt32(hits); got != 3 {
		t.Errorf("attempts: got %d, want 3 (two 202s retried, then accepted)", got)
	}
	if stats.EventsSent.Load() != 3 || stats.BatchesSent.Load() != 1 {
		t.Errorf("sent: events=%d batches=%d, want 3 and 1", stats.EventsSent.Load(), stats.BatchesSent.Load())
	}
	if stats.DropsNotAccepted.Load() != 0 || stats.DropsFlushFailed.Load() != 0 {
		t.Errorf("drops: not_accepted=%d flush_failed=%d, want 0", stats.DropsNotAccepted.Load(), stats.DropsFlushFailed.Load())
	}
}

func TestEventsFlusherCountsExhaustedAcceptedZeroAsNotAccepted(t *testing.T) {
	srv, hits := acceptedServer(t, func(int32) (int, string) {
		return http.StatusAccepted, `{"status":"provisioning","accepted":0}`
	})

	f, stats := newTestEventsFlusher(t, srv.URL, 2)
	f.notAcceptedBackoff = time.Millisecond
	f.send(sampleEventBatch(4))

	if got := atomic.LoadInt32(hits); got != 3 {
		t.Errorf("attempts: got %d, want 3 (1 + 2 retries)", got)
	}
	if stats.EventsSent.Load() != 0 || stats.BatchesSent.Load() != 0 {
		t.Errorf("a dropped batch was counted as sent: events=%d batches=%d", stats.EventsSent.Load(), stats.BatchesSent.Load())
	}
	if stats.DropsNotAccepted.Load() != 4 {
		t.Errorf("not_accepted: got %d, want 4", stats.DropsNotAccepted.Load())
	}
	if stats.DropsFlushFailed.Load() != 0 {
		t.Errorf("flush_failed: got %d, want 0 (the gateway answered; it refused)", stats.DropsFlushFailed.Load())
	}
}

func TestEventsFlusherA202WithoutABodyIsNotAccepted(t *testing.T) {
	srv, _ := acceptedServer(t, func(int32) (int, string) {
		return http.StatusAccepted, ``
	})

	f, stats := newTestEventsFlusher(t, srv.URL, 0)
	f.notAcceptedBackoff = time.Millisecond
	f.send(sampleEventBatch(2))

	if stats.EventsSent.Load() != 0 || stats.DropsNotAccepted.Load() != 2 {
		t.Errorf("events_sent=%d not_accepted=%d, want 0 and 2", stats.EventsSent.Load(), stats.DropsNotAccepted.Load())
	}
}

// A partial acceptance is NOT retried: the gateway kept some events and we
// cannot tell which, so a retry would duplicate the accepted ones. The
// shortfall is counted as not accepted.
func TestEventsFlusherPartialAcceptanceIsNotRetried(t *testing.T) {
	srv, hits := acceptedServer(t, func(int32) (int, string) {
		return http.StatusOK, `{"accepted":2}`
	})

	f, stats := newTestEventsFlusher(t, srv.URL, 4)
	f.notAcceptedBackoff = time.Millisecond
	f.send(sampleEventBatch(5))

	if got := atomic.LoadInt32(hits); got != 1 {
		t.Errorf("attempts: got %d, want 1", got)
	}
	if stats.EventsSent.Load() != 2 || stats.DropsNotAccepted.Load() != 3 {
		t.Errorf("events_sent=%d not_accepted=%d, want 2 and 3", stats.EventsSent.Load(), stats.DropsNotAccepted.Load())
	}
	if stats.BatchesSent.Load() != 1 {
		t.Errorf("batches_sent: got %d, want 1", stats.BatchesSent.Load())
	}
}

// A 200 with no recognizable body keeps today's meaning (all delivered), so
// a gateway that does not report `accepted` is unaffected.
func TestEventsFlusher200WithoutAcceptedCountsAll(t *testing.T) {
	for _, body := range []string{``, `{}`, `not json`, `{"accepted":"3"}`} {
		srv, _ := acceptedServer(t, func(int32) (int, string) { return http.StatusOK, body })
		f, stats := newTestEventsFlusher(t, srv.URL, 0)
		f.send(sampleEventBatch(3))
		if stats.EventsSent.Load() != 3 || stats.DropsNotAccepted.Load() != 0 {
			t.Errorf("body %q: events_sent=%d not_accepted=%d, want 3 and 0", body, stats.EventsSent.Load(), stats.DropsNotAccepted.Load())
		}
	}
}

func TestEventsFlusherAcceptedAboveTheBatchIsCapped(t *testing.T) {
	srv, _ := acceptedServer(t, func(int32) (int, string) { return http.StatusOK, `{"accepted":9}` })
	f, stats := newTestEventsFlusher(t, srv.URL, 0)
	f.send(sampleEventBatch(3))
	if stats.EventsSent.Load() != 3 {
		t.Errorf("events_sent: got %d, want 3", stats.EventsSent.Load())
	}
}

func TestNotAcceptedReachesThePipelineAndTheSnapshot(t *testing.T) {
	srv, _ := acceptedServer(t, func(int32) (int, string) {
		return http.StatusAccepted, `{"status":"provisioning","accepted":0}`
	})
	f, stats := newTestEventsFlusher(t, srv.URL, 0)
	f.notAcceptedBackoff = time.Millisecond
	ps := newPipelineStats()
	f.pipelineStats = ps
	f.send(sampleEventBatch(2))

	if ps.DropsNotAccepted.Load() != 2 {
		t.Errorf("pipeline not_accepted: got %d, want 2", ps.DropsNotAccepted.Load())
	}
	if got := stats.snapshot().EventsDropped.NotAccepted; got != 2 {
		t.Errorf("snapshot not_accepted: got %d, want 2", got)
	}
	if got := ps.snapshot().EventsDropped.NotAccepted; got != 2 {
		t.Errorf("pipeline snapshot not_accepted: got %d, want 2", got)
	}
}
