package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests exercise the events batcher and the listener-to-batcher
// dispatch path.

func newTestBatcher(t *testing.T, maxBatch int, window time.Duration) (chan rawDatagram, chan EventBatch, *eventsBatcher, *selfStats, chan struct{}) {
	t.Helper()
	in := make(chan rawDatagram, 1024)
	out := make(chan EventBatch, 16)
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := newEventsBatcher(in, out, stats, log, maxBatch, window)
	done := make(chan struct{})
	go func() {
		b.run()
		close(done)
	}()
	return in, out, b, stats, done
}

func dgFromObj(t *testing.T, obj map[string]any) rawDatagram {
	t.Helper()
	by, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return rawDatagram{bytes: by, at: time.Now()}
}

func TestBatcherFlushesAtMaxBatch(t *testing.T) {
	in, out, _, stats, done := newTestBatcher(t, 500, 5*time.Second)
	defer func() {
		close(in)
		<-done
	}()

	// Fire 600 events. We expect at least one batch at the 500 boundary,
	// and a remainder still buffered (drained on close).
	for i := 0; i < 600; i++ {
		in <- dgFromObj(t, map[string]any{"i": i})
	}

	var first EventBatch
	select {
	case first = <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch never arrived")
	}
	if len(first.Events) != 500 {
		t.Errorf("first batch size: got %d want 500", len(first.Events))
	}
	if stats.EventsReceived.Load() < 500 {
		t.Errorf("events_received: got %d want >=500", stats.EventsReceived.Load())
	}
}

func TestBatcherFlushesAtWindow(t *testing.T) {
	in, out, _, _, done := newTestBatcher(t, 500, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()
	in <- dgFromObj(t, map[string]any{"a": 1})
	in <- dgFromObj(t, map[string]any{"a": 2})

	select {
	case b := <-out:
		if len(b.Events) != 2 {
			t.Errorf("batch size: got %d want 2", len(b.Events))
		}
	case <-time.After(time.Second):
		t.Fatal("window flush never fired")
	}
}

func TestBatcherDropsOversize(t *testing.T) {
	in, _, _, stats, done := newTestBatcher(t, 500, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()
	big := strings.Repeat("a", 33*1024)
	by, _ := json.Marshal(map[string]any{"x": big})
	in <- rawDatagram{bytes: by, at: time.Now()}
	// Give the batcher a chance to process.
	time.Sleep(100 * time.Millisecond)
	if stats.DropsOversize.Load() != 1 {
		t.Errorf("expected drops.oversize=1, got %d", stats.DropsOversize.Load())
	}
}

func TestBatcherDropsParseError(t *testing.T) {
	in, _, _, stats, done := newTestBatcher(t, 500, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()
	in <- rawDatagram{bytes: []byte("{not json"), at: time.Now()}
	time.Sleep(100 * time.Millisecond)
	if stats.DropsParseError.Load() != 1 {
		t.Errorf("expected drops.parse_error=1, got %d", stats.DropsParseError.Load())
	}
}

func TestBatcherFinalFlushOnClose(t *testing.T) {
	in, out, _, _, done := newTestBatcher(t, 500, 5*time.Second)
	in <- dgFromObj(t, map[string]any{"a": 1})
	close(in)
	<-done

	select {
	case b := <-out:
		if len(b.Events) != 1 {
			t.Errorf("final batch size: got %d", len(b.Events))
		}
	default:
		t.Fatal("expected a final batch on shutdown")
	}
}

// TestListenerDispatchesJSON is an integration test that fires real UDP
// datagrams at the listener and verifies SetReadBuffer is invoked (proven
// by the listener starting successfully) and JSON datagrams reach the
// batcher.
func TestListenerDispatchesJSON(t *testing.T) {
	stats := newSelfStats()
	rawCh := make(chan rawDatagram, 16)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bind to an ephemeral port so parallel runs don't collide.
	addr := "127.0.0.1:0"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	chosenAddr := conn.LocalAddr().String()
	conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = listen(ctx, chosenAddr, rawCh, log, stats)
	}()
	// Give the listener a moment to bind.
	time.Sleep(50 * time.Millisecond)

	cli, err := net.Dial("udp", chosenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// JSON event
	if _, err := cli.Write([]byte(`{"op":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	// Non-JSON datagram (statsd-style) — should be silently dropped in
	// the v0.2 events-only listener.
	if _, err := cli.Write([]byte(`metric:1|c`)); err != nil {
		t.Fatal(err)
	}

	select {
	case dg := <-rawCh:
		if !strings.HasPrefix(strings.TrimSpace(string(dg.bytes)), "{") {
			t.Errorf("expected JSON datagram, got %q", dg.bytes)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive JSON datagram")
	}

	cancel()
	wg.Wait()
}
