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
	return newTestBatcherWithCap(t, maxBatch, DefaultMaxEventBytes, window)
}

func newTestBatcherWithCap(t *testing.T, maxBatch, maxEventBytes int, window time.Duration) (chan rawDatagram, chan EventBatch, *eventsBatcher, *selfStats, chan struct{}) {
	t.Helper()
	in := make(chan rawDatagram, 1024)
	out := make(chan EventBatch, 16)
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := newEventsBatcher(in, out, stats, log, maxBatch, maxEventBytes, window)
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
	in, out, _, _, done := newTestBatcher(t, 500, 5*time.Second)
	defer func() {
		close(in)
		<-done
	}()

	// Fire 600 events. We expect at least one batch at the 500 boundary,
	// and a remainder still buffered (drained on close).
	for i := 0; i < 600; i++ {
		in <- dgFromObj(t, map[string]any{"duration_ms": i})
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
}

func TestBatcherFlushesAtWindow(t *testing.T) {
	in, out, _, _, done := newTestBatcher(t, 500, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()
	in <- dgFromObj(t, map[string]any{"duration_ms": 1})
	in <- dgFromObj(t, map[string]any{"duration_ms": 2})

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
	const maxBytes = 4096
	in, _, _, stats, done := newTestBatcherWithCap(t, 500, maxBytes, 50*time.Millisecond)
	defer func() {
		close(in)
		<-done
	}()
	big := strings.Repeat("a", maxBytes+1)
	by, _ := json.Marshal(map[string]any{"status": big})
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

// TestBatcherFlushesAtByteCap verifies the byte-cap pre-flush at events.go
// (the comparison is "would appending push us past MaxBatchBytes?"). A
// regression that uses post-append size or inverts the comparison would
// produce oversized POSTs that the gateway 413s.
func TestBatcherFlushesAtByteCap(t *testing.T) {
	// Build events whose validated form is well under the configured
	// per-event cap (1 MB default). ~340 30 KB events sum to >10 MB
	// (MaxBatchBytes).
	in, out, _, _, done := newTestBatcher(t, 5000, 5*time.Second)
	defer func() {
		close(in)
		<-done
	}()

	const perEvent = 30 * 1024
	const want = (MaxBatchBytes / perEvent) + 5
	pad := strings.Repeat("a", perEvent-len(`{"status":""}`))
	for i := 0; i < want; i++ {
		ev := []byte(`{"status":"` + pad + `"}`)
		in <- rawDatagram{bytes: ev, at: time.Now()}
	}

	// At least one batch should arrive before the time window elapses,
	// because the byte cap forces an early flush.
	select {
	case b := <-out:
		// The pre-flush emits the *previous* contents, so the first batch
		// must already be packed near (but not over) the byte cap.
		var size int
		for _, ev := range b.Events {
			size += len(ev) + 1
		}
		if size > MaxBatchBytes {
			t.Errorf("batch exceeds MaxBatchBytes: %d > %d", size, MaxBatchBytes)
		}
		if len(b.Events) == 0 {
			t.Error("byte-cap flush emitted empty batch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("byte-cap flush never fired before window")
	}
}

func TestBatcherFinalFlushOnClose(t *testing.T) {
	in, out, _, _, done := newTestBatcher(t, 500, 5*time.Second)
	in <- dgFromObj(t, map[string]any{"duration_ms": 1})
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

// TestBatcherFinalFlushAbortsOnCtxCancel verifies that if the flusher is
// wedged and batchCh is full, the batcher's final flush does not deadlock
// — it observes ctx.Done() and accounts the dropped events as
// drops.shutdown rather than blocking shutdown forever.
func TestBatcherFinalFlushAbortsOnCtxCancel(t *testing.T) {
	in := make(chan rawDatagram, 16)
	// out has zero capacity and no consumer, so any send blocks.
	out := make(chan EventBatch)
	stats := newSelfStats()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := newEventsBatcher(in, out, stats, log, 500, DefaultMaxEventBytes, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	done := make(chan struct{})
	go func() {
		b.run()
		close(done)
	}()

	in <- dgFromObj(t, map[string]any{"duration_ms": 1})
	in <- dgFromObj(t, map[string]any{"duration_ms": 2})
	close(in)

	// Cancel after a short delay so the batcher reaches its final flush
	// and is blocked on `b.out <- batch`.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("batcher deadlocked on final flush despite ctx cancel")
	}
	if got := stats.DropsShutdown.Load(); got != 2 {
		t.Errorf("drops.shutdown: got %d want 2", got)
	}
}

// TestListenerDropsOnFullQueue is the regression guard for design priority
// #1 ("never block the caller"). A saturated `out` channel must produce
// drops.queue_full increments rather than stalling the read loop.
func TestListenerDropsOnFullQueue(t *testing.T) {
	stats := newSelfStats()
	// Capacity 1, never drained — anything past the first send goes to
	// the listener's `default` branch.
	rawCh := make(chan rawDatagram, 1)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	sockPath := shortTempSocketPath(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = listen(ctx, sockPath, DefaultMaxEventBytes+1, rawCh, log, stats)
	}()
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

	const N = 50
	for i := 0; i < N; i++ {
		if _, err := cli.Write([]byte(`{"i":1}`)); err != nil {
			t.Fatal(err)
		}
	}

	// Poll for drops to accumulate; the listener is non-blocking and the
	// kernel may drop some at the socket layer too, so we just need >0.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stats.DropsQueueFull.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if stats.DropsQueueFull.Load() == 0 {
		t.Fatalf("expected drops.queue_full > 0 after %d datagrams into a full channel", N)
	}

	cancel()
	wg.Wait()
}

// TestListenerDispatchesJSON is an integration test that fires real
// datagrams at the listener and verifies JSON datagrams reach the batcher
// untouched. Non-JSON datagrams pass through the listener as raw bytes;
// validation happens downstream in the batcher.
func TestListenerDispatchesJSON(t *testing.T) {
	stats := newSelfStats()
	rawCh := make(chan rawDatagram, 16)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	sockPath := shortTempSocketPath(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = listen(ctx, sockPath, DefaultMaxEventBytes+1, rawCh, log, stats)
	}()
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

	// JSON event
	if _, err := cli.Write([]byte(`{"op":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	// Non-JSON datagram — passes the listener as raw bytes; the batcher
	// will drop it as parse_error downstream.
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
