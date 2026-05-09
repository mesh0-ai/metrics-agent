package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// shortTempSocketPath returns a unique path under /tmp suitable for a UDS
// bind. macOS caps sun_path at 104 bytes and Linux at 108, which Go's
// t.TempDir() routinely exceeds — keep these short. The test registers
// a t.Cleanup hook so a botched test doesn't leave the file behind.
func shortTempSocketPath(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	p := filepath.Join("/tmp", "mesh0-agent-"+hex.EncodeToString(b[:])+".sock")
	t.Cleanup(func() { _ = os.Remove(p) })
	return p
}

// TestListenRoundTrip ensures a datagram fired at the listen path lands on
// the rawCh end of the listener with bytes intact, and that the socket
// file is unlinked when the listener returns.
func TestListenRoundTrip(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UDS-DGRAM not supported on Windows")
	}
	sockPath := shortTempSocketPath(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan rawDatagram, 4)
	stats := newSelfStats()
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	listenErr := make(chan error, 1)
	go func() { listenErr <- listen(ctx, sockPath, out, log, stats) }()

	if err := waitForSocket(sockPath, 500*time.Millisecond); err != nil {
		t.Fatalf("socket not ready: %v", err)
	}

	// 0666 is load-bearing: the agent runs as a sidecar shared with an app
	// that may use a different uid (e.g. www-data). A regression that
	// drops the chmod would silently break every cross-uid deployment.
	if fi, err := os.Stat(sockPath); err != nil {
		t.Fatalf("stat socket: %v", err)
	} else if perm := fi.Mode().Perm(); perm != 0o666 {
		t.Errorf("socket perms: got %o want 0666", perm)
	}
	if stats.BufferDegraded.Load() {
		t.Errorf("buffer_degraded set after successful bind — SetReadBuffer rejected by kernel")
	}

	cliAddr, err := net.ResolveUnixAddr("unixgram", sockPath)
	if err != nil {
		t.Fatalf("resolve client addr: %v", err)
	}
	cli, err := net.DialUnix("unixgram", nil, cliAddr)
	if err != nil {
		t.Fatalf("dial unixgram: %v", err)
	}
	payload := []byte(`{"operation":"checkout.charge","duration_ms":42}`)
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("write datagram: %v", err)
	}
	_ = cli.Close()

	select {
	case dg := <-out:
		if string(dg.bytes) != string(payload) {
			t.Errorf("payload mismatch: got %q want %q", dg.bytes, payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("no datagram received within 1s")
	}

	cancel()
	select {
	case err := <-listenErr:
		if err != nil {
			t.Errorf("listen returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listen did not exit within 2s of cancel")
	}
	if _, err := os.Stat(sockPath); !os.IsNotExist(err) {
		t.Errorf("socket file not cleaned up: stat err = %v", err)
	}
}

// TestListenRemovesStaleSocket ensures the listener removes a leftover
// socket file from a previous unclean shutdown rather than failing with
// EADDRINUSE.
func TestListenRemovesStaleSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UDS-DGRAM not supported on Windows")
	}
	sockPath := shortTempSocketPath(t)

	stale, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sockPath, Net: "unixgram"})
	if err != nil {
		t.Fatalf("plant stale socket: %v", err)
	}
	_ = stale.Close()
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("stale socket missing pre-test: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan rawDatagram, 1)
	stats := newSelfStats()
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	listenErr := make(chan error, 1)
	go func() { listenErr <- listen(ctx, sockPath, out, log, stats) }()

	if err := waitForSocket(sockPath, 500*time.Millisecond); err != nil {
		t.Fatalf("socket not ready: %v", err)
	}
	cancel()
	<-listenErr
}

// TestListenRejectsNonSocketFile ensures we don't unlink an arbitrary file
// the operator left in place.
func TestListenRejectsNonSocketFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UDS-DGRAM not supported on Windows")
	}
	path := shortTempSocketPath(t)
	if err := os.WriteFile(path, []byte("hi"), 0o644); err != nil {
		t.Fatalf("create file: %v", err)
	}

	ctx := context.Background()
	out := make(chan rawDatagram, 1)
	stats := newSelfStats()
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	err := listen(ctx, path, out, log, stats)
	if err == nil {
		t.Fatal("expected error binding over a non-socket file")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("regular file at bind path was disturbed: %v", err)
	}
}

// TestListenerOversizeOverWire fires a datagram larger than MaxEventBytes
// at the real socket, and asserts the batcher records it as drops.oversize.
// This closes the loop on the read-pool sizing — a future change that
// shrinks the pooled buffer below MaxEventBytes would silently truncate
// payloads instead of accounting them.
func TestListenerOversizeOverWire(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UDS-DGRAM not supported on Windows")
	}
	if runtime.GOOS == "darwin" {
		// macOS caps unixgram datagrams at net.local.dgram.maxdgram (2048
		// bytes by default) — well under MaxEventBytes (32KB), so we can't
		// even send an oversize payload locally. CI is Linux; the
		// regression guard runs there.
		t.Skip("macOS caps unixgram datagram size below MaxEventBytes")
	}
	sockPath := shortTempSocketPath(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawCh := make(chan rawDatagram, 4)
	batchCh := make(chan EventBatch, 4)
	stats := newSelfStats()
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	listenErr := make(chan error, 1)
	go func() { listenErr <- listen(ctx, sockPath, rawCh, log, stats) }()
	if err := waitForSocket(sockPath, 500*time.Millisecond); err != nil {
		t.Fatalf("socket not ready: %v", err)
	}

	// Drain rawCh through a real batcher so DropsOversize is incremented.
	b := newEventsBatcher(rawCh, batchCh, stats, log, 500, 50*time.Millisecond)
	batchDone := make(chan struct{})
	go func() { b.run(); close(batchDone) }()

	// MUST_DGRAM_PAYLOAD_MAX on Linux defaults around 200KB+ for unixgram
	// which is well over MaxEventBytes (32KB), so the kernel will deliver
	// our oversize payload intact and the batcher will reject it.
	cliAddr, err := net.ResolveUnixAddr("unixgram", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := net.DialUnix("unixgram", nil, cliAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 40KB payload — well past MaxEventBytes (32KB), well under 64KB pool.
	payload := append([]byte(`{"x":"`), make([]byte, 40*1024)...)
	for i := 6; i < len(payload); i++ {
		payload[i] = 'a'
	}
	payload = append(payload, []byte(`"}`)...)
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("write oversize: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stats.DropsOversize.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := stats.DropsOversize.Load(); got != 1 {
		t.Fatalf("drops.oversize: got %d want 1", got)
	}

	cancel()
	<-listenErr
	close(rawCh)
	<-batchDone
}

// TestListenRejectsEmptyPath guards against a misconfigured launch where
// MESH0_LISTEN_PATH is unset or blank.
func TestListenRejectsEmptyPath(t *testing.T) {
	stats := newSelfStats()
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := listen(context.Background(), "", make(chan rawDatagram, 1), log, stats); err == nil {
		t.Fatal("expected error from empty path")
	}
}

func waitForSocket(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fi, err := os.Stat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return os.ErrNotExist
}
