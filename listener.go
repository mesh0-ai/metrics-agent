package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// readBufBytes is the kernel-side socket receive buffer. Setting it large
// lets short bursts queue in the kernel rather than being dropped at the
// socket boundary; the agent's own goroutine drains it in a tight loop.
const readBufBytes = 8 << 20 // 8MB

// listen reads datagrams off a Unix-domain SOCK_DGRAM socket bound at
// `path` and forwards each as a rawDatagram to `out`. Every datagram is
// treated as one candidate JSON event; structural validation happens
// downstream in the batcher.
//
// The listener never blocks on a full channel — it drops the newest datagram
// and increments drops.queue_full so a slow flusher cannot stall the read
// loop. On context cancel it closes the socket (which unblocks ReadFrom)
// and unlinks the socket file before returning.
//
// readBufSize bounds each ReadFrom call. To keep oversize accounting visible,
// callers should pass `maxEventBytes + 1` so a too-large datagram is read at
// boundary+1 (and then dropped by the validator) rather than silently
// truncated at maxEventBytes by the kernel.
// listenSink is the interface the listener uses to dispatch a parsed
// datagram. In production this is implemented by *registry (multi-tenant
// routing). Tests inject a simpler sink that forwards to a single channel
// to keep the existing listener tests focused on read-loop semantics.
type listenSink interface {
	// dispatch routes one datagram. Returns false if the datagram was
	// dropped for a reason the listener should NOT account as queue_full
	// (the sink has already bumped the appropriate counter).
	dispatch(dg rawDatagram) (delivered bool, queueFull bool)
}

func listen(ctx context.Context, path string, readBufSize int, sink listenSink, log *slog.Logger, stats *selfStats) error {
	if path == "" {
		return errors.New("MESH0_LISTEN_PATH is empty")
	}
	if readBufSize <= 0 {
		// +1 vs. the validator cap so an exactly-cap-sized datagram still
		// fits and an oversize one is read at boundary+1 and accounted via
		// drops.oversize rather than silently truncated by the kernel.
		readBufSize = DefaultMaxEventBytes + 1
	}

	conn, cleanup, err := bindUnixgram(path, log, stats)
	if err != nil {
		return err
	}
	defer cleanup()

	// Pool sized to the configured read buffer; one listener exists per
	// process, so this effectively holds a single buffer reused across reads.
	// The pooled buffer is never handed downstream — bytes are copied into a
	// fresh slice before dispatch so the buffer can be reused immediately.
	pool := sync.Pool{New: func() any { b := make([]byte, readBufSize); return &b }}

	log.Info("listener started", "path", conn.LocalAddr().String(), "read_buf_bytes", readBufSize)

	closerDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		_ = conn.Close()
		close(closerDone)
	}()
	defer func() { <-closerDone }()

	const readErrBackoffCap = 250 * time.Millisecond
	var readErrBackoff time.Duration

	for {
		bp := pool.Get().(*[]byte)
		buf := *bp
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			pool.Put(bp)
			if ctx.Err() != nil {
				return nil
			}
			stats.ReadErrors.Add(1)
			log.Warn("read error", "err", err)
			if readErrBackoff == 0 {
				readErrBackoff = 10 * time.Millisecond
			} else if readErrBackoff < readErrBackoffCap {
				readErrBackoff *= 2
				if readErrBackoff > readErrBackoffCap {
					readErrBackoff = readErrBackoffCap
				}
			}
			select {
			case <-time.After(readErrBackoff):
			case <-ctx.Done():
				return nil
			}
			continue
		}
		readErrBackoff = 0
		stats.EventsReceived.Add(1)

		datagram := make([]byte, n)
		copy(datagram, buf[:n])
		pool.Put(bp)

		_, queueFull := sink.dispatch(rawDatagram{bytes: datagram, at: time.Now()})
		if queueFull {
			stats.DropsQueueFull.Add(1)
		}
	}
}

// bindUnixgram opens the SOCK_DGRAM socket and returns it alongside a
// cleanup hook. The cleanup runs after the read loop exits — it removes the
// socket file so a restart isn't blocked by EADDRINUSE.
func bindUnixgram(path string, log *slog.Logger, stats *selfStats) (net.PacketConn, func(), error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		// Best-effort: parent dir is usually pre-created by the operator
		// (mounted volume or tmpfs). If it isn't, fall through so the bind
		// error surfaces with the right context — but log at debug so the
		// chain remains traceable when an operator is debugging perms.
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Debug("mkdir parent dir failed", "dir", dir, "err", err)
		}
	}
	// Stale socket files survive ungraceful shutdowns. ListenUnixgram
	// would return EADDRINUSE; remove first so restarts are idempotent.
	// Only unlink existing *socket* nodes — never a regular file the
	// operator might have placed there.
	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, nil, fmt.Errorf("listen path %q exists and is not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, nil, fmt.Errorf("remove stale socket %q: %w", path, err)
		}
	}

	unixAddr, err := net.ResolveUnixAddr("unixgram", path)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve unix addr: %w", err)
	}
	conn, err := net.ListenUnixgram("unixgram", unixAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen unixgram: %w", err)
	}
	if err := conn.SetReadBuffer(readBufBytes); err != nil {
		stats.BufferDegraded.Store(true)
		log.Warn("set unix read buffer failed", "err", err)
	}
	// The agent runs as a sidecar shared with the app process, which
	// commonly runs as a different uid (e.g. www-data). 0666 keeps the
	// fire-and-forget contract working without forcing operators to match
	// uids. If you need stricter perms, set them on the parent directory.
	// A chmod failure means cross-uid clients cannot write — fail the bind
	// rather than silently running a useless agent that reports no traffic.
	if err := os.Chmod(path, 0o666); err != nil {
		_ = conn.Close()
		_ = os.Remove(path)
		return nil, nil, fmt.Errorf("chmod unix socket %q: %w", path, err)
	}

	cleanup := func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Warn("unix socket cleanup failed", "path", path, "err", err)
		}
	}
	return conn, cleanup, nil
}
