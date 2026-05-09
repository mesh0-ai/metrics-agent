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

// readerPool holds 64K scratch buffers reused across ReadFrom calls. The
// listener never hands the pooled buffer itself downstream — bytes are copied
// into a fresh slice before dispatch so the pooled buffer can be reused
// immediately.
var readerPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// listen reads datagrams off a Unix-domain SOCK_DGRAM socket bound at
// `path` and forwards each as a rawDatagram to `out`. Every datagram is
// treated as one candidate JSON event; structural validation happens
// downstream in the batcher.
//
// The listener never blocks on a full channel — it drops the newest datagram
// and increments drops.queue_full so a slow flusher cannot stall the read
// loop. On context cancel it closes the socket (which unblocks ReadFrom)
// and unlinks the socket file before returning.
func listen(ctx context.Context, path string, out chan<- rawDatagram, log *slog.Logger, stats *selfStats) error {
	if path == "" {
		return errors.New("MESH0_LISTEN_PATH is empty")
	}

	conn, cleanup, err := bindUnixgram(path, log, stats)
	if err != nil {
		return err
	}
	defer cleanup()

	log.Info("listener started", "path", conn.LocalAddr().String())

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
		bp := readerPool.Get().(*[]byte)
		buf := *bp
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			readerPool.Put(bp)
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
		readerPool.Put(bp)

		select {
		case out <- rawDatagram{bytes: datagram, at: time.Now()}:
		default:
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
