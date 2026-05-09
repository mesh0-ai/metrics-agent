package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// readBufBytes is the kernel-side socket receive buffer. Setting it large
// lets short bursts queue in the kernel rather than being dropped at the
// NIC/socket boundary; the agent's own goroutine drains it in a tight loop.
// Applies to both UDP and UDS-DGRAM listeners (Linux honors SO_RCVBUF on
// both).
const readBufBytes = 8 << 20 // 8MB

// readerPool holds 64K scratch buffers reused across ReadFrom calls. The
// listener never hands the pooled buffer itself downstream — bytes are copied
// into a fresh slice before dispatch so the pooled buffer can be reused
// immediately.
var readerPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// listenAddr is a parsed bind target. Exactly one of UDPAddr / UnixPath is
// populated.
type listenAddr struct {
	UDPAddr  string // "host:port" form when scheme=="udp"
	UnixPath string // filesystem path when scheme=="unix"
	scheme   string // "udp" | "unix"
}

// parseListenAddr accepts the legacy bare "host:port" form (UDP) or a
// `unix:///path` URL (UDS-DGRAM). The `unixgram://` scheme is also accepted
// as an alias because it more accurately describes what we open server-side.
func parseListenAddr(s string) (listenAddr, error) {
	if s == "" {
		return listenAddr{}, errors.New("MESH0_LISTEN_ADDR is empty")
	}
	switch {
	case strings.HasPrefix(s, "unix://"):
		path := strings.TrimPrefix(s, "unix://")
		if path == "" {
			return listenAddr{}, errors.New("unix:// listen addr requires a path")
		}
		return listenAddr{UnixPath: path, scheme: "unix"}, nil
	case strings.HasPrefix(s, "unixgram://"):
		path := strings.TrimPrefix(s, "unixgram://")
		if path == "" {
			return listenAddr{}, errors.New("unixgram:// listen addr requires a path")
		}
		return listenAddr{UnixPath: path, scheme: "unix"}, nil
	case strings.HasPrefix(s, "udp://"):
		return listenAddr{UDPAddr: strings.TrimPrefix(s, "udp://"), scheme: "udp"}, nil
	default:
		// Bare host:port — the historical form, still the default.
		return listenAddr{UDPAddr: s, scheme: "udp"}, nil
	}
}

// listen reads datagrams on `addr` and forwards each as a rawDatagram to
// `out`. Every datagram is treated as one candidate JSON event; structural
// validation happens downstream in the batcher.
//
// The listener never blocks on a full channel — it drops the newest datagram
// and increments drops.queue_full so a slow flusher cannot stall the read
// loop. On context cancel it closes the socket (which unblocks ReadFrom)
// and returns once the read loop exits. For UDS the socket file is unlinked
// on shutdown.
func listen(ctx context.Context, addr string, out chan<- rawDatagram, log *slog.Logger, stats *selfStats) error {
	la, err := parseListenAddr(addr)
	if err != nil {
		return err
	}

	conn, cleanup, err := bindListener(la, log, stats)
	if err != nil {
		return err
	}
	defer cleanup()

	log.Info("listener started",
		"scheme", la.scheme,
		"addr", conn.LocalAddr().String(),
	)

	closerDone := make(chan struct{})
	go func() {
		defer close(closerDone)
		<-ctx.Done()
		_ = conn.Close()
	}()
	defer func() {
		_ = conn.Close()
		<-closerDone
	}()

	// readErrBackoff guards against a wedged socket spinning the loop at
	// full speed. On every successful read it resets to zero; on a non-ctx
	// error it grows from 10ms to a 1s ceiling.
	const readErrBackoffCap = 1 * time.Second
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
			stats.UDPReadErrors.Add(1)
			log.Warn("read error", "scheme", la.scheme, "err", err)
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

		// Copy bytes off the pooled buffer so the reader can reuse it
		// immediately on the next iteration.
		payload := make([]byte, n)
		copy(payload, buf[:n])
		readerPool.Put(bp)

		dg := rawDatagram{bytes: payload, at: time.Now()}
		select {
		case out <- dg:
		case <-ctx.Done():
			return nil
		default:
			// Non-blocking send: drop newest when the batcher is saturated.
			stats.DropsQueueFull.Add(1)
		}
	}
}

// bindListener opens the bind socket and returns it alongside a cleanup
// hook. The cleanup runs after the read loop exits — for UDS it removes the
// socket file so a restart isn't blocked by EADDRINUSE.
func bindListener(la listenAddr, log *slog.Logger, stats *selfStats) (net.PacketConn, func(), error) {
	switch la.scheme {
	case "udp":
		udpAddr, err := net.ResolveUDPAddr("udp", la.UDPAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve udp addr: %w", err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("listen udp: %w", err)
		}
		if err := conn.SetReadBuffer(readBufBytes); err != nil {
			stats.UDPBufferDegraded.Store(true)
			log.Warn("set udp read buffer failed", "err", err)
		}
		return conn, func() {}, nil

	case "unix":
		path := la.UnixPath
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			// Best-effort: parent dir is usually pre-created by the
			// operator (a mounted volume or tmpfs). If it isn't, fall
			// through to ListenUnixgram so the underlying error surfaces
			// with the right context.
			_ = os.MkdirAll(dir, 0o755)
		}
		// Stale socket files survive ungraceful shutdowns. ListenUnixgram
		// would return EADDRINUSE; remove first so restarts are
		// idempotent. Only unlink existing *socket* nodes — never a
		// regular file the operator might have placed there.
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
			stats.UDPBufferDegraded.Store(true)
			log.Warn("set unix read buffer failed", "err", err)
		}
		// The agent runs as a sidecar shared with the app process, which
		// commonly runs as a different uid (e.g. www-data). 0666 keeps the
		// "fire and forget" contract working without forcing operators to
		// match uids. If you need stricter perms, set them on the parent
		// directory instead.
		if err := os.Chmod(path, 0o666); err != nil {
			log.Warn("chmod unix socket failed", "path", path, "err", err)
		}

		cleanup := func() {
			// Best-effort: we created this file, we remove it. If the
			// operator replaced it mid-flight (very unusual), the
			// stat-then-unlink dance would race; we accept that and just
			// unlink unconditionally — the worst case is removing a
			// replacement socket that an operator just put in place,
			// which would be detected immediately by their tooling.
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Warn("unix socket cleanup failed", "path", path, "err", err)
			}
		}
		return conn, cleanup, nil

	default:
		return nil, nil, fmt.Errorf("unknown listen scheme %q", la.scheme)
	}
}
