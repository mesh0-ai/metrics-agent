package main

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"
)

// readBufBytes is the kernel-side UDP receive buffer. Setting it large lets
// short bursts queue in the kernel rather than being dropped at the NIC/socket
// boundary; the agent's own goroutine drains it in a tight loop.
const readBufBytes = 8 << 20 // 8MB

// readerPool holds 64K scratch buffers reused across ReadFromUDP calls. The
// listener never hands the pooled buffer itself downstream — bytes are copied
// into a fresh slice before dispatch so the pooled buffer can be reused
// immediately.
var readerPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// listen reads UDP datagrams on addr and forwards each as a rawDatagram to
// `out`. Every datagram is treated as one candidate JSON event; structural
// validation happens downstream in the batcher.
//
// The listener never blocks on a full channel — it drops the newest datagram
// and increments drops.queue_full so a slow flusher cannot stall the read
// loop. On context cancel it closes the socket (which unblocks ReadFromUDP)
// and returns once the read loop exits.
func listen(ctx context.Context, addr string, out chan<- rawDatagram, log *slog.Logger, stats *selfStats) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	if err := conn.SetReadBuffer(readBufBytes); err != nil {
		stats.UDPBufferDegraded.Store(true)
		log.Warn("set udp read buffer failed", "err", err)
	}
	log.Info("udp listener started", "addr", conn.LocalAddr().String())

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
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			readerPool.Put(bp)
			if ctx.Err() != nil {
				return nil
			}
			stats.UDPReadErrors.Add(1)
			log.Warn("udp read error", "err", err)
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
