package main

import (
	"context"
	"log/slog"
	"net"
	"strings"
)

// readBufBytes is the kernel-side UDP receive buffer. Setting it large lets
// short bursts queue in the kernel rather than being dropped at the NIC/socket
// boundary; the agent's own goroutine drains it in a tight loop.
const readBufBytes = 8 << 20 // 8MB

// listen reads UDP datagrams on addr, splits them into newline-separated
// statsd lines, parses each, and ships parsed metrics down `out`.
//
// On context cancel it closes the socket (which unblocks ReadFromUDP) and
// returns once the read loop exits.
func listen(ctx context.Context, addr string, out chan<- Metric, log *slog.Logger, stats *selfStats) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	if err := conn.SetReadBuffer(readBufBytes); err != nil {
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
		// Ensure the close-on-cancel goroutine exits even when listen returns
		// for non-ctx reasons (e.g., a permanent read error).
		_ = conn.Close()
		<-closerDone
	}()

	// Datagrams over loopback can be up to 64K. We size for headroom but
	// real DogStatsD clients send <1500B; this is just defensive.
	buf := make([]byte, 64*1024)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			stats.UDPReadErrors.Add(1)
			log.Warn("udp read error", "err", err)
			continue
		}
		stats.UDPPacketsReceived.Add(1)
		// One datagram may carry many \n-separated metrics.
		payload := string(buf[:n])
		for payload != "" {
			var line string
			if i := strings.IndexByte(payload, '\n'); i >= 0 {
				line, payload = payload[:i], payload[i+1:]
			} else {
				line, payload = payload, ""
			}
			line = strings.TrimRight(line, "\r")
			if line == "" {
				continue
			}
			m, err := parseLine(line)
			if err != nil {
				stats.ParseErrors.Add(1)
				log.Debug("malformed metric", "line", line)
				continue
			}
			stats.MetricsParsed.Add(1)
			// Don't deadlock if the aggregator is gone during shutdown.
			select {
			case out <- m:
			case <-ctx.Done():
				return nil
			}
		}
	}
}
