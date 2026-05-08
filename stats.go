package main

import "sync/atomic"

// selfStats holds counters describing the agent's own behavior. Exposed via
// the health endpoint so operators can see drops, parse failures, and flush
// outcomes without scraping logs. All fields are accessed via atomics so any
// goroutine can update them on its hot path without locks.
type selfStats struct {
	UDPPacketsReceived atomic.Uint64
	MetricsParsed      atomic.Uint64
	ParseErrors        atomic.Uint64
	UDPReadErrors      atomic.Uint64
	MetricsDropped     atomic.Uint64
	FlushesOK          atomic.Uint64
	FlushesFailed      atomic.Uint64
	MetricsFlushed     atomic.Uint64
	LastFlushUnix      atomic.Int64
}

func newSelfStats() *selfStats { return &selfStats{} }

type statsSnapshot struct {
	UDPPacketsReceived uint64 `json:"udp_packets_received"`
	MetricsParsed      uint64 `json:"metrics_parsed"`
	ParseErrors        uint64 `json:"parse_errors"`
	UDPReadErrors      uint64 `json:"udp_read_errors"`
	MetricsDropped     uint64 `json:"metrics_dropped"`
	FlushesOK          uint64 `json:"flushes_ok"`
	FlushesFailed      uint64 `json:"flushes_failed"`
	MetricsFlushed     uint64 `json:"metrics_flushed"`
	LastFlushUnix      int64  `json:"last_flush_unix"`
}

func (s *selfStats) snapshot() statsSnapshot {
	return statsSnapshot{
		UDPPacketsReceived: s.UDPPacketsReceived.Load(),
		MetricsParsed:      s.MetricsParsed.Load(),
		ParseErrors:        s.ParseErrors.Load(),
		UDPReadErrors:      s.UDPReadErrors.Load(),
		MetricsDropped:     s.MetricsDropped.Load(),
		FlushesOK:          s.FlushesOK.Load(),
		FlushesFailed:      s.FlushesFailed.Load(),
		MetricsFlushed:     s.MetricsFlushed.Load(),
		LastFlushUnix:      s.LastFlushUnix.Load(),
	}
}
