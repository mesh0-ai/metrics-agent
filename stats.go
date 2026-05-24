package main

import (
	"sync/atomic"
	"time"
)

// selfStats holds counters describing the agent's own behavior. Exposed via
// the health endpoint so operators can see drops and flush outcomes without
// scraping logs. All fields are accessed via atomics so any goroutine can
// update them on its hot path without locks.
type selfStats struct {
	EventsReceived   atomic.Uint64
	EventsSent       atomic.Uint64
	BatchesSent      atomic.Uint64
	DropsParseError  atomic.Uint64
	DropsQueueFull   atomic.Uint64
	DropsOversize    atomic.Uint64
	DropsFlushFailed atomic.Uint64
	DropsShutdown    atomic.Uint64
	// DropsUnroutedMissing counts datagrams dropped because no `_project`
	// was set and no DefaultProject pipeline is registered (multi-tenant
	// keys file only, MESH0_API_KEY unset).
	DropsUnroutedMissing atomic.Uint64
	// DropsUnroutedUnknown counts datagrams whose `_project` does not
	// match any registered pipeline.
	DropsUnroutedUnknown atomic.Uint64
	ReadErrors           atomic.Uint64
	BufferDegraded   atomic.Bool  // kernel rejected the requested SO_RCVBUF
	ListenerFatal    atomic.Bool  // listener goroutine exited unexpectedly
	LastEventFlushMs atomic.Int64 // unix-millis of last successful event flush

	startUnix int64
}

func newSelfStats() *selfStats {
	return &selfStats{startUnix: time.Now().Unix()}
}

// statsSnapshot is the JSON shape of GET /stats.
type statsSnapshot struct {
	EventsReceived uint64                          `json:"events_received"`
	EventsDropped  dropStats                       `json:"events_dropped"`
	BatchesSent    uint64                          `json:"batches_sent"`
	EventsSent     uint64                          `json:"events_sent"`
	LastFlushAgeMs int64                           `json:"last_flush_age_ms"`
	ReadErrors     uint64                          `json:"read_errors"`
	BufferDegraded bool                            `json:"buffer_degraded"`
	ListenerFatal  bool                            `json:"listener_fatal"`
	UptimeS        int64                           `json:"uptime_s"`
	ByProject      map[string]projectStatsSnapshot `json:"by_project,omitempty"`
}

type dropStats struct {
	ParseError       uint64 `json:"parse_error"`
	QueueFull        uint64 `json:"queue_full"`
	Oversize         uint64 `json:"oversize"`
	FlushFailed      uint64 `json:"flush_failed"`
	Shutdown         uint64 `json:"shutdown"`
	UnroutedMissing  uint64 `json:"unrouted_missing_project"`
	UnroutedUnknown  uint64 `json:"unrouted_unknown_project"`
}

// pipelineStats tracks the per-project subset of counters. Sums across all
// pipelines must equal the process-wide totals on selfStats (modulo race
// windows). Per-project visibility lets operators attribute drops to a
// specific tenant without log-scraping.
type pipelineStats struct {
	EventsReceived   atomic.Uint64
	EventsSent       atomic.Uint64
	BatchesSent      atomic.Uint64
	DropsParseError  atomic.Uint64
	DropsQueueFull   atomic.Uint64
	DropsOversize    atomic.Uint64
	DropsFlushFailed atomic.Uint64
	DropsShutdown    atomic.Uint64
	LastEventFlushMs atomic.Int64
}

func newPipelineStats() *pipelineStats { return &pipelineStats{} }

type projectStatsSnapshot struct {
	EventsReceived uint64    `json:"events_received"`
	EventsSent     uint64    `json:"events_sent"`
	BatchesSent    uint64    `json:"batches_sent"`
	EventsDropped  dropStats `json:"events_dropped"`
	LastFlushAgeMs int64     `json:"last_flush_age_ms"`
}

func (s *pipelineStats) snapshot() projectStatsSnapshot {
	lastMs := s.LastEventFlushMs.Load()
	var ageMs int64
	if lastMs > 0 {
		ageMs = time.Now().UnixMilli() - lastMs
	}
	return projectStatsSnapshot{
		EventsReceived: s.EventsReceived.Load(),
		EventsSent:     s.EventsSent.Load(),
		BatchesSent:    s.BatchesSent.Load(),
		EventsDropped: dropStats{
			ParseError:  s.DropsParseError.Load(),
			QueueFull:   s.DropsQueueFull.Load(),
			Oversize:    s.DropsOversize.Load(),
			FlushFailed: s.DropsFlushFailed.Load(),
			Shutdown:    s.DropsShutdown.Load(),
		},
		LastFlushAgeMs: ageMs,
	}
}

func (s *selfStats) snapshot() statsSnapshot {
	now := time.Now()
	lastMs := s.LastEventFlushMs.Load()
	var ageMs int64
	if lastMs > 0 {
		ageMs = now.UnixMilli() - lastMs
	}
	return statsSnapshot{
		EventsReceived: s.EventsReceived.Load(),
		EventsDropped: dropStats{
			ParseError:      s.DropsParseError.Load(),
			QueueFull:       s.DropsQueueFull.Load(),
			Oversize:        s.DropsOversize.Load(),
			FlushFailed:     s.DropsFlushFailed.Load(),
			Shutdown:        s.DropsShutdown.Load(),
			UnroutedMissing: s.DropsUnroutedMissing.Load(),
			UnroutedUnknown: s.DropsUnroutedUnknown.Load(),
		},
		BatchesSent:    s.BatchesSent.Load(),
		EventsSent:     s.EventsSent.Load(),
		LastFlushAgeMs: ageMs,
		ReadErrors:     s.ReadErrors.Load(),
		BufferDegraded: s.BufferDegraded.Load(),
		ListenerFatal:  s.ListenerFatal.Load(),
		UptimeS:        now.Unix() - s.startUnix,
	}
}
