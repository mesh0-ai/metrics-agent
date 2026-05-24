package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// DefaultMaxEventBytes is the default per-datagram size ceiling for a JSON
// event. Operators can override via MESH0_MAX_EVENT_BYTES; anything larger
// than the configured value is dropped (drops.oversize). 1 MB is sized for
// typical traces plus modest log lines; outliers should still chunk.
const DefaultMaxEventBytes = 1 * 1024 * 1024

// MinMaxEventBytes / MaxMaxEventBytes bound the configurable knob. The lower
// bound keeps validateEvent meaningful; the upper bound caps worst-case
// queue memory at MESH0_QUEUE_SIZE * MaxMaxEventBytes.
const (
	MinMaxEventBytes = 1024
	MaxMaxEventBytes = 16 * 1024 * 1024
)

// MaxEventsPerBatch is the absolute server-side ceiling. The configurable
// MESH0_MAX_BATCH must not exceed this.
const MaxEventsPerBatch = 5000

// MaxBatchBytes is the server-side body limit (10 MB). The batcher will
// pre-flush if appending the next event would push us past this. With
// max_event_bytes near MaxBatchBytes batches degrade to one event each
// (expected, not a bug); the effective per-event cap is
// min(max_event_bytes, MaxBatchBytes), since a single event larger than
// MaxBatchBytes would produce a one-event batch the gateway 413s.
const MaxBatchBytes = 10 * 1024 * 1024

// rawDatagram is one UDS-DGRAM payload destined for the JSON event path. The bytes
// are owned by this struct (already copied off the listener's pooled read
// buffer) — receivers can keep them.
//
// project is stamped by the routing layer's dispatch (after _project extraction)
// so the demuxer goroutine can route the datagram to its pipeline without
// re-parsing. An empty project means the datagram routes to DefaultProject.
type rawDatagram struct {
	bytes   []byte
	at      time.Time
	project string
}

// EventBatch is the unit handed to the events flusher. The Events slice is a
// list of pre-validated json.RawMessage blobs that will be reassembled as
// `{"events":[...]}` on the wire.
type EventBatch struct {
	Events    []json.RawMessage
	StartedAt time.Time
}

type validateReason int

const (
	validateOK validateReason = iota
	validateOversize
	validateParseError
)

// validateEvent does structural validation only: must be a JSON object, must
// fit under maxBytes. Stamps a timestamp if absent. Returns the
// (possibly-rewritten) event bytes and a reason code; on validateOK the
// returned slice is non-nil and ready to append to a batch.
func validateEvent(b []byte, maxBytes int) (json.RawMessage, validateReason) {
	if len(b) > maxBytes {
		return nil, validateOversize
	}
	if len(b) == 0 {
		return nil, validateParseError
	}
	start := 0
	for start < len(b) && isJSONSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isJSONSpace(b[end-1]) {
		end--
	}
	if end-start < 2 || b[start] != '{' || b[end-1] != '}' {
		return nil, validateParseError
	}
	trimmed := b[start:end]
	if !json.Valid(trimmed) {
		return nil, validateParseError
	}
	if !hasTopLevelKey(trimmed, "timestamp") {
		ts, err := json.Marshal(time.Now().UTC().Format(time.RFC3339Nano))
		if err == nil {
			trimmed = injectTopLevelField(trimmed, `"timestamp":`, ts)
		}
	}
	return json.RawMessage(trimmed), validateOK
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// hasTopLevelKey decodes the object with json.Decoder so we correctly skip
// nested objects and strings.
func hasTopLevelKey(b []byte, key string) bool {
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil {
		return false
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return false
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return false
		}
		if ks, ok := k.(string); ok && ks == key {
			return true
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return false
		}
	}
	return false
}

// injectTopLevelField inserts `<keyColon><value>` as the first member of the
// object. Caller guarantees obj starts with '{' and ends with '}'.
func injectTopLevelField(obj []byte, keyColon string, value []byte) []byte {
	out := make([]byte, 0, len(obj)+len(keyColon)+len(value)+2)
	out = append(out, '{')
	out = append(out, keyColon...)
	out = append(out, value...)
	inner := bytes.TrimSpace(obj[1 : len(obj)-1])
	if len(inner) > 0 {
		out = append(out, ',')
		out = append(out, inner...)
	}
	out = append(out, '}')
	return out
}

// eventsBatcher owns a single in-flight batch. Goroutine-affine: only run()
// touches Events. Flush triggers: size cap, byte cap, or window timeout
// since the first queued event in the current batch.
type eventsBatcher struct {
	in            <-chan rawDatagram
	out           chan<- EventBatch
	stats         *selfStats
	pipelineStats *pipelineStats // optional; nil when not multi-tenant routed
	log           *slog.Logger
	maxEvents     int
	maxEventBytes int
	window        time.Duration
	// ctx is checked when sending a batch downstream so a wedged flusher
	// cannot deadlock the shutdown drain. nil is treated as never-cancelled.
	ctx context.Context

	cur       []json.RawMessage
	curBytes  int
	firstSeen time.Time
}

func newEventsBatcher(in <-chan rawDatagram, out chan<- EventBatch, stats *selfStats, log *slog.Logger, maxEvents, maxEventBytes int, window time.Duration) *eventsBatcher {
	if maxEvents <= 0 || maxEvents > MaxEventsPerBatch {
		maxEvents = MaxEventsPerBatch
	}
	if maxEventBytes <= 0 {
		maxEventBytes = DefaultMaxEventBytes
	}
	if window <= 0 {
		window = 200 * time.Millisecond
	}
	return &eventsBatcher{
		in:            in,
		out:           out,
		stats:         stats,
		log:           log,
		maxEvents:     maxEvents,
		maxEventBytes: maxEventBytes,
		window:        window,
	}
}

func (b *eventsBatcher) run() {
	var timer *time.Timer
	var timerC <-chan time.Time

	armTimer := func(d time.Duration) {
		if d <= 0 {
			d = time.Microsecond
		}
		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		}
		timerC = timer.C
	}
	disarmTimer := func() {
		timerC = nil
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		select {
		case dg, ok := <-b.in:
			if !ok {
				b.flush()
				return
			}
			ev, reason := validateEvent(dg.bytes, b.maxEventBytes)
			switch reason {
			case validateOversize:
				b.stats.DropsOversize.Add(1)
				if b.pipelineStats != nil {
					b.pipelineStats.DropsOversize.Add(1)
				}
				continue
			case validateParseError:
				b.stats.DropsParseError.Add(1)
				if b.pipelineStats != nil {
					b.pipelineStats.DropsParseError.Add(1)
				}
				continue
			}
			if b.pipelineStats != nil {
				b.pipelineStats.EventsReceived.Add(1)
			}
			if len(b.cur) > 0 && b.curBytes+len(ev)+1 > MaxBatchBytes {
				b.flush()
				disarmTimer()
			}
			if len(b.cur) == 0 {
				b.firstSeen = dg.at
				armTimer(b.window)
			}
			b.cur = append(b.cur, ev)
			b.curBytes += len(ev) + 1 // comma between elements
			if len(b.cur) >= b.maxEvents {
				b.flush()
				disarmTimer()
			}
		case <-timerC:
			b.flush()
			disarmTimer()
		}
	}
}

func (b *eventsBatcher) flush() {
	if len(b.cur) == 0 {
		return
	}
	batch := EventBatch{Events: b.cur, StartedAt: b.firstSeen}
	b.cur = nil
	b.curBytes = 0
	b.firstSeen = time.Time{}
	if b.ctx == nil {
		b.out <- batch
		return
	}
	select {
	case b.out <- batch:
	case <-b.ctx.Done():
		// Flusher is gone or the grace timer fired during a wedged POST.
		// Account for the dropped events instead of blocking forever.
		b.stats.DropsShutdown.Add(uint64(len(batch.Events)))
		if b.pipelineStats != nil {
			b.pipelineStats.DropsShutdown.Add(uint64(len(batch.Events)))
		}
		b.log.Warn("batcher abandoning batch on shutdown",
			"events", len(batch.Events))
	}
}

// encodeEventBatch concatenates the wrapping object without re-marshalling
// the events. Reuses a pooled buffer.
var (
	eventsPrefix = []byte(`{"events":[`)
	eventsSuffix = []byte(`]}`)
	encBufPool   = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

func encodeEventBatch(batch EventBatch) []byte {
	buf := encBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer encBufPool.Put(buf)
	buf.Write(eventsPrefix)
	for i, ev := range batch.Events {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(ev)
	}
	buf.Write(eventsSuffix)
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}
