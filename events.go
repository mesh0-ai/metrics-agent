package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"
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
	// token is the per-datagram credential recovered from `_token`, empty
	// when the datagram carried none. Deployments that publish a keys file
	// leave this empty and authenticate from the pipeline's configured key.
	token string
	// events is set (and bytes nil) for a batch datagram (batch.go): its
	// events, routing members stripped, each already valid JSON. It travels
	// through the shared queue and the handoff as ONE item, so a batch costs
	// one channel slot, and every drop of it is counted eventCount() times.
	events [][]byte
}

// EventBatch is the unit handed to the events flusher. The Events slice is a
// list of pre-validated json.RawMessage blobs that will be reassembled as
// `{"events":[...]}` on the wire.
type EventBatch struct {
	Events    []json.RawMessage
	StartedAt time.Time
	// Token authorizes THIS batch, when its datagrams carried one. Empty
	// means "use the flusher's configured key" — the keys-file path.
	//
	// 🛑 THE CREDENTIAL TRAVELS WITH THE EVENTS IT AUTHORIZES, rather than
	// being read off the pipeline at POST time. A pipeline's token rotates
	// (~every 10 minutes for a Xano instance), and a batch assembled before
	// a rotation but flushed after it would otherwise be sent under a
	// credential minted for a different moment. Both are valid for the same
	// project, so this is not a correctness bug today — but it makes the
	// batch self-describing, which is what lets a retry hours later still
	// carry the credential that was current when the events were accepted.
	Token string
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
	return validateEventBytes(b, maxBytes, true)
}

// validateTrustedEvent is validateEvent for bytes already known to be one
// valid JSON value (a batch datagram's events): it skips json.Valid, the one
// step that re-reads the whole event, and keeps every other check.
func validateTrustedEvent(b []byte, maxBytes int) (json.RawMessage, validateReason) {
	return validateEventBytes(b, maxBytes, false)
}

func validateEventBytes(b []byte, maxBytes int, checkSyntax bool) (json.RawMessage, validateReason) {
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
	if checkSyntax && !json.Valid(trimmed) {
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

// hasTopLevelKey reports whether the JSON object b has a top-level member
// named key (compared after unescaping, as encoding/json would). b must be
// valid JSON: both callers check that first (validateEvent runs json.Valid;
// a batch datagram's events come out of a body json.Decoder accepted).
//
// It walks the object with the routing layer's allocation-free scanner
// (scanWS/scanValue) instead of a json.Decoder, which boxed every top-level
// token and decoded every value into a fresh RawMessage once per event on
// the batcher path. Anything the scanner does not accept (nesting deeper
// than maxScanDepth, say) falls back to the decoder, so the answer never
// changes; FuzzHasTopLevelKey pins the two against each other.
func hasTopLevelKey(b []byte, key string) bool {
	if found, ok := scanTopLevelKey(b, key); ok {
		return found
	}
	return decodeTopLevelKey(b, key)
}

// scanTopLevelKey is hasTopLevelKey's fast path. ok=false means the scanner
// could not walk b and the caller must ask the decoder.
func scanTopLevelKey(b []byte, key string) (found, ok bool) {
	i := scanWS(b, 0)
	if i >= len(b) || b[i] != '{' {
		return false, false
	}
	i = scanWS(b, i+1)
	if i < len(b) && b[i] == '}' {
		return false, true
	}
	for {
		if i >= len(b) || b[i] != '"' {
			return false, false
		}
		closeQuote, sok := scanStringBody(b, i+1)
		if !sok {
			return false, false
		}
		if keyEquals(b[i:closeQuote+1], key) {
			return true, true
		}
		i = scanWS(b, closeQuote+1)
		if i >= len(b) || b[i] != ':' {
			return false, false
		}
		end, _, vok := scanValue(b, scanWS(b, i+1))
		if !vok {
			return false, false
		}
		i = scanWS(b, end)
		if i >= len(b) {
			return false, false
		}
		switch b[i] {
		case ',':
			i = scanWS(b, i+1)
		case '}':
			return false, true
		default:
			return false, false
		}
	}
}

// keyEquals compares a quoted JSON key (quotes included) with key after
// unescaping. The common case, no escapes and valid UTF-8, compares the raw
// bytes without allocating; anything else is unquoted exactly as
// encoding/json would.
func keyEquals(quoted []byte, key string) bool {
	inner := quoted[1 : len(quoted)-1]
	if bytes.IndexByte(inner, '\\') < 0 && utf8.Valid(inner) {
		return string(inner) == key
	}
	s, ok := unquoteJSONString(quoted)
	return ok && s == key
}

// decodeTopLevelKey is the json.Decoder walk hasTopLevelKey used before the
// scanner; it remains the fallback.
func decodeTopLevelKey(b []byte, key string) bool {
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
	curToken  string
	firstSeen time.Time

	// The window timer. Only run() and the methods it calls touch these.
	timer  *time.Timer
	timerC <-chan time.Time
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
	for {
		select {
		case dg, ok := <-b.in:
			if !ok {
				b.flush()
				return
			}
			if dg.events != nil {
				// A batch datagram: its events were decoded out of a body
				// json.Decoder accepted, so each is already valid JSON and
				// skips the json.Valid pass. Each is still checked, counted
				// and batched on its own, so MaxBatch, the byte cap and the
				// drop counters behave exactly as for single datagrams.
				for _, ev := range dg.events {
					b.accept(ev, dg, true)
				}
				continue
			}
			b.accept(dg.bytes, dg, false)
		case <-b.timerC:
			b.flush()
			b.disarmTimer()
		}
	}
}

// accept validates one event and appends it to the current batch, flushing
// on the size and byte caps. trusted skips the json.Valid pass for bytes
// already known to be one valid JSON value.
func (b *eventsBatcher) accept(raw []byte, dg rawDatagram, trusted bool) {
	var ev json.RawMessage
	var reason validateReason
	if trusted {
		ev, reason = validateTrustedEvent(raw, b.maxEventBytes)
	} else {
		ev, reason = validateEvent(raw, b.maxEventBytes)
	}
	switch reason {
	case validateOversize:
		b.stats.DropsOversize.Add(1)
		if b.pipelineStats != nil {
			b.pipelineStats.DropsOversize.Add(1)
		}
		return
	case validateParseError:
		b.stats.DropsParseError.Add(1)
		if b.pipelineStats != nil {
			b.pipelineStats.DropsParseError.Add(1)
		}
		return
	}
	if b.pipelineStats != nil {
		b.pipelineStats.EventsReceived.Add(1)
	}
	if len(b.cur) > 0 && b.curBytes+len(ev)+1 > MaxBatchBytes {
		b.flush()
		b.disarmTimer()
	}
	if len(b.cur) == 0 {
		b.firstSeen = dg.at
		b.armTimer(b.window)
	}
	b.cur = append(b.cur, ev)
	b.curBytes += len(ev) + 1 // comma between elements
	// Last-wins. A rotation mid-batch leaves the batch carrying the
	// FRESHER credential, which is the one with more remaining life
	// against the gateway's clock — the direction that matters when
	// the flusher retries with backoff.
	if dg.token != "" {
		b.curToken = dg.token
	}
	if len(b.cur) >= b.maxEvents {
		b.flush()
		b.disarmTimer()
	}
}

func (b *eventsBatcher) armTimer(d time.Duration) {
	if d <= 0 {
		d = time.Microsecond
	}
	if b.timer == nil {
		b.timer = time.NewTimer(d)
	} else {
		if !b.timer.Stop() {
			select {
			case <-b.timer.C:
			default:
			}
		}
		b.timer.Reset(d)
	}
	b.timerC = b.timer.C
}

func (b *eventsBatcher) disarmTimer() {
	b.timerC = nil
	if b.timer != nil && !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
}

func (b *eventsBatcher) flush() {
	if len(b.cur) == 0 {
		return
	}
	batch := EventBatch{Events: b.cur, StartedAt: b.firstSeen, Token: b.curToken}
	b.cur = nil
	b.curBytes = 0
	b.curToken = ""
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
