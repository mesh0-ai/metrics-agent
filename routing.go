package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

// DefaultProject is the sentinel name the single-key (MESH0_API_KEY) path is
// registered under. Datagrams without a `_project` field route here when it
// is the only registered project, or when MESH0_KEYS_FILE is also set (the
// API key is the fallback for unrouted datagrams).
const DefaultProject = "_default"

// pipeline holds the per-project channels + goroutines that drain a single
// project's events to the gateway. One pipeline per registered API key.
//
// Each pipeline is independently rate-limited (its own bounded queue) and
// independently observable (its own pipelineStats). A wedged project cannot
// stall another.
type pipeline struct {
	project string
	apiKey  string

	rawCh   chan rawDatagram
	batchCh chan EventBatch

	batcher *eventsBatcher
	flusher *eventsFlusher

	stats *pipelineStats

	// ctx is the pipeline's send/flush context. Cancel to abort in-flight
	// batches (used by the global shutdown grace timer + per-pipeline drain).
	ctx    context.Context
	cancel context.CancelFunc

	batcherDone chan struct{}
	flushDone   chan struct{}

	// sendMu guards rawCh against concurrent send-after-close. dispatch
	// takes RLock for the send (multiple senders coexist); drain takes
	// Lock to flip closed and close(rawCh) under exclusion. This is the
	// canonical fix for the send-on-closed-channel panic that's otherwise
	// possible when reload swaps a pipeline while a dispatch is in flight.
	sendMu sync.RWMutex
	closed bool
}

// trySend delivers dg to the pipeline's input queue without blocking. At
// most one of (closed, queueFull) is true on a non-delivered send:
//   - closed=true means the pipeline has been drained (SIGHUP reload retired
//     it, or shutdown is in progress). Caller accounts as drops.routing_closed
//     — not queue_full, since the queue may have had capacity.
//   - queueFull=true means the queue rejected the send under back-pressure.
func (p *pipeline) trySend(dg rawDatagram) (delivered, closed, queueFull bool) {
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	if p.closed {
		return false, true, false
	}
	select {
	case p.rawCh <- dg:
		return true, false, false
	default:
		return false, false, true
	}
}

// newPipeline wires a project's batcher + flusher with the supplied config.
// Caller must invoke start() to launch the goroutines.
func newPipeline(project, apiKey string, cfg Config, log *slog.Logger, processStats *selfStats) *pipeline {
	pstats := newPipelineStats()
	rawCh := make(chan rawDatagram, cfg.QueueSize)
	batchCh := make(chan EventBatch, 8)

	plog := log.With("project", project)

	b := newEventsBatcher(rawCh, batchCh, processStats, plog, cfg.MaxBatch, cfg.MaxEventBytes, cfg.BatchWindow)
	b.pipelineStats = pstats

	f := newEventsFlusherWithKey(batchCh, cfg, apiKey, plog, processStats)
	f.pipelineStats = pstats

	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	f.ctx = ctx

	return &pipeline{
		project:     project,
		apiKey:      apiKey,
		rawCh:       rawCh,
		batchCh:     batchCh,
		batcher:     b,
		flusher:     f,
		stats:       pstats,
		ctx:         ctx,
		cancel:      cancel,
		batcherDone: make(chan struct{}),
		flushDone:   make(chan struct{}),
	}
}

func (p *pipeline) start() {
	go func() {
		p.batcher.run()
		close(p.batcherDone)
	}()
	go func() {
		p.flusher.run()
		close(p.flushDone)
	}()
}

// drain closes the pipeline's input channel, waits for the batcher to emit
// its final partial batch, then waits for the flusher to drain. If grace > 0
// the pipeline context is cancelled after that duration so a wedged POST
// cannot prevent shutdown; if grace == 0 the context is cancelled immediately
// (best-effort flush, no wait).
func (p *pipeline) drain(grace time.Duration) {
	p.sendMu.Lock()
	p.closed = true
	close(p.rawCh)
	p.sendMu.Unlock()

	var graceTimer *time.Timer
	if grace > 0 {
		graceTimer = time.AfterFunc(grace, p.cancel)
	} else {
		p.cancel()
	}
	<-p.batcherDone
	close(p.batchCh)
	<-p.flushDone
	if graceTimer != nil {
		graceTimer.Stop()
	}
	p.cancel()
}

// routingTable is an immutable snapshot of project → pipeline mappings.
// Replaced atomically on SIGHUP so the listener's hot path is lock-free.
type routingTable struct {
	pipelines map[string]*pipeline
	// hasDefault is true when DefaultProject is registered. Cached so the
	// empty-`_project` fast path skips a map lookup.
	hasDefault bool
}

// registry owns the live routing table plus the inputs needed to reload it.
// Methods are safe for concurrent use.
type registry struct {
	cur atomic.Pointer[routingTable]

	cfg          Config
	log          *slog.Logger
	processStats *selfStats

	// reloadMu serialises SIGHUP handlers so two concurrent signals cannot
	// produce diverging tables. Read path (lookup) is lock-free via
	// atomic.Pointer.
	reloadMu sync.Mutex

	// drainsInFlight tracks reload-initiated drain goroutines so shutdown
	// can wait on them and they aren't orphaned if SIGHUP races SIGTERM.
	drainsInFlight sync.WaitGroup

	// KeysReloadFailures counts SIGHUP reloads that gave up after the
	// retry (the previous routing table is kept). Surfaced in /stats so
	// operators can detect a stale routing table without scraping logs.
	KeysReloadFailures atomic.Uint64
	// LastKeysReloadUnix is the unix-seconds timestamp of the most recent
	// successful keys-file reload (0 if no reload has succeeded yet).
	LastKeysReloadUnix atomic.Int64
}

func newRegistry(cfg Config, log *slog.Logger, processStats *selfStats) *registry {
	return &registry{cfg: cfg, log: log, processStats: processStats}
}

// install seeds the registry from the initial config (MESH0_API_KEY +
// MESH0_KEYS_FILE) and starts every pipeline. Returns an error if the keys
// file is set but unreadable/unparseable — initial startup is fail-fast on
// config errors so operators see the problem immediately.
func (r *registry) install() error {
	keys := map[string]string{}
	if r.cfg.APIKey != "" {
		keys[DefaultProject] = r.cfg.APIKey
	}
	if r.cfg.KeysFile != "" {
		fileKeys, err := loadKeysFile(r.cfg.KeysFile)
		if err != nil {
			return fmt.Errorf("read MESH0_KEYS_FILE: %w", err)
		}
		for k, v := range fileKeys {
			keys[k] = v
		}
	}
	if len(keys) == 0 {
		return errors.New("no API keys configured (set MESH0_API_KEY or MESH0_KEYS_FILE)")
	}
	if r.cfg.MaxProjects > 0 && len(keys) > r.cfg.MaxProjects {
		return fmt.Errorf("registered projects (%d) exceed MESH0_MAX_PROJECTS (%d); each pipeline costs ~QueueSize*MaxEventBytes of in-flight memory", len(keys), r.cfg.MaxProjects)
	}
	tbl := r.buildTable(keys, nil)
	// Start pipelines BEFORE publishing the table so dispatch can never
	// land a datagram on a pipeline whose goroutines aren't running yet.
	for _, p := range tbl.pipelines {
		p.start()
	}
	r.cur.Store(tbl)
	r.log.Info("routing installed",
		"projects", sortedKeys(tbl.pipelines),
		"has_default", tbl.hasDefault,
	)
	return nil
}

// reload re-reads the keys file and diffs against the current table. Added
// projects get fresh pipelines; removed projects are drained with the
// configured shutdown grace; projects whose key changed are replaced (drain
// old + start new). MESH0_API_KEY is process-lifetime — it never reloads.
//
// Best-effort: on parse error, the previous table is kept and an error is
// logged. The agent will not take itself down because an operator pushed a
// bad keys file.
func (r *registry) reload() {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	if r.cfg.KeysFile == "" {
		r.log.Warn("SIGHUP received but MESH0_KEYS_FILE is unset, nothing to reload")
		return
	}

	fileKeys, err := loadKeysFile(r.cfg.KeysFile)
	if err != nil {
		// One retry on parse error — a partial write that landed without
		// atomic rename can show up as transient garbage.
		r.log.Warn("keys file reload failed, retrying once", "err", err)
		time.Sleep(50 * time.Millisecond)
		fileKeys, err = loadKeysFile(r.cfg.KeysFile)
		if err != nil {
			r.KeysReloadFailures.Add(1)
			r.log.Error("keys file reload failed, keeping previous table", "err", err)
			return
		}
	}

	newKeys := map[string]string{}
	if r.cfg.APIKey != "" {
		newKeys[DefaultProject] = r.cfg.APIKey
	}
	for k, v := range fileKeys {
		newKeys[k] = v
	}

	if r.cfg.MaxProjects > 0 && len(newKeys) > r.cfg.MaxProjects {
		r.KeysReloadFailures.Add(1)
		r.log.Error("keys file reload rejected: too many projects, keeping previous table",
			"count", len(newKeys), "max_projects", r.cfg.MaxProjects)
		return
	}

	prev := r.cur.Load()
	tbl := r.buildTable(newKeys, prev)

	// Start any freshly-spawned pipelines BEFORE swapping the table, so
	// dispatch never lands on a pipeline whose goroutines aren't running.
	for name, p := range tbl.pipelines {
		if prev != nil {
			if old, ok := prev.pipelines[name]; ok && old == p {
				continue
			}
		}
		p.start()
	}

	r.cur.Store(tbl)

	// Drain pipelines that are gone or replaced. Track on a WaitGroup so
	// a SIGTERM that races with a reload doesn't orphan these goroutines
	// past the process exit (reg.shutdown waits on r.drainsInFlight).
	if prev != nil {
		for name, old := range prev.pipelines {
			if cur, ok := tbl.pipelines[name]; !ok || cur != old {
				r.drainsInFlight.Add(1)
				go func(p *pipeline) {
					defer r.drainsInFlight.Done()
					p.drain(r.cfg.ShutdownGrace)
				}(old)
			}
		}
	}

	r.LastKeysReloadUnix.Store(time.Now().Unix())
	r.log.Info("routing reloaded",
		"projects", sortedKeys(tbl.pipelines),
		"has_default", tbl.hasDefault,
	)
}

// buildTable produces a new routingTable that reuses pipelines from prev
// when the project's key is unchanged. Pipelines whose key changed are
// replaced (caller is responsible for draining the old one).
func (r *registry) buildTable(keys map[string]string, prev *routingTable) *routingTable {
	pipelines := make(map[string]*pipeline, len(keys))
	for name, key := range keys {
		if prev != nil {
			if old, ok := prev.pipelines[name]; ok && old.apiKey == key {
				pipelines[name] = old
				continue
			}
		}
		pipelines[name] = newPipeline(name, key, r.cfg, r.log, r.processStats)
	}
	_, hasDefault := pipelines[DefaultProject]
	return &routingTable{pipelines: pipelines, hasDefault: hasDefault}
}

// lookup resolves a datagram's project name to a pipeline. project may be
// empty (no `_project` on the wire); in that case the DefaultProject
// pipeline is used if registered, else the lookup misses.
func (r *registry) lookup(project string) (*pipeline, bool) {
	t := r.cur.Load()
	if t == nil {
		return nil, false
	}
	if project == "" {
		// RequireProject disables the DefaultProject fallback so multi-tenant
		// deployments can surface mis-tagged callers via unrouted_missing
		// rather than silently cross-attributing them to whichever tenant
		// owns MESH0_API_KEY.
		if t.hasDefault && !r.cfg.RequireProject {
			return t.pipelines[DefaultProject], true
		}
		return nil, false
	}
	p, ok := t.pipelines[project]
	return p, ok
}

// dispatch implements listenSink. Routes a datagram to its project's
// pipeline based on the `_project` field; strips `_project` so the gateway
// sees the original CustomEventInput shape. The listener has already bumped
// the process-wide EventsReceived counter; per-project EventsReceived is
// bumped by the batcher after validation. Drops route to:
//   - DropsParseError (process-wide) when the routing-strip pass finds the
//     payload is not a JSON object.
//   - DropsUnrouted{Missing,Unknown} (process-wide) for routing-layer drops,
//     including non-string `_project` values (treated as unknown — the JSON
//     is well-formed but unusable for routing).
//   - DropsQueueFull (per-pipeline and process-wide) for queue saturation.
//   - DropsRoutingClosed (per-pipeline and process-wide) when the destination
//     pipeline was drained between lookup and send (SIGHUP reload race).
//
// The listener also bumps process-wide DropsQueueFull when we return
// queueFull=true. Closed-pipeline drops do NOT signal queueFull — they
// would mis-attribute reload churn to back-pressure.
func (r *registry) dispatch(dg rawDatagram) (delivered bool, queueFull bool) {
	project, stripped, removed, malformed, badProject := extractAndStripProject(dg.bytes)
	if malformed {
		r.processStats.DropsParseError.Add(1)
		return false, false
	}
	if badProject {
		// `_project` was present but not a JSON string. JSON is structurally
		// valid; the routing intent is unusable. Account as unrouted_unknown
		// so an alert on that counter catches malformed client SDKs without
		// conflating with broken-JSON parse_error.
		r.processStats.DropsUnroutedUnknown.Add(1)
		return false, false
	}
	if removed {
		dg.bytes = stripped
	}
	p, ok := r.lookup(project)
	if !ok {
		if project == "" {
			r.processStats.DropsUnroutedMissing.Add(1)
		} else {
			r.processStats.DropsUnroutedUnknown.Add(1)
		}
		return false, false
	}
	delivered, closed, full := p.trySend(dg)
	switch {
	case delivered:
		return true, false
	case closed:
		p.stats.DropsRoutingClosed.Add(1)
		r.processStats.DropsRoutingClosed.Add(1)
		return false, false
	case full:
		p.stats.DropsQueueFull.Add(1)
		return false, true
	}
	return false, false
}

// shutdown drains every pipeline. Called from main on SIGINT/SIGTERM after
// the listener has stopped accepting new datagrams. Also waits for any
// reload-initiated drains so SIGHUP-then-SIGTERM doesn't orphan goroutines.
func (r *registry) shutdown(grace time.Duration) {
	t := r.cur.Load()
	if t == nil {
		r.drainsInFlight.Wait()
		return
	}
	var wg sync.WaitGroup
	for _, p := range t.pipelines {
		wg.Add(1)
		go func(p *pipeline) {
			defer wg.Done()
			p.drain(grace)
		}(p)
	}
	wg.Wait()
	r.drainsInFlight.Wait()
}

// snapshot returns a per-project view of pipeline counters for /stats.
func (r *registry) snapshot() map[string]projectStatsSnapshot {
	t := r.cur.Load()
	if t == nil {
		return nil
	}
	out := make(map[string]projectStatsSnapshot, len(t.pipelines))
	for name, p := range t.pipelines {
		out[name] = p.stats.snapshot()
	}
	return out
}

// keysFileMaxBytes caps the keys file read so a runaway-large file (or an
// attacker-substituted /dev/zero symlink target, if O_NOFOLLOW were missing)
// cannot exhaust process memory on SIGHUP. 1 MiB comfortably fits ~4000
// project entries at typical key/name sizes.
const keysFileMaxBytes = 1 << 20

// loadKeysFile reads a JSON object mapping project name → API key. Project
// names must be non-empty and may not start with `_` (reserved for sentinels
// like DefaultProject). API keys must be non-empty strings.
//
// The file is opened with O_NOFOLLOW so an attacker who can replace the
// configured path with a symlink cannot redirect us to read arbitrary files.
// World-writable files are rejected (a writable keys file is effectively a
// per-process root credential for every registered tenant — operators should
// keep it 0600 or 0640). The read is capped at keysFileMaxBytes.
func loadKeysFile(path string) (map[string]string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		// ELOOP from O_NOFOLLOW surfaces as a "too many levels of symbolic
		// links" syscall error; wrap to make the security context explicit.
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("keys file %q is a symlink (refusing for safety)", path)
		}
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("keys file %q is not a regular file", path)
	}
	if fi.Mode().Perm()&0o002 != 0 {
		return nil, fmt.Errorf("keys file %q is world-writable (perm %#o); chmod 0600 it", path, fi.Mode().Perm())
	}
	b, err := io.ReadAll(io.LimitReader(f, keysFileMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > keysFileMaxBytes {
		return nil, fmt.Errorf("keys file %q exceeds %d bytes", path, keysFileMaxBytes)
	}
	var raw map[string]string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse keys file: %w", err)
	}
	for name, key := range raw {
		if name == "" {
			return nil, errors.New("keys file: project name must not be empty")
		}
		if name[0] == '_' {
			return nil, fmt.Errorf("keys file: project name %q must not start with '_' (reserved)", name)
		}
		if key == "" {
			return nil, fmt.Errorf("keys file: api key for project %q must not be empty", name)
		}
	}
	return raw, nil
}

// projectKeyMarker is the cheap prefilter for the no-_project fast path:
// every datagram that lacks this substring cannot possibly carry a top-level
// `_project` field, so we skip the scanner entirely. False positives (the
// substring appearing inside a string value or nested object key) fall
// through to the scanner, which then correctly reports removed=false.
var projectKeyMarker = []byte(`"_project"`)

// projectKey is the bare key name compared against unquoted JSON key bytes
// during the top-level walk. projectKeyBytes is the precomputed []byte form
// used by bytes.Equal so we don't pay []byte(const) per match.
const projectKey = "_project"

var projectKeyBytes = []byte(projectKey)

// maxScanDepth caps nesting in scanContainer. A 1 MB datagram of nothing but
// `[` is legal JSON to encoding/json but useless to us; the cap turns
// adversarial deep nesting into a parse_error drop without affecting any
// realistic payload.
const maxScanDepth = 256

// extractAndStripProject pulls `_project` out of a top-level JSON object and
// returns (project, bytes-without-_project, removed, malformed, badProject).
//
//   - removed=true means at least one `_project` member was stripped and
//     `stripped` is the rewritten body.
//   - malformed=true means the input is not a valid top-level JSON object
//     (or has a non-string key, or a value the scanner couldn't consume).
//     The caller MUST account this as a parse_error and drop — the original
//     bytes would 400 against the gateway and poison the batch.
//   - badProject=true means at least one `_project` member was present but
//     its LAST occurrence is not a JSON string (e.g. number, null, object).
//     The body is still well-formed JSON; routing is unusable. Caller
//     accounts as unrouted_unknown so a misbehaving client SDK doesn't get
//     silently routed to the DefaultProject fallback.
//   - removed=false, malformed=false, badProject=false means the input is a
//     JSON object with no `_project` (the common case).
//
// All occurrences of `_project` are stripped (duplicate top-level keys are
// non-canonical JSON, but leaking even one through would 400 against the
// gateway's DisallowUnknownFields). The "last wins" convention is followed
// for the returned project name to match what json.Unmarshal would do.
//
// The implementation is a hand-rolled top-level scanner — encoding/json is
// 5–10× slower on this hot path because it boxes every token into an
// interface{} and allocates a fresh json.RawMessage per value. Behavior is
// cross-checked against encoding/json by FuzzExtractAndStripProject.
func extractAndStripProject(b []byte) (project string, stripped []byte, removed, malformed, badProject bool) {
	// Hot-path prefilter: most datagrams in single-tenant deployments don't
	// carry the field. bytes.IndexByte-driven Contains keeps this zero-alloc.
	if !bytes.Contains(b, projectKeyMarker) {
		return "", b, false, false, false
	}

	i := scanWS(b, 0)
	if i >= len(b) {
		return "", b, false, true, false
	}
	if b[i] != '{' {
		// Not an object — caller's validator will drop with parse_error
		// regardless, so don't double-count here.
		return "", b, false, false, false
	}
	i++

	type span struct {
		keyStart, valEnd, idx int
	}
	var hits []span
	idx := 0

	// Empty-object short-circuit. Without this the loop would fail on the
	// missing `"` of the (nonexistent) first key and return malformed.
	// Trailing commas like {"a":1,} are still rejected because the
	// post-comma branch falls back into the loop's key-quote check.
	i = scanWS(b, i)
	if i >= len(b) {
		return "", b, false, true, false
	}
	if b[i] == '}' {
		return "", b, false, false, false
	}

	for {
		// Key.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != '"' {
			return "", b, false, true, false
		}
		keyStart := i
		keyContentStart := i + 1
		keyContentEnd, ok := scanStringBody(b, keyContentStart)
		if !ok {
			return "", b, false, true, false
		}
		i = keyContentEnd + 1 // past closing quote

		// Colon.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != ':' {
			return "", b, false, true, false
		}
		i++

		// Value.
		i = scanWS(b, i)
		valStart := i
		valEnd, valKind, ok := scanValue(b, i)
		if !ok {
			return "", b, false, true, false
		}
		i = valEnd

		// Match `_project` against the raw key bytes. A key written with
		// escape sequences (e.g. "_project") won't match this literal
		// compare, but the projectKeyMarker prefilter would also have
		// rejected such inputs before reaching the scanner — so behavior
		// is consistent with the pre-scanner code path (which never
		// entered the json.Decoder either).
		if keyContentEnd-keyContentStart == len(projectKey) &&
			bytes.Equal(b[keyContentStart:keyContentEnd], projectKeyBytes) {
			hits = append(hits, span{keyStart: keyStart, valEnd: valEnd, idx: idx})
			// Last-wins for both project name and badProject — reset each
			// iteration so a string-typed later occurrence overrides an
			// earlier non-string one (and vice versa).
			project = ""
			badProject = true
			if valKind == kindString {
				pv, pvOK := unquoteJSONString(b[valStart:valEnd])
				if pvOK {
					project = pv
					badProject = false
				}
			}
		}
		idx++

		// Separator or end.
		i = scanWS(b, i)
		if i >= len(b) {
			return "", b, false, true, false
		}
		if b[i] == ',' {
			i++
			continue
		}
		if b[i] == '}' {
			break
		}
		return "", b, false, true, false
	}

	total := idx
	if len(hits) == 0 {
		return "", b, false, false, false
	}
	if len(hits) == total {
		// All members were `_project`. Result is the empty object.
		return project, []byte("{}"), true, false, badProject
	}

	// Splice out each hit, extending the cut to absorb exactly one
	// separating comma per removed member so the surviving object stays
	// well-formed. A first-position member owns the comma after its value;
	// any other position owns the comma before its key.
	type cut struct{ start, end int }
	cuts := make([]cut, 0, len(hits))
	for _, h := range hits {
		if h.idx == 0 {
			end := h.valEnd
			for end < len(b) && isJSONSpace(b[end]) {
				end++
			}
			if end < len(b) && b[end] == ',' {
				end++
			}
			cuts = append(cuts, cut{start: h.keyStart, end: end})
		} else {
			start := h.keyStart
			for start > 0 && isJSONSpace(b[start-1]) {
				start--
			}
			if start > 0 && b[start-1] == ',' {
				start--
			}
			cuts = append(cuts, cut{start: start, end: h.valEnd})
		}
	}

	out := make([]byte, 0, len(b))
	pos := 0
	for _, c := range cuts {
		if c.start > pos {
			out = append(out, b[pos:c.start]...)
		}
		pos = c.end
	}
	out = append(out, b[pos:]...)
	return project, out, true, false, badProject
}

// scanWS advances past JSON whitespace and returns the next non-space offset.
func scanWS(b []byte, i int) int {
	for i < len(b) {
		c := b[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		return i
	}
	return i
}

// scanStringBody walks the body of a JSON string starting just past the
// opening quote (b[start] is the first content byte) and returns the offset
// of the matching closing quote. Handles \" and \\ escapes (and \uXXXX for
// length-only purposes — full escape validation happens in unquoteJSONString
// only for the project value we care about). Rejects unescaped control
// bytes per RFC 8259 §7.
func scanStringBody(b []byte, start int) (closeQuote int, ok bool) {
	i := start
	for i < len(b) {
		c := b[i]
		if c == '"' {
			return i, true
		}
		if c == '\\' {
			if i+1 >= len(b) {
				return 0, false
			}
			if b[i+1] == 'u' {
				// \uXXXX — must have 4 hex digits.
				if i+6 > len(b) {
					return 0, false
				}
				for k := i + 2; k < i+6; k++ {
					if !isHex(b[k]) {
						return 0, false
					}
				}
				i += 6
				continue
			}
			// Single-char escape: " \ / b f n r t. Other follow chars are
			// invalid per RFC 8259 but tolerated here — unquoteJSONString
			// will fail them via encoding/json if we ever care.
			i += 2
			continue
		}
		if c < 0x20 {
			return 0, false
		}
		i++
	}
	return 0, false
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

type valueKind int

const (
	kindUnknown valueKind = iota
	kindString
	kindNumber
	kindObject
	kindArray
	kindTrue
	kindFalse
	kindNull
)

// scanValue advances past one JSON value at b[i] and returns the byte
// offset just past it plus the value's kind. ok=false on any structural
// error (mismatched brackets, truncated literals, malformed number).
func scanValue(b []byte, i int) (end int, kind valueKind, ok bool) {
	if i >= len(b) {
		return 0, kindUnknown, false
	}
	switch b[i] {
	case '"':
		cq, ok := scanStringBody(b, i+1)
		if !ok {
			return 0, kindUnknown, false
		}
		return cq + 1, kindString, true
	case '{', '[':
		end, ok := scanContainer(b, i)
		kind := kindObject
		if b[i] == '[' {
			kind = kindArray
		}
		return end, kind, ok
	case 't':
		if i+4 <= len(b) && string(b[i:i+4]) == "true" {
			return i + 4, kindTrue, true
		}
		return 0, kindUnknown, false
	case 'f':
		if i+5 <= len(b) && string(b[i:i+5]) == "false" {
			return i + 5, kindFalse, true
		}
		return 0, kindUnknown, false
	case 'n':
		if i+4 <= len(b) && string(b[i:i+4]) == "null" {
			return i + 4, kindNull, true
		}
		return 0, kindUnknown, false
	default:
		end, ok := scanNumber(b, i)
		return end, kindNumber, ok
	}
}

// scanContainer walks an object or array and returns the offset just past
// the matching closing bracket. Uses an explicit bracket stack so that
// mismatched pairs like `[1, 2}` are rejected as malformed at the routing
// layer (rather than relying on the downstream json.Valid re-check, which
// would mis-attribute the parse_error to the pipeline-stats layer).
// Respects string-quoting so brackets inside string values don't confuse
// the count. Bounded by maxScanDepth to defang adversarial deeply-nested
// input.
func scanContainer(b []byte, i int) (int, bool) {
	var stack [maxScanDepth]byte
	depth := 0
	push := func(open byte) bool {
		if depth >= maxScanDepth {
			return false
		}
		stack[depth] = open
		depth++
		return true
	}
	pop := func(close byte) bool {
		if depth == 0 {
			return false
		}
		depth--
		want := byte('}')
		if stack[depth] == '[' {
			want = ']'
		}
		return want == close
	}

	if !push(b[i]) {
		return 0, false
	}
	i++
	for i < len(b) && depth > 0 {
		c := b[i]
		switch c {
		case '"':
			cq, ok := scanStringBody(b, i+1)
			if !ok {
				return 0, false
			}
			i = cq + 1
		case '{', '[':
			if !push(c) {
				return 0, false
			}
			i++
		case '}', ']':
			if !pop(c) {
				return 0, false
			}
			i++
		default:
			i++
		}
	}
	if depth != 0 {
		return 0, false
	}
	return i, true
}

// scanNumber matches the RFC 8259 number grammar:
// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?
func scanNumber(b []byte, i int) (int, bool) {
	start := i
	if i < len(b) && b[i] == '-' {
		i++
	}
	if i >= len(b) {
		return 0, false
	}
	if b[i] == '0' {
		i++
	} else if b[i] >= '1' && b[i] <= '9' {
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	} else {
		return 0, false
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i == start {
		return 0, false
	}
	return i, true
}

// unquoteJSONString takes b including the surrounding quotes and returns
// the decoded string. Fast path: if there are no backslashes, returns a
// string conversion of the inner bytes (one allocation). Slow path defers
// to encoding/json for escape handling so we don't reimplement surrogate-
// pair logic.
func unquoteJSONString(b []byte) (string, bool) {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return "", false
	}
	inner := b[1 : len(b)-1]
	if bytes.IndexByte(inner, '\\') < 0 && utf8.Valid(inner) {
		// Fast path: no escapes and clean UTF-8 → one allocation. Invalid
		// UTF-8 in the fast path would diverge from encoding/json (which
		// replaces with U+FFFD), so fall through to the stdlib unmarshal
		// in that case.
		return string(inner), true
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return "", false
	}
	return s, true
}

func sortedKeys(m map[string]*pipeline) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
