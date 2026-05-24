package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
// its final partial batch, then waits for the flusher to drain. graceTimer,
// if non-nil, cancels the pipeline context after firing so a wedged POST
// cannot prevent shutdown.
func (p *pipeline) drain(grace time.Duration) {
	close(p.rawCh)

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
	// no-_project lookup is one map miss instead of two.
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
	tbl := r.buildTable(keys, nil)
	r.cur.Store(tbl)
	for _, p := range tbl.pipelines {
		p.start()
	}
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

	prev := r.cur.Load()
	tbl := r.buildTable(newKeys, prev)
	r.cur.Store(tbl)

	// Start any freshly-spawned pipelines.
	for name, p := range tbl.pipelines {
		if prev != nil {
			if old, ok := prev.pipelines[name]; ok && old == p {
				continue
			}
		}
		p.start()
	}

	// Drain pipelines that are gone or replaced.
	if prev != nil {
		for name, old := range prev.pipelines {
			if cur, ok := tbl.pipelines[name]; !ok || cur != old {
				go old.drain(r.cfg.ShutdownGrace)
			}
		}
	}

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
		if t.hasDefault {
			return t.pipelines[DefaultProject], true
		}
		return nil, false
	}
	p, ok := t.pipelines[project]
	return p, ok
}

// dispatch implements listenSink. Routes a datagram to its project's
// pipeline based on the `_project` field; strips `_project` so the gateway
// sees the original CustomEventInput shape. Drops are accounted at the
// process level (DropsUnrouted{Missing,Unknown}) and not propagated as
// queue_full so the listener doesn't double-count.
func (r *registry) dispatch(dg rawDatagram) (delivered bool, queueFull bool) {
	project, stripped, removed := extractAndStripProject(dg.bytes)
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
	r.processStats.EventsReceived.Add(1)
	select {
	case p.rawCh <- dg:
		return true, false
	default:
		p.stats.DropsQueueFull.Add(1)
		return false, true
	}
}

// shutdown drains every pipeline. Called from main on SIGINT/SIGTERM after
// the listener has stopped accepting new datagrams.
func (r *registry) shutdown(grace time.Duration) {
	t := r.cur.Load()
	if t == nil {
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

// loadKeysFile reads a JSON object mapping project name → API key. Project
// names must be non-empty and may not start with `_` (reserved for sentinels
// like DefaultProject). API keys must be non-empty strings.
func loadKeysFile(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]string
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
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

// extractAndStripProject pulls `_project` out of a top-level JSON object and
// returns (project, bytes-without-_project, true). If the input is not a
// JSON object or `_project` is absent, returns ("", original, false) — the
// caller still routes/validates, treating the input as having no project.
//
// The strip path uses json.Decoder.InputOffset to slice the key+value out,
// avoiding an unmarshal + remarshal round-trip on the hot path. Order of
// remaining fields is preserved.
func extractAndStripProject(b []byte) (project string, stripped []byte, removed bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil {
		return "", b, false
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return "", b, false
	}

	var keyStart, valEnd int64 = -1, -1
	var foundIdx, idx int = -1, 0

	for dec.More() {
		ks := dec.InputOffset()
		k, err := dec.Token()
		if err != nil {
			return "", b, false
		}
		keyStr, ok := k.(string)
		if !ok {
			return "", b, false
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return "", b, false
		}
		ve := dec.InputOffset()
		if keyStr == "_project" && foundIdx < 0 {
			keyStart, valEnd = ks, ve
			foundIdx = idx
			var pv string
			if err := json.Unmarshal(raw, &pv); err == nil {
				project = pv
			}
			// Don't break — we need the total sibling count for the
			// comma-handling branch below, and a duplicate key (rare,
			// non-canonical) shouldn't confuse the splice.
		}
		idx++
	}
	if foundIdx < 0 {
		return "", b, false
	}
	siblings := idx - 1
	if siblings == 0 {
		return project, []byte("{}"), true
	}
	out := make([]byte, 0, len(b))
	if foundIdx == 0 {
		out = append(out, b[:keyStart]...)
		i := int(valEnd)
		for i < len(b) && isJSONSpace(b[i]) {
			i++
		}
		if i < len(b) && b[i] == ',' {
			i++
		}
		out = append(out, b[i:]...)
	} else {
		i := int(keyStart)
		for i > 0 && isJSONSpace(b[i-1]) {
			i--
		}
		if i > 0 && b[i-1] == ',' {
			i--
		}
		out = append(out, b[:i]...)
		out = append(out, b[valEnd:]...)
	}
	return project, out, true
}

func sortedKeys(m map[string]*pipeline) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
