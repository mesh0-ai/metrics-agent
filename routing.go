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
	"path/filepath"
	"sort"
	"strings"
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

// pipelineHandoffDepth is the per-pipeline rawCh capacity. With Plan B's
// shared queue absorbing bursts, the per-pipeline channel only needs enough
// slack to decouple the demuxer goroutine from the batcher goroutine. A
// noisy project that overruns this depth gets its own per-pipeline
// queue_full drops without consuming the shared budget. Sized so worst-case
// per-pipeline memory (depth * MaxEventBytes) stays bounded across many
// projects: 16 * 1 MB * 64 projects ≈ 1 GiB ceiling, dwarfed by the shared
// queue contribution.
const pipelineHandoffDepth = 16

// pipeline holds the per-project channels + goroutines that drain a single
// project's events to the gateway. One pipeline per registered API key.
//
// Each pipeline is independently rate-limited (its own bounded queue) and
// independently observable (its own pipelineStats). A wedged project cannot
// stall another.
type pipeline struct {
	project string
	apiKey  string

	// inline marks a pipeline created on demand from a datagram's `_token`
	// rather than from the keys file. Only these are subject to idle
	// expiry: a keys-file pipeline is an operator's declaration and stays
	// registered whether or not traffic arrives.
	inline bool
	// lastSeen is unix-nanos of the most recent datagram dispatched here.
	// Written on the listener's hot path, so an atomic rather than a lock.
	lastSeen atomic.Int64

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
	// Per-pipeline handoff buffer. The big in-flight buffer is the
	// registry's shared queue; this just decouples demuxer from batcher.
	rawCh := make(chan rawDatagram, pipelineHandoffDepth)
	batchCh := make(chan EventBatch, 8)

	plog := log.With("project", project)

	b := newEventsBatcher(rawCh, batchCh, processStats, plog, cfg.MaxBatch, cfg.MaxEventBytes, cfg.BatchWindow)
	b.pipelineStats = pstats

	f := newEventsFlusherWithKey(batchCh, cfg, apiKey, plog, processStats)
	f.pipelineStats = pstats

	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	f.ctx = ctx

	p := &pipeline{
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
	p.lastSeen.Store(time.Now().UnixNano())
	return p
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
//
// Plan B architecture: one process-wide bounded queue (sharedRawCh) absorbs
// listener bursts. A single demuxer goroutine drains it and routes each
// datagram to its pipeline's small handoff channel. This makes the total
// in-flight memory ceiling O(QueueSize × MaxEventBytes), independent of the
// number of registered projects — and lets operators set a single sizing
// knob that matches the sidecar's memory limit. Trade-off: one slow project
// that fills its handoff channel accounts to its own per-project
// queue_full; one project that monopolises the shared queue (sustained
// high traffic against a wedged flusher) can starve the rest. For the 99%
// case where projects belong to the same customer that's acceptable; a
// tenant that needs isolation should run a dedicated agent.
type registry struct {
	cur atomic.Pointer[routingTable]

	cfg          Config
	log          *slog.Logger
	processStats *selfStats

	// sharedRawCh is the process-wide bounded queue between dispatch and
	// the demuxer. Cap = cfg.QueueSize. Sized to match the sidecar's
	// memory budget (depth × MaxEventBytes worst-case).
	sharedRawCh chan rawDatagram
	// demuxDone closes when the demuxer goroutine has drained sharedRawCh
	// after it was closed. shutdown() waits on this before draining
	// pipelines to ensure no in-flight datagrams are stranded.
	demuxDone chan struct{}
	// sharedSendMu guards sharedRawCh against send-after-close.
	// Hot-path dispatchers take RLock for the send; closeShared takes
	// Lock to flip sharedClosed and close(sharedRawCh) under exclusion.
	// Same canonical fix as the per-pipeline sendMu, now sized to a
	// single process-wide mutex instead of one per pipeline.
	sharedSendMu    sync.RWMutex
	sharedClosed    bool
	sharedCloseOnce sync.Once

	// reloadMu serialises SIGHUP handlers so two concurrent signals cannot
	// produce diverging tables. Read path (lookup) is lock-free via
	// atomic.Pointer.
	reloadMu sync.Mutex

	// inlineMu serialises on-demand registration of `_token` pipelines.
	// Distinct from reloadMu because the two contend on different paths:
	// reload is a signal handler, inline registration happens on the
	// listener's dispatch path the first time a project is seen. Both
	// publish through the same copy-on-write swap of cur.
	inlineMu sync.Mutex

	// InlineProjectsRegistered counts pipelines created from an inline
	// `_token`. InlineProjectsExpired counts those later retired for
	// idleness. Both surface in /stats so an operator can see the inline
	// population without reading logs.
	InlineProjectsRegistered atomic.Uint64
	InlineProjectsExpired    atomic.Uint64

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
	qs := cfg.QueueSize
	if qs <= 0 {
		qs = 2000
	}
	return &registry{
		cfg:          cfg,
		log:          log,
		processStats: processStats,
		sharedRawCh:  make(chan rawDatagram, qs),
		demuxDone:    make(chan struct{}),
	}
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
		// Only reachable when MESH0_KEYS_FILE is set (loadConfig requires one
		// of API_KEY/KEYS_FILE, and API_KEY alone always yields a key). A
		// readable-but-empty keys file is a normal fresh-deploy state on
		// Kubernetes: the Secret is templated as `{}` and a reconciler fills
		// it in after the pod starts. Exiting here would turn that
		// provisioning window into a crash loop, so start with an empty
		// routing table and let the keys-file poll / SIGHUP install routes
		// when they appear. Datagrams arriving meanwhile drop as unrouted.
		r.log.Warn("keys file has no entries yet, starting with empty routing table",
			"keys_file", r.cfg.KeysFile)
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
	// Start the demuxer AFTER pipelines are running so its first lookup
	// always resolves to a started pipeline.
	go r.demux()
	r.log.Info("routing installed",
		"projects", sortedKeys(tbl.pipelines),
		"has_default", tbl.hasDefault,
		"shared_queue_size", cap(r.sharedRawCh),
	)
	return nil
}

// demux drains sharedRawCh and forwards each datagram to its pipeline's
// handoff channel. Runs in its own goroutine; exits when sharedRawCh is
// closed (by shutdown). The lookup is performed against the live routing
// table — a project removed by SIGHUP between dispatch and demux is
// accounted as routing_closed.
func (r *registry) demux() {
	defer close(r.demuxDone)
	for dg := range r.sharedRawCh {
		p, ok := r.lookup(dg.project)
		if !ok {
			// Project disappeared between dispatch and demux (SIGHUP
			// reload removed it after the datagram was queued). Account
			// as routing_closed rather than re-deriving the original
			// missing/unknown reason — the datagram was successfully
			// routed at dispatch time, the destination went away.
			r.processStats.DropsRoutingClosed.Add(1)
			continue
		}
		delivered, closed, full := p.trySend(dg)
		switch {
		case delivered:
			// nothing to account; batcher will bump pipelineStats after
			// validateEvent succeeds.
		case closed:
			p.stats.DropsRoutingClosed.Add(1)
			r.processStats.DropsRoutingClosed.Add(1)
		case full:
			// Per-project handoff buffer overflow. Reserved for the
			// pipeline's own counter — the shared-queue overflow path
			// (process-wide DropsQueueFull) is bumped at dispatch when
			// sharedRawCh itself is full. Distinguishing the two lets
			// operators tell "the whole agent is back-pressured" from
			// "this one project's batcher/flusher is stuck."
			p.stats.DropsQueueFull.Add(1)
		}
	}
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
	if prev != nil && keysUnchanged(prev, newKeys) {
		// Steady-state poll tick: the file re-read succeeded and nothing
		// changed. Stamp freshness but skip the table swap so periodic polls
		// don't churn allocations or spam "routing reloaded" every interval.
		r.LastKeysReloadUnix.Store(time.Now().Unix())
		return
	}
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

// keysUnchanged reports whether the desired keys map matches the live table
// exactly — same project set, same API key per project. Used by reload to
// make the periodic keys-file poll a quiet no-op in the steady state.
func keysUnchanged(t *routingTable, keys map[string]string) bool {
	if len(t.pipelines) != len(keys) {
		return false
	}
	for name, key := range keys {
		p, ok := t.pipelines[name]
		if !ok || p.apiKey != key {
			return false
		}
	}
	return true
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
// bumped by the batcher after validation.
//
// Plan B: dispatch parses + validates routability + checks the destination
// pipeline isn't already closed, then enqueues onto the shared queue. The
// demuxer goroutine does the actual pipeline.trySend. Splitting it this way
// preserves the original synchronous accounting for routing-time drops
// (unrouted_*, routing_closed of an already-drained pipeline) while moving
// the per-pipeline handoff onto an async path that doesn't block the
// listener's read loop.
//
// Drops route to:
//   - DropsParseError (process-wide) when routing-strip finds non-JSON-object.
//   - DropsUnrouted{Missing,Unknown} (process-wide) when no pipeline matches,
//     including non-string `_project` values.
//   - DropsRoutingClosed (per-pipeline + process-wide) when the destination
//     pipeline was already closed at dispatch time (SIGHUP reload race).
//   - DropsQueueFull (process-wide) when the shared queue is full —
//     signalled to the listener via queueFull=true so the listener bumps it.
//
// Per-pipeline DropsQueueFull is bumped in the demuxer when the pipeline's
// own handoff buffer is full (a single noisy project), distinct from
// process-wide queue_full (shared queue exhausted).
func (r *registry) dispatch(dg rawDatagram) (delivered bool, queueFull bool) {
	f := extractAndStripRouting(dg.bytes)
	if f.malformed {
		r.processStats.DropsParseError.Add(1)
		return false, false
	}
	if f.badProject {
		// `_project` was present but not a JSON string. JSON is structurally
		// valid; the routing intent is unusable. Account as unrouted_unknown
		// so an alert on that counter catches malformed client SDKs without
		// conflating with broken-JSON parse_error.
		r.processStats.DropsUnroutedUnknown.Add(1)
		return false, false
	}
	project := f.project
	if f.removed {
		// Note this assignment happens for a stripped `_token` too, even on
		// the drop paths below — the credential is out of the body before
		// anything else can happen to it.
		dg.bytes = f.stripped
	}
	if f.badToken && r.cfg.InlineTokens {
		// Present but unusable. NOT silently downgraded to the keys-file
		// credential: a caller that meant to authenticate with a token and
		// got the type wrong must not be quietly authorized as somebody
		// else, and on an empty keys file that downgrade is a drop anyway.
		r.processStats.DropsBadToken.Add(1)
		return false, false
	}
	if r.cfg.InlineTokens {
		dg.token = f.token
	}

	p, ok := r.lookup(project)
	if !ok && r.cfg.InlineTokens && f.token != "" && project != "" {
		// On-demand registration. A datagram that names a project AND
		// carries its own credential needs no keys-file entry — the
		// credential IS the authorization, and the gateway resolves the
		// project from the token's own claim. This is the whole path for
		// deployments that mint per-instance tokens and publish no keys.
		p, ok = r.registerInline(project)
	}
	if !ok {
		if project == "" {
			r.processStats.DropsUnroutedMissing.Add(1)
		} else {
			r.processStats.DropsUnroutedUnknown.Add(1)
		}
		return false, false
	}
	// An inline pipeline holds no credential of its own (see registerInline),
	// so a datagram that routes there without carrying one cannot be
	// authenticated at all. Drop it here rather than letting the batcher
	// assemble events the flusher would POST under an empty bearer — that
	// spends a whole batch to earn a 401, and accounts as flush_failed, which
	// points an operator at the gateway instead of at the emitter.
	//
	// Reachable in practice: an emitter that normally sends `_token` still
	// emits without one when its own minting fails, and the project stays
	// registered from earlier traffic.
	if p.inline && dg.token == "" {
		p.stats.DropsMissingToken.Add(1)
		r.processStats.DropsMissingToken.Add(1)
		return false, false
	}
	p.lastSeen.Store(dg.at.UnixNano())
	// Early closed-pipeline check. If the pipeline was already drained at
	// dispatch time (pre-drain in tests, or a reload that retired the
	// project before this dispatch fired), short-circuit with synchronous
	// routing_closed accounting. The demuxer also handles the closed case
	// for the dispatch→demux race window, but doing it here too keeps the
	// caller's view of "did this drop happen yet?" synchronous, which is
	// what the existing closed-pipeline test asserts.
	p.sendMu.RLock()
	pclosed := p.closed
	p.sendMu.RUnlock()
	if pclosed {
		p.stats.DropsRoutingClosed.Add(1)
		r.processStats.DropsRoutingClosed.Add(1)
		return false, false
	}
	// Stamp project so the demuxer can route without re-parsing. May be ""
	// when routing to DefaultProject — lookup() handles that case.
	dg.project = project
	// Manual RUnlock (no defer) — this is the listener's hot path; the
	// defer overhead is measurable here and the function is small enough
	// that the early returns below are easy to audit by eye.
	r.sharedSendMu.RLock()
	if r.sharedClosed {
		r.sharedSendMu.RUnlock()
		// Shutdown in progress; account as routing_closed for the
		// destination pipeline so this drop is attributable. Not
		// queueFull (the queue may have had capacity; the agent is
		// just exiting).
		p.stats.DropsRoutingClosed.Add(1)
		r.processStats.DropsRoutingClosed.Add(1)
		return false, false
	}
	select {
	case r.sharedRawCh <- dg:
		r.sharedSendMu.RUnlock()
		return true, false
	default:
		r.sharedSendMu.RUnlock()
		return false, true
	}
}

// registerInline creates and publishes a pipeline for a project first seen on
// a datagram carrying its own `_token`. Returns the pipeline and true when the
// project is routable afterwards.
//
// WHY THIS EXISTS. Under the keys-file model an operator declares every
// project up front, so an unknown `_project` is a mis-tagged caller and
// dropping it is right. Under inline credentials there is nothing to declare:
// the emitting instance mints a token whose own claim names the project, and
// the first this agent hears of that project is the datagram in hand. Without
// on-demand registration such a deployment routes nothing at all — which is
// exactly the empty-keys-file silence this path was built to end.
//
// The pipeline is created with an EMPTY apiKey. That is deliberate and load
// bearing: an inline pipeline has no credential of its own, and every batch it
// flushes must carry the token that arrived with its events (EventBatch.Token).
// Giving it a fallback key would let a rotation gap or a stripped token
// silently authenticate one project's events under another credential.
//
// MaxProjects is enforced here as well as on the keys file, and the two share
// a budget: an inline registration that would exceed the cap is refused and the
// datagram drops as unrouted_unknown, which is the same outcome an unknown
// project has always had.
func (r *registry) registerInline(project string) (*pipeline, bool) {
	r.inlineMu.Lock()
	defer r.inlineMu.Unlock()

	// Re-check under the lock: several datagrams for a new project can race
	// into dispatch together, and only the first may create the pipeline.
	if p, ok := r.lookup(project); ok {
		return p, true
	}

	prev := r.cur.Load()
	if prev != nil && r.cfg.MaxProjects > 0 && len(prev.pipelines) >= r.cfg.MaxProjects {
		r.log.Warn("inline project registration refused: max_projects reached",
			"project", project, "max_projects", r.cfg.MaxProjects)
		return nil, false
	}

	p := newPipeline(project, "", r.cfg, r.log, r.processStats)
	p.inline = true
	p.start()

	// Copy-on-write publish, same discipline as reload: readers hold the old
	// table until the atomic swap, so lookup stays lock-free.
	next := &routingTable{pipelines: make(map[string]*pipeline, lenPipelines(prev)+1)}
	if prev != nil {
		for name, existing := range prev.pipelines {
			next.pipelines[name] = existing
		}
		next.hasDefault = prev.hasDefault
	}
	next.pipelines[project] = p
	r.cur.Store(next)

	r.InlineProjectsRegistered.Add(1)
	r.log.Info("inline project registered from datagram credential",
		"project", project, "projects", len(next.pipelines))
	return p, true
}

func lenPipelines(t *routingTable) int {
	if t == nil {
		return 0
	}
	return len(t.pipelines)
}

// expireInline retires inline pipelines idle for longer than idle. Keys-file
// pipelines are never touched: an operator declared those, and a project that
// happens to be quiet is not a project that has gone away.
//
// Returns the number retired. Safe to call concurrently with dispatch — the
// swap is copy-on-write and the drain runs after the retired pipeline is no
// longer reachable from the published table, so a dispatch that grabbed it
// just before the swap still delivers (or accounts routing_closed, which the
// existing closed-pipeline path already handles).
func (r *registry) expireInline(idle time.Duration, now time.Time) int {
	if idle <= 0 {
		return 0
	}
	r.inlineMu.Lock()

	prev := r.cur.Load()
	if prev == nil {
		r.inlineMu.Unlock()
		return 0
	}
	cutoff := now.Add(-idle).UnixNano()
	var retired []*pipeline
	for name, p := range prev.pipelines {
		if p.inline && p.lastSeen.Load() < cutoff {
			retired = append(retired, p)
			_ = name
		}
	}
	if len(retired) == 0 {
		r.inlineMu.Unlock()
		return 0
	}

	next := &routingTable{
		pipelines:  make(map[string]*pipeline, len(prev.pipelines)-len(retired)),
		hasDefault: prev.hasDefault,
	}
	for name, p := range prev.pipelines {
		keep := true
		for _, dead := range retired {
			if dead == p {
				keep = false
				break
			}
		}
		if keep {
			next.pipelines[name] = p
		}
	}
	r.cur.Store(next)
	r.inlineMu.Unlock()

	// Drain OUTSIDE the lock and after the swap: draining flushes in-flight
	// batches, which can take up to the shutdown grace, and holding
	// inlineMu across that would block every new project registration.
	for _, p := range retired {
		r.drainsInFlight.Add(1)
		go func(dead *pipeline) {
			defer r.drainsInFlight.Done()
			dead.drain(r.cfg.ShutdownGrace)
			r.InlineProjectsExpired.Add(1)
			r.log.Info("inline project expired (idle)", "project", dead.project)
		}(p)
	}
	return len(retired)
}

// runInlineExpiry ticks expireInline until stop closes. Started from main
// only when both an idle window and a keys-less/inline deployment make it
// meaningful; a zero idle window disables it entirely.
func (r *registry) runInlineExpiry(idle time.Duration, stop <-chan struct{}) {
	if idle <= 0 {
		return
	}
	// Check several times per window so a pipeline is retired reasonably
	// close to its deadline without a timer per pipeline.
	tick := idle / 4
	if tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			r.expireInline(idle, now)
		}
	}
}

// shutdown drains every pipeline. Called from main on SIGINT/SIGTERM after
// the listener has stopped accepting new datagrams. Also waits for any
// reload-initiated drains so SIGHUP-then-SIGTERM doesn't orphan goroutines.
//
// Ordering matters: close the shared queue and wait the demuxer to exit
// before touching pipelines, so the demuxer doesn't race a pipeline.drain
// (which sets closed and closes pipeline.rawCh).
func (r *registry) shutdown(grace time.Duration) {
	// Close-once guard: a test may install() then defer shutdown(0)
	// alongside an explicit shutdown call. Closing twice would panic.
	r.closeShared()
	<-r.demuxDone

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

// closeShared closes sharedRawCh once under exclusion against in-flight
// dispatch sends. Idempotent so callers don't have to coordinate; tests
// that defer shutdown(0) alongside an explicit shutdown still work.
func (r *registry) closeShared() {
	r.sharedCloseOnce.Do(func() {
		r.sharedSendMu.Lock()
		r.sharedClosed = true
		close(r.sharedRawCh)
		r.sharedSendMu.Unlock()
	})
}

// snapshot returns a per-project view of pipeline counters for /stats.
// inlineLive counts the currently-registered inline pipelines. Read from the
// published table, so it reflects what dispatch would find right now.
func (r *registry) inlineLive() int {
	t := r.cur.Load()
	if t == nil {
		return 0
	}
	n := 0
	for _, p := range t.pipelines {
		if p.inline {
			n++
		}
	}
	return n
}

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
// Symlinks that resolve within the file's own directory are followed — see
// openKeysFile — because Kubernetes Secret volumes always present files that
// way. World-writable files are rejected (a writable keys file is effectively
// a per-process root credential for every registered tenant — operators
// should keep it 0600 or 0640). The read is capped at keysFileMaxBytes.
func loadKeysFile(path string) (map[string]string, error) {
	f, err := openKeysFile(path)
	if err != nil {
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

// openKeysFile opens the keys file, refusing symlinks that escape the file's
// own directory. Kubernetes Secret volumes always present files as symlinks
// (`keys.json → ..data/keys.json`, with `..data` itself a symlink to a
// timestamped directory inside the mount), so a flat O_NOFOLLOW open can
// never start under the agent's primary deployment mode. Following a link
// that stays inside the directory adds no reach: an attacker who can plant a
// symlink there can already overwrite the keys file itself. Links resolving
// elsewhere (/etc/shadow, /dev/zero) are still refused, which is what the
// O_NOFOLLOW was protecting against.
func openKeysFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	// ELOOP from O_NOFOLLOW surfaces as a "too many levels of symbolic
	// links" syscall error.
	if err == nil || !errors.Is(err, syscall.ELOOP) {
		return f, err
	}
	base, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(base, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("keys file %q is a symlink resolving outside its directory (refusing for safety)", path)
	}
	return os.OpenFile(resolved, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// projectKeyMarker is the cheap prefilter for the no-_project fast path:
// every datagram that lacks this substring cannot possibly carry a top-level
// `_project` field, so we skip the scanner entirely. False positives (the
// substring appearing inside a string value or nested object key) fall
// through to the scanner, which then correctly reports removed=false.
var projectKeyMarker = []byte(`"_project"`)

// tokenKeyMarker is the same prefilter for `_token`, the per-datagram
// credential used by deployments that mint their own instance tokens instead
// of publishing a keys file. See extractAndStripFields.
var tokenKeyMarker = []byte(`"_token"`)

// projectKey / tokenKey are the bare key names compared against unquoted JSON
// key bytes during the top-level walk. The *Bytes forms are precomputed so we
// don't pay []byte(const) per match.
const projectKey = "_project"

const tokenKey = "_token"

var projectKeyBytes = []byte(projectKey)

var tokenKeyBytes = []byte(tokenKey)

// maxScanDepth caps nesting in scanContainer. A 1 MB datagram of nothing but
// `[` is legal JSON to encoding/json but useless to us; the cap turns
// adversarial deep nesting into a parse_error drop without affecting any
// realistic payload.
const maxScanDepth = 256

// routingFields is what one pass of the top-level scanner recovered from a
// datagram: where it should be routed, what credential authorizes it, and the
// body with both of those members removed.
//
// 🛑 BOTH MEMBERS ARE STRIPPED, AND FOR `_token` THAT IS NOT MERELY ABOUT THE
// GATEWAY'S DisallowUnknownFields. A `_token` left in the body is a live
// credential written into stored telemetry, readable by anyone who can later
// read the event back. It must not survive this function.
type routingFields struct {
	project string
	token   string

	stripped  []byte
	removed   bool
	malformed bool

	// badProject / badToken: the member was present but its last occurrence
	// was not a JSON string. The body is still well-formed; the intent is
	// unusable. Kept separate because they route to different drops — a bad
	// project is unrouted_unknown, a bad token is a credential fault.
	badProject bool
	badToken   bool
}

// extractAndStripProject is the `_project`-only entry point, preserved with
// its exact original behavior.
//
// It is NOT a thin alias for the two-field scan: a body carrying only `_token`
// must come back from THIS function untouched, with removed=false. The
// `_project` contract predates inline credentials, and its test suite —
// including the differential fuzz against encoding/json — is written against
// that shape. Widening it in place would silently change what the fuzz is
// comparing.
func extractAndStripProject(b []byte) (project string, stripped []byte, removed, malformed, badProject bool) {
	f := extractAndStripFields(b, false)
	return f.project, f.stripped, f.removed, f.malformed, f.badProject
}

// extractAndStripRouting is the two-field scan used by dispatch: `_project`
// for routing and `_token` for the credential, both removed from the body in
// the same single pass.
func extractAndStripRouting(b []byte) routingFields {
	return extractAndStripFields(b, true)
}

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
// extractAndStripFields is the shared implementation. withToken selects the
// two-field scan; false reproduces the original `_project`-only behavior byte
// for byte, including leaving a `_token` member in place.
func extractAndStripFields(b []byte, withToken bool) routingFields {
	miss := routingFields{stripped: b}
	// Hot-path prefilter: most datagrams in single-tenant deployments carry
	// neither field. bytes.IndexByte-driven Contains keeps this zero-alloc.
	// Both markers are checked because either one alone requires the scan.
	if !bytes.Contains(b, projectKeyMarker) &&
		!(withToken && bytes.Contains(b, tokenKeyMarker)) {
		return miss
	}

	i := scanWS(b, 0)
	if i >= len(b) {
		return routingFields{stripped: b, malformed: true}
	}
	if b[i] != '{' {
		// Not an object — caller's validator will drop with parse_error
		// regardless, so don't double-count here.
		return miss
	}
	i++

	type span struct {
		keyStart, valEnd, idx int
	}
	var hits []span
	idx := 0

	var (
		project    string
		token      string
		badProject bool
		badToken   bool
	)

	// Empty-object short-circuit. Without this the loop would fail on the
	// missing `"` of the (nonexistent) first key and return malformed.
	// Trailing commas like {"a":1,} are still rejected because the
	// post-comma branch falls back into the loop's key-quote check.
	i = scanWS(b, i)
	if i >= len(b) {
		return routingFields{stripped: b, malformed: true}
	}
	if b[i] == '}' {
		return miss
	}

	for {
		// Key.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != '"' {
			return routingFields{stripped: b, malformed: true}
		}
		keyStart := i
		keyContentStart := i + 1
		keyContentEnd, ok := scanStringBody(b, keyContentStart)
		if !ok {
			return routingFields{stripped: b, malformed: true}
		}
		i = keyContentEnd + 1 // past closing quote

		// Colon.
		i = scanWS(b, i)
		if i >= len(b) || b[i] != ':' {
			return routingFields{stripped: b, malformed: true}
		}
		i++

		// Value.
		i = scanWS(b, i)
		valStart := i
		valEnd, valKind, ok := scanValue(b, i)
		if !ok {
			return routingFields{stripped: b, malformed: true}
		}
		i = valEnd

		// Match `_project` against the raw key bytes. A key written with
		// escape sequences (e.g. "_project") won't match this literal
		// compare, but the projectKeyMarker prefilter would also have
		// rejected such inputs before reaching the scanner — so behavior
		// is consistent with the pre-scanner code path (which never
		// entered the json.Decoder either).
		keyLen := keyContentEnd - keyContentStart
		switch {
		case keyLen == len(projectKey) &&
			bytes.Equal(b[keyContentStart:keyContentEnd], projectKeyBytes):
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
		case withToken && keyLen == len(tokenKey) &&
			bytes.Equal(b[keyContentStart:keyContentEnd], tokenKeyBytes):
			// Stripped on the SAME terms as `_project`, and unconditionally:
			// a non-string or otherwise unusable `_token` still must not
			// reach stored telemetry. Only the routing intent is discarded,
			// never the removal.
			hits = append(hits, span{keyStart: keyStart, valEnd: valEnd, idx: idx})
			token = ""
			badToken = true
			if valKind == kindString {
				tv, tvOK := unquoteJSONString(b[valStart:valEnd])
				if tvOK {
					token = tv
					badToken = false
				}
			}
		}
		idx++

		// Separator or end.
		i = scanWS(b, i)
		if i >= len(b) {
			return routingFields{stripped: b, malformed: true}
		}
		if b[i] == ',' {
			i++
			continue
		}
		if b[i] == '}' {
			break
		}
		return routingFields{stripped: b, malformed: true}
	}

	total := idx
	if len(hits) == 0 {
		return miss
	}
	result := routingFields{
		project:    project,
		token:      token,
		removed:    true,
		badProject: badProject,
		badToken:   badToken,
	}
	if len(hits) == total {
		// Every member was a routing field. Result is the empty object.
		result.stripped = []byte("{}")
		return result
	}

	// Splice out each hit, extending the cut to absorb exactly one
	// separating comma per removed member so the surviving object stays
	// well-formed.
	//
	// A member owns the comma AFTER it when no surviving member precedes it,
	// and the comma BEFORE it otherwise. Since hits are in ascending member
	// order, "no survivor before me" is exactly `h.idx == k` — the hit is
	// still inside the leading run of removed members.
	//
	// 🛑 THE CONDITION IS `h.idx == k`, NOT `h.idx == 0`. With one removal the
	// two agree, which is why the original held. With two ADJACENT removals at
	// the head, `h.idx == 0` makes the first claim the comma between them and
	// the second claim that same comma again — nothing absorbs the separator
	// after the run, and the object comes back as `{,"a":1}`. That was already
	// reachable before `_token` existed, via duplicate `_project` keys.
	type cut struct{ start, end int }
	cuts := make([]cut, 0, len(hits))
	for k, h := range hits {
		if h.idx == k {
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
	result.stripped = out
	return result
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
