package main

import (
	"math"
	"sort"
	"time"
)

// seriesKey identifies a unique (name, type, tags) tuple. The canonical
// TagsKey from parser.go means equivalent tag sets share a key.
type seriesKey struct {
	Name string
	Type MetricType
	Tags string
}

type counterAgg struct {
	tags  map[string]string
	value float64
}

type gaugeAgg struct {
	tags  map[string]string
	value float64
}

// timingAgg holds raw samples, capped to maxTimingSamples per series so a
// noisy producer can't OOM the agent. When the cap is hit we truncate (drop
// further samples for this window). This biases percentiles toward the start
// of the window, but in a 10s window with >10K samples the percentile signal
// is still useful, and count/sum/min/max remain exact.
type timingAgg struct {
	tags    map[string]string
	samples []float64
	count   uint64
	sum     float64
	min     float64
	max     float64
}

const maxTimingSamples = 10_000

// aggregator owns its maps without locks: it runs as a single goroutine,
// fed by a channel from the UDP reader. This keeps the hot path
// allocation-light and contention-free.
type aggregator struct {
	in       <-chan Metric
	flush    <-chan time.Time
	out      chan<- Snapshot
	stats    *selfStats
	counters map[seriesKey]*counterAgg
	gauges   map[seriesKey]*gaugeAgg
	timings  map[seriesKey]*timingAgg
	since    time.Time
}

func newAggregator(in <-chan Metric, flush <-chan time.Time, out chan<- Snapshot, stats *selfStats) *aggregator {
	return &aggregator{
		in:       in,
		flush:    flush,
		out:      out,
		stats:    stats,
		counters: make(map[seriesKey]*counterAgg),
		gauges:   make(map[seriesKey]*gaugeAgg),
		timings:  make(map[seriesKey]*timingAgg),
		since:    time.Now(),
	}
}

// run drives the aggregator until its input channel is closed by the
// upstream listener-shutdown path. On exit it emits one final snapshot so
// the in-flight flush window isn't dropped on SIGTERM.
func (a *aggregator) run() {
	for {
		select {
		case m, ok := <-a.in:
			if !ok {
				a.emit(time.Now())
				return
			}
			a.add(m)
		case t := <-a.flush:
			a.emit(t)
		}
	}
}

func (a *aggregator) add(m Metric) {
	k := seriesKey{Name: m.Name, Type: m.Type, Tags: m.TagsKey}
	switch m.Type {
	case MetricCounter:
		c, ok := a.counters[k]
		if !ok {
			c = &counterAgg{tags: m.Tags}
			a.counters[k] = c
		}
		// Sample rate scales the increment: @0.1 means each packet stands
		// for ~10 events, so the recorded count multiplies by 10.
		c.value += m.Value / m.SampleRate
	case MetricGauge:
		g, ok := a.gauges[k]
		if !ok {
			g = &gaugeAgg{tags: m.Tags}
			a.gauges[k] = g
		}
		g.value = m.Value
	case MetricTiming:
		t, ok := a.timings[k]
		if !ok {
			t = &timingAgg{tags: m.Tags, min: math.Inf(1), max: math.Inf(-1)}
			a.timings[k] = t
		}
		t.count++
		t.sum += m.Value
		if m.Value < t.min {
			t.min = m.Value
		}
		if m.Value > t.max {
			t.max = m.Value
		}
		if len(t.samples) < maxTimingSamples {
			t.samples = append(t.samples, m.Value)
		}
	}
}

// Snapshot is the flush payload handed to the flusher goroutine.
type Snapshot struct {
	Since   time.Time
	Until   time.Time
	Metrics []FlushedMetric
}

// FlushedMetric is the wire-shape for one series in one flush window.
// Numeric fields use pointers so a zero is distinguishable from "not set":
// counter=0, gauge=0, and timing min=0 are all valid values that must reach
// the gateway, not be silently dropped by `omitempty` on a bare float64.
type FlushedMetric struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Tags       map[string]string `json:"tags,omitempty"`
	Timestamp  time.Time         `json:"timestamp"`
	IntervalMs int64             `json:"interval_ms"`
	Value      *float64          `json:"value,omitempty"`
	Count      *uint64           `json:"count,omitempty"`
	Sum        *float64          `json:"sum,omitempty"`
	Min        *float64          `json:"min,omitempty"`
	Max        *float64          `json:"max,omitempty"`
	P50        *float64          `json:"p50,omitempty"`
	P95        *float64          `json:"p95,omitempty"`
	P99        *float64          `json:"p99,omitempty"`
}

func f64p(v float64) *float64 { return &v }
func u64p(v uint64) *uint64   { return &v }

func (a *aggregator) emit(now time.Time) {
	intervalMs := now.Sub(a.since).Milliseconds()
	if intervalMs <= 0 {
		intervalMs = 1
	}
	out := make([]FlushedMetric, 0, len(a.counters)+len(a.gauges)+len(a.timings))

	for k, c := range a.counters {
		out = append(out, FlushedMetric{
			Name:       k.Name,
			Type:       "counter",
			Tags:       c.tags,
			Timestamp:  a.since,
			IntervalMs: intervalMs,
			Value:      f64p(c.value),
		})
	}
	for k, g := range a.gauges {
		out = append(out, FlushedMetric{
			Name:       k.Name,
			Type:       "gauge",
			Tags:       g.tags,
			Timestamp:  now,
			IntervalMs: intervalMs,
			Value:      f64p(g.value),
		})
	}
	for k, t := range a.timings {
		fm := FlushedMetric{
			Name:       k.Name,
			Type:       "timing",
			Tags:       t.tags,
			Timestamp:  a.since,
			IntervalMs: intervalMs,
			Count:      u64p(t.count),
			Sum:        f64p(t.sum),
			Min:        f64p(t.min),
			Max:        f64p(t.max),
		}
		if len(t.samples) > 0 {
			sort.Float64s(t.samples)
			fm.P50 = f64p(quantile(t.samples, 0.50))
			fm.P95 = f64p(quantile(t.samples, 0.95))
			fm.P99 = f64p(quantile(t.samples, 0.99))
		}
		out = append(out, fm)
	}

	a.counters = make(map[seriesKey]*counterAgg)
	a.gauges = make(map[seriesKey]*gaugeAgg)
	a.timings = make(map[seriesKey]*timingAgg)
	prev := a.since
	a.since = now

	if len(out) == 0 {
		return
	}
	a.out <- Snapshot{Since: prev, Until: now, Metrics: out}
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
