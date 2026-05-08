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
// noisy producer can't OOM the agent. When the cap is hit we reservoir-sample
// (keep first N) — good enough for percentile signal in a 10s flush window
// and orders of magnitude simpler than t-digest.
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
	counters map[seriesKey]*counterAgg
	gauges   map[seriesKey]*gaugeAgg
	timings  map[seriesKey]*timingAgg
	since    time.Time
}

func newAggregator(in <-chan Metric, flush <-chan time.Time, out chan<- Snapshot) *aggregator {
	return &aggregator{
		in:       in,
		flush:    flush,
		out:      out,
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
// Only fields relevant to the metric Type are populated.
type FlushedMetric struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Tags       map[string]string `json:"tags,omitempty"`
	Timestamp  time.Time         `json:"timestamp"`
	IntervalMs int64             `json:"interval_ms"`
	Value      float64           `json:"value,omitempty"`
	Count      uint64            `json:"count,omitempty"`
	Sum        float64           `json:"sum,omitempty"`
	Min        float64           `json:"min,omitempty"`
	Max        float64           `json:"max,omitempty"`
	P50        float64           `json:"p50,omitempty"`
	P95        float64           `json:"p95,omitempty"`
	P99        float64           `json:"p99,omitempty"`
}

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
			Value:      c.value,
		})
	}
	for k, g := range a.gauges {
		out = append(out, FlushedMetric{
			Name:       k.Name,
			Type:       "gauge",
			Tags:       g.tags,
			Timestamp:  now,
			IntervalMs: intervalMs,
			Value:      g.value,
		})
	}
	for k, t := range a.timings {
		fm := FlushedMetric{
			Name:       k.Name,
			Type:       "timing",
			Tags:       t.tags,
			Timestamp:  a.since,
			IntervalMs: intervalMs,
			Count:      t.count,
			Sum:        t.sum,
			Min:        t.min,
			Max:        t.max,
		}
		if len(t.samples) > 0 {
			sort.Float64s(t.samples)
			fm.P50 = quantile(t.samples, 0.50)
			fm.P95 = quantile(t.samples, 0.95)
			fm.P99 = quantile(t.samples, 0.99)
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
