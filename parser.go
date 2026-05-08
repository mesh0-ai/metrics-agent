package main

import (
	"errors"
	"sort"
	"strconv"
	"strings"
)

// MetricType is the kind of measurement carried by a single statsd line.
type MetricType uint8

const (
	MetricCounter MetricType = iota
	MetricGauge
	MetricTiming
)

// Metric is a parsed statsd line. Tags are stored sorted+joined ("k1:v1,k2:v2")
// so they can be used directly as a map key without per-sample allocation.
type Metric struct {
	Name       string
	Type       MetricType
	Value      float64
	SampleRate float64
	TagsKey    string
	Tags       map[string]string
}

var errMalformed = errors.New("malformed statsd line")

// parseLine parses one statsd / DogStatsD line:
//
//	name:value|type[|@sample_rate][|#tag1:v1,tag2:v2]
//
// It is allocation-light on the hot path: the only allocations are the Metric
// itself, the tag map (when tags are present), and the canonical TagsKey.
func parseLine(line string) (Metric, error) {
	var m Metric
	m.SampleRate = 1.0

	colon := strings.IndexByte(line, ':')
	if colon <= 0 || colon == len(line)-1 {
		return m, errMalformed
	}
	m.Name = line[:colon]
	rest := line[colon+1:]

	pipe := strings.IndexByte(rest, '|')
	if pipe <= 0 || pipe == len(rest)-1 {
		return m, errMalformed
	}
	v, err := strconv.ParseFloat(rest[:pipe], 64)
	if err != nil {
		return m, errMalformed
	}
	m.Value = v
	rest = rest[pipe+1:]

	// Type token, optionally followed by more |-separated extensions.
	typeTok := rest
	if i := strings.IndexByte(rest, '|'); i >= 0 {
		typeTok = rest[:i]
		rest = rest[i+1:]
	} else {
		rest = ""
	}
	switch typeTok {
	case "c":
		m.Type = MetricCounter
	case "g":
		m.Type = MetricGauge
	case "ms", "h", "d":
		m.Type = MetricTiming
	default:
		return m, errMalformed
	}

	for rest != "" {
		var tok string
		if i := strings.IndexByte(rest, '|'); i >= 0 {
			tok, rest = rest[:i], rest[i+1:]
		} else {
			tok, rest = rest, ""
		}
		if tok == "" {
			continue
		}
		switch tok[0] {
		case '@':
			r, err := strconv.ParseFloat(tok[1:], 64)
			if err != nil || r <= 0 || r > 1 {
				return m, errMalformed
			}
			m.SampleRate = r
		case '#':
			tags, key, err := parseTags(tok[1:])
			if err != nil {
				return m, err
			}
			m.Tags = tags
			m.TagsKey = key
		}
	}
	return m, nil
}

// parseTags returns both the tag map and a canonical "k1:v1,k2:v2" key
// (sorted by key) so equivalent tag sets aggregate to the same series.
func parseTags(s string) (map[string]string, string, error) {
	if s == "" {
		return nil, "", nil
	}
	parts := strings.Split(s, ",")
	tags := make(map[string]string, len(parts))
	keys := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, ':')
		var k, v string
		if eq < 0 {
			k = p
		} else {
			k, v = p[:eq], p[eq+1:]
		}
		if k == "" {
			return nil, "", errMalformed
		}
		if _, dup := tags[k]; !dup {
			keys = append(keys, k)
		}
		tags[k] = v
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(tags[k])
	}
	return tags, b.String(), nil
}
