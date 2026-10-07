package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// hasTopLevelKey runs once per event on the batcher's hot path. It used a
// json.Decoder, which boxes every top-level token into an interface{} and
// decodes every value into a fresh RawMessage. It now walks the object with
// the routing layer's allocation-free scanner, falling back to the decoder
// for anything the scanner does not accept. These pin that the two agree on
// every VALID JSON input, which is its contract: both callers run it only on
// bytes json.Valid accepted (validateEvent checks first; a batch's events were
// decoded out of a body json.Decoder accepted). On invalid input the two may
// disagree, since the decoder answers as soon as it meets the key.

// referenceHasTopLevelKey is the json.Decoder implementation this replaced,
// kept verbatim as the oracle.
func referenceHasTopLevelKey(b []byte, key string) bool {
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

var topLevelKeyCorpus = []string{
	`{}`,
	`{"timestamp":"2024-01-01T00:00:00Z"}`,
	`{"a":1,"timestamp":2}`,
	`{"a":{"timestamp":1}}`,
	`{"a":"timestamp"}`,
	`{"a":"\"timestamp\":1"}`,
	`{"time\u0073tamp":1}`,
	`{"timestamp ":1}`,
	`{" timestamp":1}`,
	`{"Timestamp":1}`,
	`{ "a" : [1,2,{"timestamp":3}] , "b" : null }`,
	`{"a":[],"b":{},"c":true,"d":false,"e":null,"f":-1.5e3,"timestamp":0}`,
	"{\n\t\"timestamp\"\t:\n1\n}",
	`{"a":"x\\"y","timestamp":1}`,
	`{"a":"\u00e9","timestamp":1}`,
	`{"trace_id":"abc","attributes":{"event_name":"flow_function_enter","flow_evidence":{"seq":1}},"timestamp":"2026-10-05T12:00:00.000Z"}`,
	`{"trace_id":"abc","attributes":{"timestamp":"nested"}}`,
	`{"timestamp":1,"timestamp":2}`,
	`[{"timestamp":1}]`,
	`"timestamp"`,
	`{"a":1`,
	`{"a":}`,
	`{"timestamp"}`,
	`{"a":1,}`,
	`{"a":tru}`,
	`{"a":"\x01"}`,
	``,
}

func TestHasTopLevelKeyMatchesTheDecoder(t *testing.T) {
	deep := strings.Repeat(`{"a":`, 300) + `1` + strings.Repeat(`}`, 300)
	corpus := append([]string{deep, `{"timestamp":` + deep + `}`}, topLevelKeyCorpus...)
	for _, in := range corpus {
		if !json.Valid([]byte(in)) {
			continue
		}
		for _, key := range []string{"timestamp", "a", "time\u0073tamp", ""} {
			got := hasTopLevelKey([]byte(in), key)
			want := referenceHasTopLevelKey([]byte(in), key)
			if got != want {
				t.Errorf("hasTopLevelKey(%q, %q) = %v, decoder says %v", in, key, got, want)
			}
		}
	}
}

func FuzzHasTopLevelKey(f *testing.F) {
	for _, in := range topLevelKeyCorpus {
		f.Add(in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if !json.Valid([]byte(in)) {
			t.Skip()
		}
		got := hasTopLevelKey([]byte(in), "timestamp")
		want := referenceHasTopLevelKey([]byte(in), "timestamp")
		if got != want {
			t.Fatalf("hasTopLevelKey(%q) = %v, decoder says %v", in, got, want)
		}
	})
}

var flowEvidenceEvent = []byte(`{"timestamp":"2026-10-05T12:00:00.000Z","trace_id":"0af7651916cd43dd8448eb211c80319c","attributes":{"event_name":"flow_function_exit","flow_evidence":{"seq":42,"after":[41],"fact":{"run_id":"8b0f1e2a-6a43-4b5b-9d5e-3f2c1a0b9c8d","execution_id":"3f2c1a0b9c8d4e5f","kind":"function_invocation","phase":"exit","object":{"type":"function","id":17,"workspace_id":1,"branch_id":1},"outcome":"ok"}}},"_project":"workspace-1"}`)

func BenchmarkHasTopLevelKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hasTopLevelKey(flowEvidenceEvent, "timestamp")
	}
}

func BenchmarkReferenceHasTopLevelKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = referenceHasTopLevelKey(flowEvidenceEvent, "timestamp")
	}
}
