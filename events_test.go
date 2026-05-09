package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Tests for the event validator (the v0.2 replacement for the old statsd
// line parser).

func TestValidateEventOK(t *testing.T) {
	in := []byte(`{"status":"success","duration_ms":12}`)
	out, reason := validateEvent(in, DefaultMaxEventBytes)
	if reason != validateOK {
		t.Fatalf("reason: %v", reason)
	}
	// Timestamp should have been injected (the input has none).
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := got["timestamp"]; !ok {
		t.Errorf("expected timestamp injected, got %+v", got)
	}
	if got["status"] != "success" {
		t.Errorf("expected status preserved, got %+v", got)
	}
}

func TestValidateEventPreservesExistingTimestamp(t *testing.T) {
	in := []byte(`{"timestamp":"2024-01-01T00:00:00Z","status":"success"}`)
	out, reason := validateEvent(in, DefaultMaxEventBytes)
	if reason != validateOK {
		t.Fatalf("reason: %v", reason)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["timestamp"] != "2024-01-01T00:00:00Z" {
		t.Errorf("timestamp overwritten: %+v", got)
	}
}

func TestValidateEventRejectsNonObject(t *testing.T) {
	cases := []string{
		`[]`,
		`"hello"`,
		`123`,
		``,
		`null`,
	}
	for _, c := range cases {
		_, reason := validateEvent([]byte(c), DefaultMaxEventBytes)
		if reason == validateOK {
			t.Errorf("accepted non-object %q", c)
		}
	}
}

func TestValidateEventRejectsMalformed(t *testing.T) {
	in := []byte(`{not json`)
	_, reason := validateEvent(in, DefaultMaxEventBytes)
	if reason != validateParseError {
		t.Errorf("expected parse error, got %v", reason)
	}
}

func TestValidateEventOversize(t *testing.T) {
	// Payload one byte past the configured cap. Use a small maxBytes so the
	// test stays fast and is decoupled from the package default.
	const maxBytes = 4096
	big := bytes.Repeat([]byte("a"), maxBytes)
	in := append([]byte(`{"status":"`), big...)
	in = append(in, []byte(`"}`)...)
	_, reason := validateEvent(in, maxBytes)
	if reason != validateOversize {
		t.Errorf("expected oversize, got %v", reason)
	}
}

func TestValidateEventBoundaryFits(t *testing.T) {
	// A payload of exactly maxBytes is accepted (off-by-one guard).
	const maxBytes = 4096
	pad := bytes.Repeat([]byte("a"), maxBytes-len(`{"status":""}`))
	in := append([]byte(`{"status":"`), pad...)
	in = append(in, []byte(`"}`)...)
	if len(in) != maxBytes {
		t.Fatalf("test setup: input is %d bytes, want %d", len(in), maxBytes)
	}
	_, reason := validateEvent(in, maxBytes)
	if reason != validateOK {
		t.Errorf("expected OK at exact boundary, got %v", reason)
	}
}

func TestValidateEventLeadingWhitespace(t *testing.T) {
	in := []byte("  \n\t" + `{"duration_ms":1}` + "  \n")
	out, reason := validateEvent(in, DefaultMaxEventBytes)
	if reason != validateOK {
		t.Fatalf("reason: %v", reason)
	}
	if !strings.HasPrefix(string(out), "{") {
		t.Errorf("expected trimmed result starting with {, got %q", out)
	}
}

func TestInspectTopLevelIgnoresNested(t *testing.T) {
	// A nested "timestamp" inside attributes must not be treated as a
	// top-level timestamp; the unknown "x" outer key must be flagged.
	b := []byte(`{"attributes":{"timestamp":"nested"},"x":1}`)
	hasTs, unknown, ok := inspectTopLevel(b)
	if !ok {
		t.Fatal("expected ok")
	}
	if hasTs {
		t.Error("matched nested timestamp as top-level")
	}
	if unknown != "x" {
		t.Errorf("expected unknown=x, got %q", unknown)
	}
}

// TestInspectTopLevelIgnoresStringContents guards against a regression to a
// hand-rolled scanner: keys that appear inside string values (with or
// without escaped quotes) must not be matched as top-level keys.
func TestInspectTopLevelIgnoresStringContents(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantHasTs  bool
		wantUnknwn string // "" means none
	}{
		{"plain string value", `{"status":"timestamp inside","duration_ms":1}`, false, ""},
		{"escaped quotes inside string", `{"status":"has \"timestamp\" inside","duration_ms":1}`, false, ""},
		{"value contains key as substring", `{"status":"timestamp_field"}`, false, ""},
		{"actual top-level after decoy", `{"status":"timestamp","timestamp":"2024-01-01T00:00:00Z"}`, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hasTs, unknown, ok := inspectTopLevel([]byte(tc.in))
			if !ok {
				t.Fatal("expected ok")
			}
			if hasTs != tc.wantHasTs {
				t.Errorf("hasTs=%v, want %v", hasTs, tc.wantHasTs)
			}
			if unknown != tc.wantUnknwn {
				t.Errorf("unknown=%q, want %q", unknown, tc.wantUnknwn)
			}
		})
	}
}

// TestValidateEventRejectsUnknownTopLevelKey ensures we fail per-event when
// a caller sends a key not in the mesh0 ingest contract — the API would 400
// the entire batch otherwise (DisallowUnknownFields).
func TestValidateEventRejectsUnknownTopLevelKey(t *testing.T) {
	cases := []string{
		`{"operation":"x"}`,                  // legacy field, no longer top-level
		`{"status":"success","app_id":"a"}`,  // mixed valid + unknown
		`{"model":{"id":"gpt-4"}}`,           // legacy nested unknown
		`{"project_id":"00000000-0000-0000-0000-000000000000"}`, // server-managed
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, reason := validateEvent([]byte(c), DefaultMaxEventBytes)
			if reason != validateUnknownField {
				t.Errorf("expected unknown-field reject for %s, got %v", c, reason)
			}
		})
	}
}

// TestValidateEventAcceptsAllAllowedKeys ensures every documented top-level
// field passes structural validation. Failing this means an SDK update made
// the agent stricter than the API.
func TestValidateEventAcceptsAllAllowedKeys(t *testing.T) {
	in := []byte(`{` +
		`"event_id":"e","trace_id":"t","span_id":"s","parent_span_id":"p",` +
		`"timestamp":"2024-01-01T00:00:00Z","duration_ms":1,"status":"success",` +
		`"attributes":{"k":"v"},"data":{"big":"payload"}` +
		`}`)
	_, reason := validateEvent(in, DefaultMaxEventBytes)
	if reason != validateOK {
		t.Errorf("expected OK, got %v", reason)
	}
}
