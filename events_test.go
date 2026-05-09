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
	in := []byte(`{"operation":"x","duration_ms":12}`)
	out, reason := validateEvent(in)
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
	if got["operation"] != "x" {
		t.Errorf("expected operation preserved, got %+v", got)
	}
}

func TestValidateEventPreservesExistingTimestamp(t *testing.T) {
	in := []byte(`{"timestamp":"2024-01-01T00:00:00Z","op":"y"}`)
	out, reason := validateEvent(in)
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
		_, reason := validateEvent([]byte(c))
		if reason == validateOK {
			t.Errorf("accepted non-object %q", c)
		}
	}
}

func TestValidateEventRejectsMalformed(t *testing.T) {
	in := []byte(`{not json`)
	_, reason := validateEvent(in)
	if reason != validateParseError {
		t.Errorf("expected parse error, got %v", reason)
	}
}

func TestValidateEventOversize(t *testing.T) {
	// 33KB payload — over the 32KB cap.
	big := bytes.Repeat([]byte("a"), 33*1024)
	in := append([]byte(`{"x":"`), big...)
	in = append(in, []byte(`"}`)...)
	_, reason := validateEvent(in)
	if reason != validateOversize {
		t.Errorf("expected oversize, got %v", reason)
	}
}

func TestValidateEventLeadingWhitespace(t *testing.T) {
	in := []byte("  \n\t" + `{"a":1}` + "  \n")
	out, reason := validateEvent(in)
	if reason != validateOK {
		t.Fatalf("reason: %v", reason)
	}
	if !strings.HasPrefix(string(out), "{") {
		t.Errorf("expected trimmed result starting with {, got %q", out)
	}
}

func TestHasTopLevelKeyIgnoresNested(t *testing.T) {
	b := []byte(`{"x":{"timestamp":"nested"},"y":1}`)
	if hasTopLevelKey(b, "timestamp") {
		t.Error("matched nested timestamp")
	}
	if !hasTopLevelKey(b, "x") {
		t.Error("missed top-level x")
	}
	if !hasTopLevelKey(b, "y") {
		t.Error("missed top-level y")
	}
}

// TestHasTopLevelKeyIgnoresStringContents guards against a regression to a
// hand-rolled scanner: keys that appear inside string values (with or
// without escaped quotes) must not be matched as top-level keys.
func TestHasTopLevelKeyIgnoresStringContents(t *testing.T) {
	cases := []struct {
		name string
		in   string
		key  string
		want bool
	}{
		{"plain string value", `{"a":"timestamp inside","b":1}`, "timestamp", false},
		{"escaped quotes inside string", `{"a":"has \"timestamp\" inside","b":1}`, "timestamp", false},
		{"value contains key as substring", `{"name":"timestamp_field"}`, "timestamp", false},
		{"actual top-level after decoy", `{"a":"timestamp","timestamp":"2024-01-01T00:00:00Z"}`, "timestamp", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasTopLevelKey([]byte(tc.in), tc.key); got != tc.want {
				t.Errorf("hasTopLevelKey(%q, %q) = %v, want %v", tc.in, tc.key, got, tc.want)
			}
		})
	}
}
