package main

import "testing"

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Metric
		wantErr bool
	}{
		{
			name: "counter no tags",
			in:   "checkout.charge:1|c",
			want: Metric{Name: "checkout.charge", Type: MetricCounter, Value: 1, SampleRate: 1},
		},
		{
			name: "counter with sample rate and tags",
			in:   "checkout.charge:1|c|@0.1|#tier:pro,region:us-east-1",
			want: Metric{
				Name: "checkout.charge", Type: MetricCounter, Value: 1, SampleRate: 0.1,
				TagsKey: "region:us-east-1,tier:pro",
				Tags:    map[string]string{"tier": "pro", "region": "us-east-1"},
			},
		},
		{
			name: "gauge",
			in:   "queue.depth:42|g",
			want: Metric{Name: "queue.depth", Type: MetricGauge, Value: 42, SampleRate: 1},
		},
		{
			name: "timing as ms",
			in:   "llm.latency:142|ms|#model:gpt-4o",
			want: Metric{
				Name: "llm.latency", Type: MetricTiming, Value: 142, SampleRate: 1,
				TagsKey: "model:gpt-4o",
				Tags:    map[string]string{"model": "gpt-4o"},
			},
		},
		{
			name: "histogram alias",
			in:   "llm.latency:142|h",
			want: Metric{Name: "llm.latency", Type: MetricTiming, Value: 142, SampleRate: 1},
		},
		{
			name: "tags canonical order independent of input order",
			in:   "x:1|c|#zebra:a,alpha:b",
			want: Metric{
				Name: "x", Type: MetricCounter, Value: 1, SampleRate: 1,
				TagsKey: "alpha:b,zebra:a",
				Tags:    map[string]string{"zebra": "a", "alpha": "b"},
			},
		},
		{name: "empty", in: "", wantErr: true},
		{name: "no colon", in: "foo|c", wantErr: true},
		{name: "no value", in: "foo:|c", wantErr: true},
		{name: "no type", in: "foo:1", wantErr: true},
		{name: "bad type", in: "foo:1|q", wantErr: true},
		{name: "bad value", in: "foo:abc|c", wantErr: true},
		{name: "bad sample rate", in: "foo:1|c|@2.0", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLine(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.Name != tt.want.Name || got.Type != tt.want.Type ||
				got.Value != tt.want.Value || got.SampleRate != tt.want.SampleRate ||
				got.TagsKey != tt.want.TagsKey {
				t.Errorf("got %+v want %+v", got, tt.want)
			}
			if len(got.Tags) != len(tt.want.Tags) {
				t.Fatalf("tag len got %d want %d", len(got.Tags), len(tt.want.Tags))
			}
			for k, v := range tt.want.Tags {
				if got.Tags[k] != v {
					t.Errorf("tag %s: got %q want %q", k, got.Tags[k], v)
				}
			}
		})
	}
}

func BenchmarkParseLine(b *testing.B) {
	line := "checkout.charge:1|c|@0.1|#tier:pro,region:us-east-1,env:prod"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := parseLine(line); err != nil {
			b.Fatal(err)
		}
	}
}
