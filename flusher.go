package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// flushPayload is the wire shape POSTed to gateway.mesh0.ai.
type flushPayload struct {
	ProjectID string          `json:"project_id"`
	Source    string          `json:"source"`
	Since     time.Time       `json:"since"`
	Until     time.Time       `json:"until"`
	Metrics   []FlushedMetric `json:"metrics"`
}

type flusher struct {
	in         <-chan Snapshot
	url        string
	apiKey     string
	projectID  string
	httpClient *http.Client
	log        *slog.Logger
}

func newFlusher(in <-chan Snapshot, cfg Config, log *slog.Logger) *flusher {
	return &flusher{
		in:        in,
		url:       cfg.GatewayURL + cfg.FlushPath,
		apiKey:    cfg.APIKey,
		projectID: cfg.ProjectID,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		log: log,
	}
}

// run drains snapshots until `in` is closed, then returns. The producer
// (main) closes `in` only after the aggregator has emitted its final
// snapshot, so this loop guarantees the last flush window is sent.
func (f *flusher) run() {
	for s := range f.in {
		f.send(s)
	}
}

func (f *flusher) send(s Snapshot) {
	payload := flushPayload{
		ProjectID: f.projectID,
		Source:    "mesh0-metrics-agent",
		Since:     s.Since,
		Until:     s.Until,
		Metrics:   s.Metrics,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		f.log.Error("marshal flush payload", "err", err)
		return
	}

	// One quick retry on transient failure. The agent is fire-and-forget by
	// design — we don't back up indefinitely if the gateway is down.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		err := f.postOnce(body)
		if err == nil {
			f.log.Debug("flush ok", "metrics", len(s.Metrics), "attempt", attempt)
			return
		}
		lastErr = err
		if errors.Is(err, context.Canceled) {
			return
		}
	}
	f.log.Warn("flush failed", "err", lastErr, "metrics", len(s.Metrics))
}

func (f *flusher) postOnce(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.apiKey)
	req.Header.Set("User-Agent", "mesh0-metrics-agent/"+Version)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("gateway %d: %s", resp.StatusCode, string(excerpt))
}
