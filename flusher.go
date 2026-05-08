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
	Source  string          `json:"source"`
	Since   time.Time       `json:"since"`
	Until   time.Time       `json:"until"`
	Metrics []FlushedMetric `json:"metrics"`
}

type flusher struct {
	in         <-chan Snapshot
	url        string
	apiKey     string
	httpClient *http.Client
	log        *slog.Logger
	stats      *selfStats
	// ctx bounds the lifetime of in-flight HTTP calls. Set by main; cancelled
	// after the shutdown grace period so we don't outlive a SIGKILL.
	ctx context.Context
}

func newFlusher(in <-chan Snapshot, cfg Config, log *slog.Logger, stats *selfStats) *flusher {
	return &flusher{
		in:     in,
		url:    cfg.GatewayURL + cfg.FlushPath,
		apiKey: cfg.APIKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		log:   log,
		stats: stats,
		ctx:   context.Background(),
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

// retryableErr signals that the gateway response merits one more attempt
// (5xx, 408, 429). 4xx other than those is a client problem retrying won't
// fix.
type retryableErr struct{ err error }

func (r *retryableErr) Error() string { return r.err.Error() }
func (r *retryableErr) Unwrap() error { return r.err }

func isRetryable(err error) bool {
	var re *retryableErr
	return errors.As(err, &re)
}

func (f *flusher) send(s Snapshot) {
	payload := flushPayload{
		Source:  "mesh0-metrics-agent",
		Since:   s.Since,
		Until:   s.Until,
		Metrics: s.Metrics,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		f.stats.FlushesFailed.Add(1)
		f.log.Error("marshal flush payload", "err", err)
		return
	}

	// One quick retry on transient failure (network or 5xx/408/429). The
	// agent is fire-and-forget by design — we don't back up indefinitely.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-f.ctx.Done():
				lastErr = f.ctx.Err()
				goto done
			}
		}
		err := f.postOnce(body)
		if err == nil {
			f.stats.FlushesOK.Add(1)
			f.stats.MetricsFlushed.Add(uint64(len(s.Metrics)))
			f.stats.LastFlushUnix.Store(time.Now().Unix())
			f.log.Debug("flush ok", "metrics", len(s.Metrics), "attempt", attempt)
			return
		}
		lastErr = err
		if errors.Is(err, context.Canceled) {
			break
		}
		if !isRetryable(err) {
			break
		}
	}
done:
	f.stats.FlushesFailed.Add(1)
	f.log.Warn("flush failed", "err", lastErr, "metrics", len(s.Metrics))
}

func (f *flusher) postOnce(body []byte) error {
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
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
		// Network errors are retryable unless the context was cancelled by
		// shutdown — in that case, propagate context.Canceled untagged so
		// the retry loop doesn't burn another attempt on a doomed call.
		if errors.Is(err, context.Canceled) {
			return err
		}
		return &retryableErr{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	httpErr := fmt.Errorf("gateway %d: %s", resp.StatusCode, string(excerpt))
	if resp.StatusCode >= 500 || resp.StatusCode == 408 || resp.StatusCode == 429 {
		return &retryableErr{err: httpErr}
	}
	return httpErr
}
