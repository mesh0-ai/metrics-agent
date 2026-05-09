package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"
)

// retryableErr signals that the gateway response merits another attempt
// (5xx, 408, 429). 4xx other than those is a client problem retrying won't
// fix.
type retryableErr struct{ err error }

func (r *retryableErr) Error() string { return r.err.Error() }
func (r *retryableErr) Unwrap() error { return r.err }

func isRetryable(err error) bool {
	var re *retryableErr
	return errors.As(err, &re)
}

// postJSON performs a single POST with bearer auth. Returns *retryableErr for
// transient failures (network, 5xx, 408, 429) so callers can decide to retry.
func postJSON(parent context.Context, client *http.Client, url, apiKey string, body []byte) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("User-Agent", "mesh0-metrics-agent/"+Version)

	resp, err := client.Do(req)
	if err != nil {
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

// eventsFlusher pulls EventBatch values off `in` and POSTs each as
// {"events":[...]} to the configured events endpoint. Retries with
// exponential backoff + jitter on 429/5xx/network up to MaxRetries, then
// drops the batch and increments DropsFlushFailed.
type eventsFlusher struct {
	in         <-chan EventBatch
	url        string
	apiKey     string
	httpClient *http.Client
	log        *slog.Logger
	stats      *selfStats
	maxRetries int
	ctx        context.Context

	// rng is intentionally unsynchronised; only run() touches it.
	rng *rand.Rand
}

func newEventsFlusher(in <-chan EventBatch, cfg Config, log *slog.Logger, stats *selfStats) *eventsFlusher {
	return &eventsFlusher{
		in:         in,
		url:        cfg.GatewayURL + cfg.EventsPath,
		apiKey:     cfg.APIKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		log:        log,
		stats:      stats,
		maxRetries: cfg.MaxRetries,
		ctx:        context.Background(),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (f *eventsFlusher) run() {
	for batch := range f.in {
		f.send(batch)
	}
}

// send POSTs one batch with retry/backoff. Drops the batch once retries are
// exhausted (drops.flush_failed) or the shutdown context is cancelled mid-flight
// (drops.shutdown). Splitting the two lets operators distinguish a wedged
// gateway from a hard shutdown.
func (f *eventsFlusher) send(batch EventBatch) {
	body := encodeEventBatch(batch)
	var lastErr error
	cancelled := false

	// Total attempts = 1 + maxRetries. Backoff base 250ms, doubling, capped
	// at 5s; jitter scales by [0.5, 1.5) to spread retries across instances.
	const baseBackoff = 250 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if attempt > 0 {
			d := baseBackoff << (attempt - 1)
			if d > maxBackoff {
				d = maxBackoff
			}
			jf := 0.5 + f.rng.Float64()
			d = time.Duration(float64(d) * jf)
			select {
			case <-time.After(d):
			case <-f.ctx.Done():
				lastErr = f.ctx.Err()
				cancelled = true
				goto fail
			}
		}
		err := postJSON(f.ctx, f.httpClient, f.url, f.apiKey, body)
		if err == nil {
			f.stats.BatchesSent.Add(1)
			f.stats.EventsSent.Add(uint64(len(batch.Events)))
			f.stats.LastEventFlushMs.Store(time.Now().UnixMilli())
			f.log.Debug("events flush ok",
				"events", len(batch.Events),
				"bytes", len(body),
				"attempt", attempt,
			)
			return
		}
		lastErr = err
		if errors.Is(err, context.Canceled) {
			cancelled = true
			break
		}
		if !isRetryable(err) {
			break
		}
	}
fail:
	if cancelled {
		f.stats.DropsShutdown.Add(uint64(len(batch.Events)))
		f.log.Warn("events flush cancelled by shutdown, dropping batch",
			"events", len(batch.Events),
			"err", lastErr,
		)
		return
	}
	f.stats.DropsFlushFailed.Add(uint64(len(batch.Events)))
	f.log.Warn("events flush failed, dropping batch",
		"events", len(batch.Events),
		"err", lastErr,
	)
}
