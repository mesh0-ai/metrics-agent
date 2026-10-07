package main

import (
	"bytes"
	"context"
	"encoding/json"
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

// notAcceptedErr is a 2xx that accepted none of the batch: mesh0's open-mode
// admission answers `202 {"status":"provisioning","accepted":0}` while a
// project is provisioning, which means "dropped, keep sending". It is
// returned wrapped in a *retryableErr, so it is retried like a 5xx, and when
// retries run out the batch is counted as not_accepted rather than
// flush_failed: the gateway answered, it refused the events.
type notAcceptedErr struct{ status int }

func (e *notAcceptedErr) Error() string {
	return fmt.Sprintf("gateway %d accepted none of the batch", e.status)
}

func isNotAccepted(err error) bool {
	var na *notAcceptedErr
	return errors.As(err, &na)
}

// postOutcome is what a 2xx answer said about the batch. accepted is only
// meaningful when known is true, i.e. the body carried a numeric
// `accepted`.
type postOutcome struct {
	status   int
	accepted int
	known    bool
}

// maxAcceptBody bounds how much of a 2xx body is read to find `accepted`.
// mesh0's answer is a few dozen bytes; the rest is discarded.
const maxAcceptBody = 64 << 10

// readAccepted decodes `{"accepted": N}` from a 2xx body. Anything else
// (empty, not JSON, no numeric `accepted`) is reported as unknown, which
// keeps a gateway that does not report it on today's meaning.
func readAccepted(body []byte) (int, bool) {
	var p struct {
		Accepted *json.Number `json:"accepted"`
	}
	if err := json.Unmarshal(body, &p); err != nil || p.Accepted == nil {
		return 0, false
	}
	n, err := p.Accepted.Int64()
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// postJSON performs a single POST with bearer auth. Returns *retryableErr for
// transient failures (network, 5xx, 408, 429) so callers can decide to retry.
// On a 2xx it returns what the body said about acceptance; deciding whether
// that is a delivery is the caller's (acceptedOutcome), since only it knows
// how many events the batch held.
func postJSON(parent context.Context, client *http.Client, url, apiKey string, body []byte) (postOutcome, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return postOutcome{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("User-Agent", "mesh0-metrics-agent/"+Version)

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return postOutcome{}, err
		}
		return postOutcome{}, &retryableErr{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		answer, _ := io.ReadAll(io.LimitReader(resp.Body, maxAcceptBody))
		_, _ = io.Copy(io.Discard, resp.Body)
		accepted, known := readAccepted(answer)
		return postOutcome{status: resp.StatusCode, accepted: accepted, known: known}, nil
	}
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	httpErr := fmt.Errorf("gateway %d: %s", resp.StatusCode, string(excerpt))
	if resp.StatusCode >= 500 || resp.StatusCode == 408 || resp.StatusCode == 429 {
		return postOutcome{}, &retryableErr{err: httpErr}
	}
	return postOutcome{}, httpErr
}

// acceptedOutcome decides what a 2xx answer means for a batch of n events:
// how many were delivered, or a retryable notAcceptedErr when none were.
//
//   - `accepted` present: that many were delivered, capped at n. Zero (with
//     n > 0) is a refusal and is retried. A partial count is NOT retried: the
//     gateway kept some events and does not say which, so a retry would
//     duplicate them; the shortfall is counted as not_accepted.
//   - `accepted` absent: a 202 is mesh0's "dropped, keep sending" and is
//     retried; any other 2xx keeps the old meaning, all delivered.
func acceptedOutcome(o postOutcome, n int) (int, error) {
	if o.known {
		if o.accepted <= 0 && n > 0 {
			return 0, &retryableErr{err: &notAcceptedErr{status: o.status}}
		}
		if o.accepted > n {
			return n, nil
		}
		return o.accepted, nil
	}
	if o.status == http.StatusAccepted && n > 0 {
		return 0, &retryableErr{err: &notAcceptedErr{status: o.status}}
	}
	return n, nil
}

// eventsFlusher pulls EventBatch values off `in` and POSTs each as
// {"events":[...]} to the configured events endpoint. Retries with
// exponential backoff + jitter on 429/5xx/network up to MaxRetries, then
// drops the batch and increments DropsFlushFailed.
type eventsFlusher struct {
	in            <-chan EventBatch
	url           string
	apiKey        string
	httpClient    *http.Client
	log           *slog.Logger
	stats         *selfStats
	pipelineStats *pipelineStats // optional; nil when not multi-tenant routed
	maxRetries    int
	ctx           context.Context
	// notAcceptedBackoff is the first wait before retrying a batch the
	// gateway accepted none of (doubling, capped at maxBackoff). It is longer
	// than the 5xx base because what clears it is a project finishing
	// provisioning, which takes seconds, not a transient overload.
	notAcceptedBackoff time.Duration

	// rng is intentionally unsynchronised; only run() touches it.
	rng *rand.Rand
}

func newEventsFlusher(in <-chan EventBatch, cfg Config, log *slog.Logger, stats *selfStats) *eventsFlusher {
	return newEventsFlusherWithKey(in, cfg, cfg.APIKey, log, stats)
}

func newEventsFlusherWithKey(in <-chan EventBatch, cfg Config, apiKey string, log *slog.Logger, stats *selfStats) *eventsFlusher {
	return &eventsFlusher{
		in:         in,
		url:        cfg.GatewayURL + cfg.EventsPath,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		log:        log,
		stats:      stats,
		maxRetries: cfg.MaxRetries,
		ctx:        context.Background(),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),

		notAcceptedBackoff: time.Second,
	}
}

func (f *eventsFlusher) run() {
	for batch := range f.in {
		f.send(batch)
	}
}

// send POSTs one batch with retry/backoff. Drops the batch once retries are
// exhausted (drops.flush_failed, or drops.not_accepted when the gateway kept
// answering 2xx with nothing accepted) or the shutdown context is cancelled
// mid-flight (drops.shutdown). Splitting them lets operators distinguish a
// wedged gateway, a refusing one and a hard shutdown.
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
			base := baseBackoff
			if isNotAccepted(lastErr) && f.notAcceptedBackoff > 0 {
				base = f.notAcceptedBackoff
			}
			d := base << (attempt - 1)
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
		// The batch's own credential when it carried one, else the
		// pipeline's configured key. Empty Token is the keys-file path.
		key := f.apiKey
		if batch.Token != "" {
			key = batch.Token
		}
		outcome, err := postJSON(f.ctx, f.httpClient, f.url, key, body)
		sent := 0
		if err == nil {
			sent, err = acceptedOutcome(outcome, len(batch.Events))
		}
		if err == nil {
			short := uint64(len(batch.Events) - sent)
			f.stats.BatchesSent.Add(1)
			f.stats.EventsSent.Add(uint64(sent))
			f.stats.DropsNotAccepted.Add(short)
			f.stats.LastEventFlushMs.Store(time.Now().UnixMilli())
			if f.pipelineStats != nil {
				f.pipelineStats.BatchesSent.Add(1)
				f.pipelineStats.EventsSent.Add(uint64(sent))
				f.pipelineStats.DropsNotAccepted.Add(short)
				f.pipelineStats.LastEventFlushMs.Store(time.Now().UnixMilli())
			}
			if short > 0 {
				f.log.Warn("gateway accepted part of a batch; the rest is dropped (a retry would duplicate the accepted events)",
					"events", len(batch.Events),
					"accepted", sent,
				)
			}
			f.log.Debug("events flush ok",
				"events", len(batch.Events),
				"accepted", sent,
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
		if f.pipelineStats != nil {
			f.pipelineStats.DropsShutdown.Add(uint64(len(batch.Events)))
		}
		f.log.Warn("events flush cancelled by shutdown, dropping batch",
			"events", len(batch.Events),
			"err", lastErr,
		)
		return
	}
	if isNotAccepted(lastErr) {
		f.stats.DropsNotAccepted.Add(uint64(len(batch.Events)))
		if f.pipelineStats != nil {
			f.pipelineStats.DropsNotAccepted.Add(uint64(len(batch.Events)))
		}
		f.log.Warn("gateway kept accepting none of the batch, dropping it",
			"events", len(batch.Events),
			"err", lastErr,
		)
		return
	}
	f.stats.DropsFlushFailed.Add(uint64(len(batch.Events)))
	if f.pipelineStats != nil {
		f.pipelineStats.DropsFlushFailed.Add(uint64(len(batch.Events)))
	}
	f.log.Warn("events flush failed, dropping batch",
		"events", len(batch.Events),
		"err", lastErr,
	)
}
