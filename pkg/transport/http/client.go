package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// raftMessagePath is the fixed endpoint every peer POSTs Raft RPCs to.
// Mirror of the server route registered in 09-03 (WIRE §1).
const raftMessagePath = "/raft/message"

// client is the outbound half of the HTTP transport. It owns ONE shared
// *http.Client (built over a pooled *http.Transport) reused for every peer —
// never a client per Send (RESEARCH Anti-pattern: exhausts sockets, kills
// keep-alive). Send is safe from any goroutine and holds no lock
// (CONCURRENCY.md:179 forbids a lock across Transport.Send).
//
// All timing routes through cfg.Clock (backoff sleeps). The ONE sanctioned
// wall-clock use is context.WithTimeout for the per-request deadline (stdlib
// context reads time.Now internally); this package calls time.Now nowhere.
type client struct {
	cfg  Config
	http *http.Client
}

// newClient builds the shared pooled client from a validated Config. The
// *http.Transport enables keep-alive connection reuse across Sends; the
// per-attempt deadline is applied via context.WithTimeout in Send, so the
// client itself carries no global Timeout (which would fight the ctx budget).
func newClient(cfg Config) *client {
	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
	return &client{
		cfg:  cfg,
		http: &http.Client{Transport: tr},
	}
}

// Send resolves PeerURLs[msg.To], marshals the message via the 09-02 wire
// layer, and POSTs it to {base}/raft/message with Content-Type
// application/json. On a TRANSIENT failure (dial refused, timeout, 5xx) it
// retries with bounded exponential backoff driven by cfg.Clock, capped by
// cfg.Backoff.MaxAttempts AND ctx.Done()/SendTimeout (SC3). The backoff loop
// lives entirely inside this single call — invisible to the Raft core, whose
// heartbeat remains the cross-tick retry (see RESEARCH §SC3 reconciliation).
//
// Contract (raft.Transport.Send): best-effort, safe from any goroutine, never
// blocks indefinitely, wraps the underlying error with %w. An unknown peer is
// a wrapped error the driver logs-and-drops.
func (c *client) Send(ctx context.Context, msg raft.Message) error {
	base, ok := c.cfg.PeerURLs[msg.To]
	if !ok {
		return fmt.Errorf("http.Send: unknown peer %q (not in PeerURLs)", msg.To)
	}

	body, err := json.Marshal(fromMessage(msg))
	if err != nil {
		// Non-transient: a marshal failure will never succeed on retry.
		return fmt.Errorf("http.Send: marshal message to %q: %w", msg.To, err)
	}
	url := base + raftMessagePath

	// Bound the whole Send (all attempts + backoff) by SendTimeout. The sole
	// sanctioned wall-clock use: context.WithTimeout (inside stdlib context).
	if c.cfg.SendTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.SendTimeout)
		defer cancel()
	}

	attempts := c.cfg.Backoff.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			// Backoff sleep BEFORE this retry. Always select on ctx.Done() so
			// Send never blocks indefinitely (RESEARCH Pitfall 6).
			d := c.backoffDelay(attempt - 1)
			select {
			case <-c.cfg.Clock.After(d):
			case <-ctx.Done():
				return fmt.Errorf("http.Send to %q: %w", msg.To, ctx.Err())
			}
		}

		err := c.postOnce(ctx, url, body)
		if err == nil {
			return nil
		}
		if !isTransient(err) {
			// Non-transient (e.g. 4xx that isn't 5xx): do NOT retry.
			return fmt.Errorf("http.Send to %q: %w", msg.To, err)
		}
		lastErr = err
	}

	return fmt.Errorf("http.Send to %q: exhausted %d attempts: %w", msg.To, attempts, lastErr)
}

// postOnce performs a single POST /raft/message. It returns nil on a 2xx, a
// *httpStatusError on a non-2xx response (transient iff 5xx), or the raw
// transport error (dial/timeout — transient) on a network failure. The
// response body is always drained+closed so the pooled connection is reusable.
func (c *client) postOnce(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		// Network-level failure (dial refused, timeout, connection reset) —
		// treated as transient by isTransient.
		return err
	}
	// Drain + close so keep-alive can reuse the connection.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 == 2 {
		return nil
	}
	return &httpStatusError{status: resp.StatusCode}
}

// backoffDelay computes Base * Factor^n for the n-th retry (0-indexed).
// Factor <= 1 degrades to a constant Base delay.
func (c *client) backoffDelay(n int) time.Duration {
	base := c.cfg.Backoff.Base
	if base <= 0 {
		return 0
	}
	factor := c.cfg.Backoff.Factor
	d := float64(base)
	for i := 0; i < n; i++ {
		d *= factor
	}
	return time.Duration(d)
}

// httpStatusError carries a non-2xx response status so isTransient can decide
// whether it is retryable (5xx) or terminal (4xx).
type httpStatusError struct {
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("peer responded %d %s", e.status, http.StatusText(e.status))
}

// isTransient reports whether an error from postOnce warrants a backoff+retry.
// Transient: a 5xx status, or any network-level failure (dial refused, timeout,
// connection reset — surfaced by net.Error / *net.OpError). Terminal: a 4xx
// response (the request is malformed / rejected; retrying is pointless).
func isTransient(err error) bool {
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.status/100 == 5
	}
	// Any transport-level error (net.Error, *net.OpError, DNS failures, EOF on
	// dial) is a transient condition a healthy peer would not produce.
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return true
	}
	// A ctx error surfaced through http.Client.Do is handled by the Send loop's
	// ctx.Done() branch, not here; but treat a bare url.Error wrapping a dial
	// failure as transient too.
	return true
}
