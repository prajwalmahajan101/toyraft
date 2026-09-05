package http

import (
	"fmt"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Config is the HTTP transport's own address book and timer bundle.
//
// The Raft core (raft.Config.Peers is []NodeID) carries NO NodeID->address
// map — it is deliberately URL-agnostic. The http transport therefore owns
// the resolution table here: Send(msg) dials PeerURLs[msg.To]. This is the
// new public surface ratified by ADR-0015.
//
// All timing routes through Clock; the transport reads the wall clock ONLY via
// Clock. Clock is the sole time source (FOUND-05 spirit; scripts/
// check-no-time-now.sh guards it as of 09-05).
type Config struct {
	// NodeID is this node's stable identifier (the "from" on outbound RPCs).
	NodeID raft.NodeID

	// ListenAddr is the host:port the transport's HTTP server binds (09-03).
	ListenAddr string

	// PeerURLs resolves a peer NodeID to its base URL (e.g. "http://10.0.0.2:8080").
	// Send(msg) POSTs to PeerURLs[msg.To] + "/raft/message"; an unknown peer is
	// an error the driver logs-and-drops (best-effort send, WIRE §1).
	PeerURLs map[raft.NodeID]string

	// Clock is the sole time source for send timeouts and backoff. Optional; a
	// nil Clock defaults to the real clock in New (parity with raft.Config).
	// Injecting a Fake requires internal access and is for in-module tests only.
	Clock clock.Clock

	// SendTimeout bounds a single outbound POST /raft/message attempt.
	SendTimeout time.Duration

	// Backoff governs ret/re-dial pacing for a failed peer send (09-04).
	Backoff BackoffConfig

	// MaxBodyBytes caps an inbound request body; past it the server responds
	// 413 Payload Too Large (WIRE §1 headers / §3). 0 means use the package
	// default (8 MiB, WIRE §1 recommendation) applied by the server (09-03).
	MaxBodyBytes int64

	// ShutdownTimeout bounds graceful server drain on Close (09-03).
	ShutdownTimeout time.Duration
}

// applyDefaults fills a nil Clock with the real clock so external consumers —
// who cannot construct internal/clock — can build the transport by leaving
// Clock unset. Mirrors pkg/raft.Config.applyDefaults; called by New before Validate.
func (c *Config) applyDefaults() {
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
}

// BackoffConfig is the exponential-backoff schedule for a failed peer send.
// Delay(n) = Base * Factor^n, capped by MaxAttempts total attempts.
type BackoffConfig struct {
	Base        time.Duration
	Factor      float64
	MaxAttempts int
}

// Validate returns an actionable error when a field required to construct a
// working transport is missing. It checks only the invariants this package can
// enforce locally; per-request bounds are applied by the server/client.
func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("http.Config: NodeID must be set")
	}
	if c.Clock == nil {
		return fmt.Errorf("http.Config: Clock must be non-nil (use internal/clock.Real); the transport reads the wall clock only via Clock")
	}
	if len(c.PeerURLs) == 0 {
		return fmt.Errorf("http.Config: PeerURLs must be non-empty (the transport cannot resolve any peer address)")
	}
	if _, self := c.PeerURLs[c.NodeID]; self {
		// A self-URL is harmless but almost always a config mistake: the node
		// never Sends to itself. Flag it so operators catch a copy-paste error.
		return fmt.Errorf("http.Config: PeerURLs must not contain this node's own ID %q", c.NodeID)
	}
	return nil
}
