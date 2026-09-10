package inproc

import (
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// HubConfig configures a Hub. Fully zero-value-safe: NewHub defaults a nil
// Clock to the real clock (ADR-0023 parity), and InboundCap and CloseTimeout
// receive sane defaults when zero.
type HubConfig struct {
	// Clock drives the dispatcher's logical time. Optional; a nil Clock
	// defaults to the real clock in NewHub so external embedders can build a
	// Hub without constructing internal/clock. In-module tests pass a
	// *clock.Fake here so message delivery is reproducible from a seed.
	Clock clock.Clock

	// Seed is the int64 PRNG seed for chaos decisions. Unused in plan
	// 04-03 (no chaos yet); plan 04-04 will split this into per-knob
	// sub-RNGs (see ADR-0007).
	Seed int64

	// InboundCap is the per-node inbound channel capacity. Default 256
	// when zero. Bounded buffers + select-on-ctx are how Close unblocks
	// parked senders (SC4).
	InboundCap int

	// CloseTimeout caps how long Close waits for the dispatcher to exit
	// before declaring leak. Default 100ms per SC4.
	CloseTimeout time.Duration

	// SyncDelivery selects the SYNCHRONOUS delivery model: when true, NewHub
	// does NOT start the background dispatcher goroutine, and the caller must
	// pump due messages onto receiver inbound channels itself via DrainDueSync.
	// This gives a byte-deterministic delivery schedule under FakeClock with no
	// goroutine/wall-clock handoff — the seam internal/raftest.Cluster.Tick uses
	// so `same seed -> byte-identical trace` holds under chaos (ADR-0017). The
	// default (false) keeps the async single-dispatcher model used by every
	// other caller (hub_test / chaos_test / transporttest conformance).
	SyncDelivery bool
}

const (
	defaultInboundCap   = 256
	defaultCloseTimeout = 100 * time.Millisecond
)

// applyDefaults fills a nil Clock with the real clock so external embedders —
// who cannot construct internal/clock — can build a Hub by leaving Clock unset.
// Mirrors pkg/raft.Config and pkg/transport/http.Config (ADR-0023); called by
// NewHub before use. InboundCap/CloseTimeout keep their own zero-defaults in
// NewHub. In-module tests still inject a *clock.Fake for deterministic delivery.
func (c *HubConfig) applyDefaults() {
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
}
