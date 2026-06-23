package inproc

import (
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// HubConfig configures a Hub. Zero-value-safe modulo Clock: NewHub returns
// an actionable error when Clock is nil (FOUND-05 spirit). InboundCap and
// CloseTimeout receive sane defaults when zero.
type HubConfig struct {
	// Clock drives the dispatcher's logical time. Required. Tests pass a
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
}

const (
	defaultInboundCap   = 256
	defaultCloseTimeout = 100 * time.Millisecond
)
