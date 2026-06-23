package raft

import (
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// TestRNGPerNodeSameSeedDifferentNodeIDs pins ROADMAP SC2: two distinct
// nodeIDs seeded with the same Config.Seed produce divergent draw
// streams. We require >=95 of the first 100 positions to differ —
// asserting all 100 would be brittle if a future Go version changes
// PCG's stream shape and any single position happens to coincide.
//
// ELEC-02 / SC2.
func TestRNGPerNodeSameSeedDifferentNodeIDs(t *testing.T) {
	t.Parallel()
	const seed = int64(42)
	rA := newNodeRNG(seed, NodeID("node-a"), clock.NewReal())
	rB := newNodeRNG(seed, NodeID("node-b"), clock.NewReal())

	differing := 0
	for range 100 {
		a := rA.IntN(1 << 20)
		b := rB.IntN(1 << 20)
		if a != b {
			differing++
		}
	}
	if differing < 95 {
		t.Fatalf("same-seed different-nodeID divergence: got %d/100 differing draws, want >=95 (SC2)", differing)
	}
}

// TestRNGReproducibleSameSeedSameNodeID is SC2's corollary: chaos
// tests must replay byte-identically given (Config.Seed, nodeID). Two
// independently constructed RNGs with identical inputs must produce
// identical first-100 draws.
func TestRNGReproducibleSameSeedSameNodeID(t *testing.T) {
	t.Parallel()
	const seed = int64(42)
	r1 := newNodeRNG(seed, NodeID("n1"), clock.NewReal())
	r2 := newNodeRNG(seed, NodeID("n1"), clock.NewReal())
	for i := range 100 {
		a := r1.Uint64()
		b := r2.Uint64()
		if a != b {
			t.Fatalf("reproducibility break at draw %d: r1=%d r2=%d", i, a, b)
		}
	}
}

// TestRNGZeroSeedUsesTimeAndNodeID is a smoke test for the cfgSeed==0
// branch: the formula mixes time.Now().UnixNano() so two RNGs
// constructed across a small time delta MUST NOT panic and MUST
// produce a non-nil *Rand. We deliberately do NOT assert the draws
// differ — that is non-deterministic by design (the time delta could
// theoretically straddle a FNV cycle boundary, though in practice the
// xor with UnixNano shifts the seed every nanosecond).
func TestRNGZeroSeedUsesTimeAndNodeID(t *testing.T) {
	t.Parallel()
	r1 := newNodeRNG(0, NodeID("n1"), clock.NewReal())
	time.Sleep(time.Microsecond)
	r2 := newNodeRNG(0, NodeID("n1"), clock.NewReal())
	if r1 == nil || r2 == nil {
		t.Fatalf("newNodeRNG(0, ...) returned nil")
	}
	// Smoke-draw to ensure neither panics.
	_ = r1.Uint64()
	_ = r2.Uint64()
}

// TestRNGZeroSeedDifferentNodeIDsDiffer locks the cfgSeed==0 partial
// of SC2: even without a seed, two distinct nodeIDs must produce
// divergent first draws. Because the cfgSeed==0 branch XORs in
// time.Now().UnixNano() which is shared between the two calls in a
// tight loop, the divergence comes from the FNV(nodeID) component.
func TestRNGZeroSeedDifferentNodeIDsDiffer(t *testing.T) {
	t.Parallel()
	rA := newNodeRNG(0, NodeID("alpha"), clock.NewReal())
	rB := newNodeRNG(0, NodeID("bravo"), clock.NewReal())
	differing := 0
	for range 100 {
		if rA.Uint64() != rB.Uint64() {
			differing++
		}
	}
	if differing < 95 {
		t.Fatalf("zero-seed nodeID divergence: got %d/100 differing draws, want >=95", differing)
	}
}

// TestSplitmix64NonZeroForZeroInput is a defensive check: splitmix64(0)
// must not return (0, 0) — that would yield a degenerate PCG state.
// The constant-addition step in splitmix64 ensures this.
func TestSplitmix64NonZeroForZeroInput(t *testing.T) {
	t.Parallel()
	lo, hi := splitmix64(0)
	if lo == 0 && hi == 0 {
		t.Fatalf("splitmix64(0) = (0, 0); would yield degenerate PCG state")
	}
}
