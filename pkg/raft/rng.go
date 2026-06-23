package raft

import (
	"hash/fnv"
	mrand "math/rand/v2"
	"time"
)

// newNodeRNG constructs a per-node *math/rand/v2.Rand seeded from
// Config.Seed mixed with nodeID via FNV-1a 64. When cfgSeed == 0 the
// seed degrades to FNV(nodeID) ^ uint64(time.Now().UnixNano()) per
// ROADMAP SC2.
//
// Mixing chain: FNV-1a 64(nodeID) -> XOR with cfgSeed (or time.Now)
// -> splitmix64 -> rand/v2.NewPCG. Rationale and rejected alternatives
// are documented in docs/adr/0009-per-node-rng-mixing.md.
//
// Per-instance (not global) Rand — P1-4 prevention: the global
// math/rand source is forbidden because tests racing on it produce
// scheduler-dependent draws.
func newNodeRNG(cfgSeed int64, id NodeID) *mrand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	nodeHash := h.Sum64()
	var seed uint64
	if cfgSeed == 0 {
		seed = nodeHash ^ uint64(time.Now().UnixNano())
	} else {
		seed = nodeHash ^ uint64(cfgSeed)
	}
	lo, hi := splitmix64(seed)
	return mrand.New(mrand.NewPCG(hi, lo))
}

// splitmix64 expands a single uint64 seed into two well-distributed
// uint64s suitable for seeding rand/v2's PCG generator. PCG's two-
// word state benefits from inputs with good bit-mixing; a tiny seed
// fed directly to NewPCG yields a tiny state and a measurably weaker
// first stretch of output. splitmix64 (Vigna) is the canonical fix.
//
// Reference: https://prng.di.unimi.it/splitmix64.c
func splitmix64(x uint64) (uint64, uint64) {
	next := func() uint64 {
		x += 0x9E3779B97F4A7C15
		z := x
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	return next(), next()
}
