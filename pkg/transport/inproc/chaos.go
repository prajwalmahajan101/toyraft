package inproc

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Sub-RNG tags. Stable strings — DO NOT rename; reordering or renaming
// shifts the SHA-256-derived sub-seeds and breaks reproducibility of
// every committed test seed in the repo. See ADR-0007.
const (
	tagDrop    = "drop"
	tagDelay   = "delay"
	tagReorder = "reorder"
	tagDup     = "duplicate"
)

// splitSeed derives a stable int64 sub-seed from (seed, tag) via
// SHA-256(seed_be8 || tag)[:8]. ADR-0007 records why XOR-folding was
// rejected (correlation collisions when the tag and seed namespaces
// overlap).
func splitSeed(seed int64, tag string) int64 {
	var be [8]byte
	binary.BigEndian.PutUint64(be[:], uint64(seed))
	sum := sha256.Sum256(append(be[:], tag...))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// chaos holds all stochastic state for a Hub. All sub-RNGs are drawn
// from on the goroutine that calls Hub.send — which is the node's tick
// loop, itself driven synchronously by FakeClock.Advance. A single
// dispatcher goroutine handles delivery → no scheduler-dependent
// interleaving (RESEARCH Pattern 2).
type chaos struct {
	dropRNG    *rand.Rand
	delayRNG   *rand.Rand
	reorderRNG *rand.Rand
	dupRNG     *rand.Rand

	// Live knobs (mutated by Hub.Partition/Heal/DropRate/Delay/Reorder/Duplicate).
	// Reads + writes are guarded by Hub.mu (single mutex, ADR-0004).
	partitions  map[partitionKey]struct{}
	dropPerNode map[raft.NodeID]float64
	delayMin    time.Duration
	delayMax    time.Duration
	reorderOn   bool
	reorderQD   int
	dupRate     float64
}

// partitionKey is an unordered pair used as a map key. Hub.Partition
// installs BOTH directions (a,b) and (b,a) so the partition predicate
// is symmetric without a normalising compare — RESEARCH Pitfall 7.
type partitionKey struct {
	A raft.NodeID
	B raft.NodeID
}

// newChaos initialises per-knob PCG sub-RNGs from a single int64 seed.
// The second PCG word mixes in the golden-ratio constant so a small
// seed does not produce a tiny state. ADR-0007 documents the choice.
func newChaos(seed int64) *chaos {
	mk := func(tag string) *rand.Rand {
		s := uint64(splitSeed(seed, tag))
		return rand.New(rand.NewPCG(s, s^0x9E3779B97F4A7C15))
	}
	return &chaos{
		dropRNG:     mk(tagDrop),
		delayRNG:    mk(tagDelay),
		reorderRNG:  mk(tagReorder),
		dupRNG:      mk(tagDup),
		partitions:  make(map[partitionKey]struct{}),
		dropPerNode: make(map[raft.NodeID]float64),
	}
}

// isPartitioned reports whether (a -> b) is currently cut. Caller holds
// Hub.mu.
func (c *chaos) isPartitioned(a, b raft.NodeID) bool {
	_, ok := c.partitions[partitionKey{A: a, B: b}]
	return ok
}

// dropDecision returns true if this message should be dropped. Caller
// holds Hub.mu. The draw is taken regardless of the per-node rate so
// that toggling the rate from zero to non-zero between sends does not
// shift the stream's downstream draws (the consumer of a probability
// is its threshold; the stream is invariant).
func (c *chaos) dropDecision(from raft.NodeID) bool {
	p := c.dropPerNode[from]
	if p <= 0 {
		return false
	}
	return c.dropRNG.Float64() < p
}

// sampleDelay returns a per-Send delay drawn from [delayMin, delayMax).
// When the range is degenerate (max <= min) the deterministic delayMin
// is returned with no RNG draw. Caller holds Hub.mu.
func (c *chaos) sampleDelay() time.Duration {
	if c.delayMax <= c.delayMin {
		return c.delayMin
	}
	span := int64(c.delayMax - c.delayMin)
	return c.delayMin + time.Duration(c.delayRNG.Int64N(span))
}

// dupDecision returns true if this message should be duplicated.
// Caller holds Hub.mu.
func (c *chaos) dupDecision() bool {
	if c.dupRate <= 0 {
		return false
	}
	return c.dupRNG.Float64() < c.dupRate
}
