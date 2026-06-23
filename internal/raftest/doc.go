// Package raftest provides the deterministic test harness for ToyRaft.
// It assembles a FakeClock (internal/clock) and an inproc.Hub
// (pkg/transport/inproc) into an N-node Cluster wired through a single
// int64 seed. Client operations are captured as HistoryEvent values
// shaped 1:1 with anishathalye/porcupine v1.0.3's Operation type, so the
// recorded history is directly consumable by porcupine.CheckOperations
// in Phase 12.
//
// Phase 4 ships this package with a no-op consensus node (no election,
// no log replication). The harness determinism contract (SC5: same seed
// produces byte-identical history) is provable before the no-op node is
// replaced — see TestCluster_TwoRunsByteIdentical. Phase 5 swaps the
// no-op node for raft.Node without changing the harness wiring.
//
// This package lives under internal/ per LLD section 1. Promotion to a
// public pkg/raftest path requires an ADR (see Phase 4 RESEARCH Open
// Question 4 and the STOR-07 precedent).
//
// Node ID encoding: NewCluster uses zero-padded identifiers (n00, n01,
// ...) so that lexicographic sort of node IDs matches numeric index
// order even for N >= 10. The Hub's sortedNodes iteration order
// therefore matches index order, which keeps Cluster.Tick deterministic
// across runs.
package raftest
