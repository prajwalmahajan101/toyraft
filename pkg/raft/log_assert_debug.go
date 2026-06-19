//go:build raftdebug
// +build raftdebug

package raft

import "fmt"

// assertAppendInvariants panics on the five §4 invariants from
// .planning/phases/02-foundations/02-RESEARCH.md. Invoked from Log.Append
// (always called; under !raftdebug it's a no-op via the paired file
// log_assert_prod.go).
//
// Panic-message templates (the requirePanic substring assertions are the
// contract — do NOT change without updating log_invariants_test.go):
//   - "zero-term entry"        — Invariant 4: Term==0 reserved for sentinel
//   - "index gap"              — Invariant 2: first append index != LastIndex+1
//   - "non-monotonic term"     — Invariant 3: term regression (across or in-batch)
//   - "non-contiguous batch"   — Invariant 1: entries[i+1].Index != entries[i].Index+1
func assertAppendInvariants(prevIdx Index, prevTerm Term, entries []Entry) {
	// Invariant 4 (per-entry zero-term) FIRST so the panic message is the
	// most specific available before invariant 1/2/3 fire on derived state.
	for i, e := range entries {
		if e.Term == 0 {
			panic(fmt.Sprintf("raft/log: zero-term entry at batch position %d (Term 0 is reserved for pre-log sentinel)", i))
		}
	}
	// Invariant 2: first entry continues the log.
	expected := prevIdx + 1
	if entries[0].Index != expected {
		panic(fmt.Sprintf("raft/log: index gap: first append index=%d, expected %d (LastIndex+1)", entries[0].Index, expected))
	}
	// Invariant 3a (across-Append term monotonicity): first entry's term >= prevTerm.
	if entries[0].Term < prevTerm {
		panic(fmt.Sprintf("raft/log: non-monotonic term: entries[0].Term=%d < prev term %d", entries[0].Term, prevTerm))
	}
	// Invariants 1 (contiguity) + 3b (term monotonicity within batch).
	for i := 0; i < len(entries)-1; i++ {
		if entries[i+1].Index != entries[i].Index+1 {
			panic(fmt.Sprintf("raft/log: non-contiguous batch: entries[%d].Index=%d, expected %d", i+1, entries[i+1].Index, entries[i].Index+1))
		}
		if entries[i+1].Term < entries[i].Term {
			panic(fmt.Sprintf("raft/log: non-monotonic term: entries[%d].Term=%d < prev term %d", i+1, entries[i+1].Term, entries[i].Term))
		}
	}
}
