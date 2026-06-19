package raft

import (
	"testing"
)

// FuzzLogAppendTruncate drives random valid Append+TruncateSuffix sequences
// against a fresh Log and asserts the LastIndex / LastTerm contract per
// docs/LLD.md §2.1 and FOUND-05 zero-value semantics.
//
// Per RFC-0002 the surface is bounded to *Log only — no Storage, Transport,
// or Node reachable from here.
//
// Strategy: interpret the fuzz []byte as a script of (op, payload) pairs
// where op = data[i] % 2 (0 = Append one entry, 1 = TruncateSuffix). Term
// and Index are derived monotonically from the script position so we never
// trip the §4 invariants (those are covered by log_invariants_test.go under
// -tags raftdebug). The fuzz target is property checking on VALID input
// sequences, not negative testing.
func FuzzLogAppendTruncate(f *testing.F) {
	// Seed corpus is loaded from pkg/raft/testdata/fuzz/FuzzLogAppendTruncate/.
	// Plus inline seeds for the obvious base cases:
	f.Add([]byte{})              // empty log invariants
	f.Add([]byte{0})             // one append
	f.Add([]byte{0, 0, 0})       // three appends
	f.Add([]byte{0, 0, 1})       // two appends + truncate-tail
	f.Add([]byte{0, 0, 0, 1, 0}) // appends, truncate, append again

	f.Fuzz(func(t *testing.T, script []byte) {
		var l Log
		var nextTerm Term = 1
		var expectedLast Index = 0
		var expectedLastTerm Term = 0

		for _, b := range script {
			switch b % 2 {
			case 0:
				// Append one entry with monotonic term + contiguous index.
				e := Entry{Term: nextTerm, Index: expectedLast + 1}
				l.Append(e)
				expectedLast = e.Index
				expectedLastTerm = e.Term
				// Occasionally bump the term so we exercise non-trivial
				// Term() lookups.
				if b%4 == 2 {
					nextTerm++
				}
			case 1:
				// TruncateSuffix to a random in-range index.
				if expectedLast == 0 {
					continue
				}
				from := Index(b%byte(expectedLast)) + 1 // 1..expectedLast
				l.TruncateSuffix(from)
				expectedLast = from - 1
				if expectedLast == 0 {
					expectedLastTerm = 0
				} else {
					term, err := l.Term(expectedLast)
					if err != nil {
						t.Fatalf("Term(%d) after truncate: unexpected err %v", expectedLast, err)
					}
					expectedLastTerm = term
				}
			}
		}

		if got := l.LastIndex(); got != expectedLast {
			t.Fatalf("LastIndex: got %d, want %d", got, expectedLast)
		}
		if got := l.LastTerm(); got != expectedLastTerm {
			t.Fatalf("LastTerm: got %d, want %d", got, expectedLastTerm)
		}
		// Pre-log sentinel must always hold.
		if !l.Match(0, 0) {
			t.Fatalf("Match(0,0) must be true on any log; got false")
		}
		// Term(0) sentinel.
		if term, err := l.Term(0); term != 0 || err != nil {
			t.Fatalf("Term(0): got (%d, %v), want (0, nil)", term, err)
		}
	})
}

// FuzzLogMatch builds a random log, then asserts that for every in-range
// idx, Match(idx, Term(idx)) == true and Match(idx, wrongTerm) == false.
// Also asserts Match(idx > LastIndex, anyTerm) == false.
func FuzzLogMatch(f *testing.F) {
	f.Add([]byte{1})
	f.Add([]byte{1, 1, 1})
	f.Add([]byte{1, 1, 2, 2, 3})

	f.Fuzz(func(t *testing.T, terms []byte) {
		var l Log
		var prevTerm Term = 0
		for i, b := range terms {
			// Term is monotonically non-decreasing per §4 invariant 3.
			t0 := Term(b)
			if t0 == 0 {
				t0 = 1 // §4 invariant 4: no zero-term entries
			}
			if t0 < prevTerm {
				t0 = prevTerm
			}
			l.Append(Entry{Term: t0, Index: Index(i) + 1})
			prevTerm = t0
		}

		last := l.LastIndex()
		// For every in-range idx, Match with the correct term holds.
		for idx := Index(1); idx <= last; idx++ {
			term, err := l.Term(idx)
			if err != nil {
				t.Fatalf("Term(%d): unexpected err %v", idx, err)
			}
			if !l.Match(idx, term) {
				t.Fatalf("Match(%d, %d) = false, want true (Term match)", idx, term)
			}
			// Wrong term must miss (pick a safe non-equal value).
			wrong := term + 1
			if wrong == term {
				wrong = term - 1
			}
			if l.Match(idx, wrong) {
				t.Fatalf("Match(%d, %d) = true, want false (wrong term)", idx, wrong)
			}
		}
		// Out-of-range idx must always miss.
		if l.Match(last+1, 1) {
			t.Fatalf("Match(LastIndex+1, anyTerm) must be false; got true")
		}
		// Pre-log sentinel.
		if !l.Match(0, 0) {
			t.Fatalf("Match(0, 0) must be true; got false")
		}
		if l.Match(0, 1) {
			t.Fatalf("Match(0, 1) must be false (prevTerm must be 0 at sentinel); got true")
		}
	})
}
