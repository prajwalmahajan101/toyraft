package raft

import (
	"fmt"
	"io"
)

// Log is the in-memory replicated log used by the Raft core.
//
// Concurrency: Log is NOT safe for concurrent use. Callers (Node) hold
// Node.mu (a single sync.Mutex) for every call — see docs/CONCURRENCY.md §2
// and docs/adr/0004-single-mutex-state-machine.md. Tests on *Log alone do
// NOT add t.Parallel() with a shared receiver; that violates the ADR-0004
// policy.
//
// Index semantics: 1-based, contiguous, strictly increasing.
//   - LastIndex() == 0 means the log is empty (zero-value safe, FOUND-05).
//   - Term(0) returns (Term(0), nil) — the implicit pre-log sentinel used by
//     the AppendEntries consistency check (Raft §5.3 log-matching property).
//
// Invariants enforced under -tags raftdebug (see log_assert_debug.go):
//  1. Append batch is contiguous: entries[i+1].Index == entries[i].Index+1
//  2. First entry continues the log: entries[0].Index == LastIndex()+1
//  3. Terms are non-decreasing within a batch and across appends
//  4. No entry has Term==0 (Term 0 is reserved for the empty-log sentinel)
//
// Production builds elide these checks via build tags (zero runtime cost).
type Log struct {
	entries []Entry // entries[i].Index == Index(i+1); never has gaps
}

// Append appends entries to the end of the log.
//
// Empty / nil input is a no-op (zero-value-safe symmetry per FOUND-05).
// Under -tags raftdebug, panics if the §4 invariants are violated.
// Under production builds, silently appends; the caller is trusted to
// satisfy invariants (Node holds Node.mu and computes the values from
// its own state).
func (l *Log) Append(entries ...Entry) {
	if len(entries) == 0 {
		return
	}
	assertAppendInvariants(l.LastIndex(), l.LastTerm(), entries)
	l.entries = append(l.entries, entries...)
}

// TruncateSuffix discards entries with Index >= from.
//
// Semantics:
//   - from == 0 truncates the entire log (legal; Phase 2 has no commitIndex
//     so no committed-prefix protection — that guard lives in Phase 6's AE
//     receiver per docs P0-3, NOT here).
//   - from > LastIndex() is a no-op.
//   - from == LastIndex()+1 is a no-op (idempotent).
func (l *Log) TruncateSuffix(from Index) {
	if from == 0 {
		l.entries = l.entries[:0]
		return
	}
	if from > l.LastIndex() {
		return
	}
	// from is 1-based; convert to 0-based slice index.
	l.entries = l.entries[:from-1]
}

// Term returns the term of the entry at idx, or Term(0) if idx == 0 (the
// pre-log sentinel). For idx > LastIndex(), returns (0, error) where the
// error wraps io.ErrUnexpectedEOF — matches LLD §3 LogStorage.Term contract.
func (l *Log) Term(idx Index) (Term, error) {
	if idx == 0 {
		return Term(0), nil
	}
	if idx > l.LastIndex() {
		return 0, fmt.Errorf("raft/log: Term(%d): %w", idx, io.ErrUnexpectedEOF)
	}
	return l.entries[idx-1].Term, nil
}

// Match reports whether the log contains an entry at prevIdx with term
// prevTerm. Returns true for (0, 0) — the pre-log sentinel base case used
// by AppendEntries consistency checks (Raft §5.3).
func (l *Log) Match(prevIdx Index, prevTerm Term) bool {
	if prevIdx == 0 {
		return prevTerm == 0
	}
	if prevIdx > l.LastIndex() {
		return false
	}
	return l.entries[prevIdx-1].Term == prevTerm
}

// LastIndex returns the largest Index present, or 0 if the log is empty.
func (l *Log) LastIndex() Index {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the Term of the last entry, or Term(0) if empty.
func (l *Log) LastTerm() Term {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}
